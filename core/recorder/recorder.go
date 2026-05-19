package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	filters "restreamer/core/filters"
	shared "restreamer/core/shared"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	mediats "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts"
	mediatscodecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts/codecs"
	"go.uber.org/zap"
)

const (
	defaultSegmentDuration = 2 * time.Second
	defaultTargetDuration  = 2
	defaultAudioRate       = 44100
	defaultAudioChannels   = 2
	gracefulCloseTimeout   = 2 * time.Second
	recordPlaylistName     = "stream.m3u8"
)

type segmentEntry struct {
	Seq      int
	FileName string
	Duration float64
}

type ioWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
}

type segmentFileOutput struct {
	current ioWriteCloser
}

func (o *segmentFileOutput) Write(p []byte) (int, error) {
	if o.current == nil {
		return 0, errors.New("segment output is closed")
	}
	return o.current.Write(p)
}

func (o *segmentFileOutput) Switch(f ioWriteCloser) {
	o.current = f
}

type Recorder struct {
	id         string
	url        string
	outputRoot shared.Folder
	pathPrefix string

	gopBuffer *filters.GOPBuffer

	done      chan struct{}
	Started   chan struct{}
	closeOnce sync.Once
	runWG     sync.WaitGroup

	isStarted   bool
	isInitiated bool

	segmentDuration time.Duration
	targetDuration  int
	now             func() time.Time

	writeMu sync.Mutex

	sessionUnix         int64
	sessionID           string
	sessionFolder       shared.Folder
	sessionPlaylistName string
	sessionPlaylistPath string
	segmentIndex        int
	entries             []segmentEntry

	currentSegmentFile     ioWriteCloser
	currentSegmentWriter   *mediats.Writer
	currentSegmentFileName string
	currentSegmentStartPTS time.Duration
	currentSegmentLastPTS  time.Duration
	currentSegmentStart90k int64
	currentSegmentLast90k  int64
	segmentOutput          *segmentFileOutput

	videoTrack *mediats.Track
	audioTrack *mediats.Track
	cachedSPS  []byte
	cachedPPS  []byte

	hasTimelineBase90k bool
	timelineBase90k    int64

	TotalAudioFrames   int64
	TotalVideoFrames   int64
	DroppedAudioFrames float64
	DroppedVideoFrames float64
	lastAudioWrite     time.Time
	lastVideoWrite     time.Time

	hasLastAudioPTS90k bool
	lastAudioPTS90k    int64

	hasLastVideoPTS90k bool
	lastVideoPTS90k    int64
	hasLastVideoDTS90k bool
	lastVideoDTS90k    int64
	events             *shared.EventEmitter
}

func New(id string, outputRoot any, opts ...Option) (*Recorder, error) {
	id = normalizeBaseID(id)
	if id == "" {
		return nil, fmt.Errorf("recorder requires id")
	}
	rootFolder, err := shared.AdaptFolder(outputRoot)
	if err != nil || rootFolder == nil {
		return nil, fmt.Errorf("recorder requires output folder")
	}

	r := &Recorder{
		id:              id,
		url:             id,
		outputRoot:      rootFolder,
		// Recorder output must not drop stale frames, otherwise reference-frame
		// loss can corrupt later packets inside the generated segments.
		gopBuffer:       filters.NewGOPBufferWithOptions(true, true, true, true, false),
		done:            make(chan struct{}),
		Started:         make(chan struct{}),
		segmentDuration: defaultSegmentDuration,
		targetDuration:  defaultTargetDuration,
		now:             time.Now,
		events:          shared.NewEventEmitter(256),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}

	return r, nil
}

func (r *Recorder) GetVideoChan() chan *shared.Frame { return r.gopBuffer.VideoFrameChan }
func (r *Recorder) GetAudioChan() chan *shared.Frame { return r.gopBuffer.AudioFrameChan }
func (r *Recorder) GetID() string                    { return r.id }
func (r *Recorder) Type() string                     { return "writer" }
func (r *Recorder) IsRestartable() bool              { return false }
func (r *Recorder) RestartInterval() time.Duration   { return 0 }

func (r *Recorder) Clone() (shared.Stream, error) {
	return nil, errors.New("recorder cannot be cloned")
}

func (r *Recorder) WaitForStart(ctx context.Context) error {
	select {
	case <-r.Started:
		return nil
	case <-r.done:
		return errors.New("stream is closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Recorder) EventChan() chan shared.Event {
	if r.events == nil {
		return nil
	}
	return r.events.Chan()
}

func (r *Recorder) Start() {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	if r.isStarted {
		return
	}

	if err := r.recoverStaleSessionsLocked(); err != nil {
		getLogger().Warn("recorder stale session recovery failed", zap.String("output_id", r.id), zap.Error(err))
	}
	r.beginSessionLocked(r.now().Unix())
	r.isStarted = true
	r.emitRecordStartedLocked()

	if r.isInitiated {
		return
	}

	r.isInitiated = true
	close(r.Started)

	r.runWG.Add(2)
	go func() {
		defer r.runWG.Done()
		r.run()
	}()
	go func() {
		defer r.runWG.Done()
		r.gopBuffer.Run()
	}()
}

func (r *Recorder) Stop() {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	if !r.isStarted {
		return
	}

	r.isStarted = false
	r.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: r.id, StreamType: r.Type(), Message: "recorder stopped"})
	if err := r.finalizeSessionLocked(); err != nil {
		getLogger().Warn("recorder finalize on stop failed", zap.String("output_id", r.id), zap.Error(err))
	}
}

func (r *Recorder) Close() {
	r.closeOnce.Do(func() {
		r.writeMu.Lock()
		shouldDrain := r.isStarted && r.isInitiated
		r.writeMu.Unlock()

		if shouldDrain {
			time.Sleep(gracefulCloseTimeout)
		}

		if r.gopBuffer != nil {
			r.gopBuffer.Close()
		}
		r.runWG.Wait()

		r.writeMu.Lock()
		wasStarted := r.isStarted
		r.isStarted = false
		if wasStarted {
			r.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: r.id, StreamType: r.Type(), Message: "recorder stopped"})
		}
		if err := r.finalizeSessionLocked(); err != nil {
			getLogger().Warn("recorder finalize on close failed", zap.String("output_id", r.id), zap.Error(err))
		}
		r.writeMu.Unlock()

		close(r.done)
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: r.id, StreamType: r.Type(), Message: "recorder closed"})
		r.events.Close()
	})
}

func (r *Recorder) State() *shared.State {
	lastIO := r.lastVideoWrite
	if r.lastAudioWrite.After(lastIO) {
		lastIO = r.lastAudioWrite
	}

	return &shared.State{
		IsStarted:          r.isStarted,
		IsResumable:        r.IsRestartable(),
		LastIO:             lastIO,
		StreamID:           r.id,
		Url:                r.sessionPlaylistURL(),
		Type:               r.Type(),
		TotalVideoFrames:   r.TotalVideoFrames,
		TotalAudioFrames:   r.TotalAudioFrames,
		DroppedAudioFrames: r.DroppedAudioFrames,
		DroppedVideoFrames: r.DroppedVideoFrames,
	}
}

func (r *Recorder) run() {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		r.consumeVideoReady()
	}()

	go func() {
		defer wg.Done()
		r.consumeAudioReady()
	}()

	wg.Wait()
}

func (r *Recorder) consumeVideoReady() {
	for {
		select {
		case frame, ok := <-r.gopBuffer.GetVideoReadyChan():
			if !ok {
				return
			}
			if frame == nil || !r.isStarted {
				continue
			}
			r.handleVideoFrame(frame)
		case <-r.done:
			return
		}
	}
}

func (r *Recorder) consumeAudioReady() {
	for {
		select {
		case frame, ok := <-r.gopBuffer.GetAudioReadyChan():
			if !ok {
				return
			}
			if frame == nil || !r.isStarted {
				continue
			}
			r.handleAudioFrame(frame)
		case <-r.done:
			return
		}
	}
}

func (r *Recorder) handleVideoFrame(frame *shared.Frame) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	r.cacheH264ParameterSets(frame.Payload)

	if err := r.ensureSegmentLocked(frame.PTS); err != nil {
		r.DroppedVideoFrames++
		getLogger().Warn("recorder ensure segment failed", zap.String("output_id", r.id), zap.Error(err))
		return
	}

	rawPTS90k := durationTo90k(frame.PTS)
	rawDTS90k := durationTo90k(frame.DTS)
	if rawDTS90k == 0 {
		rawDTS90k = rawPTS90k
	}

	if frame.IsKeyFrame && rawDTS90k-r.currentSegmentStart90k >= durationTo90k(r.segmentDuration) {
		if err := r.rotateSegmentLocked(ticks90kToDuration(rawPTS90k)); err != nil {
			r.DroppedVideoFrames++
			getLogger().Warn("recorder rotate failed", zap.String("output_id", r.id), zap.Error(err))
			return
		}
	}

	pts90k := rawPTS90k - r.timelineBase90k
	dts90k := rawDTS90k - r.timelineBase90k
	if pts90k < 0 {
		pts90k = 0
	}
	if dts90k < 0 {
		dts90k = 0
	}
	pts90k, dts90k = r.normalizeVideoTimestamps90k(pts90k, dts90k)

	videoPayload := r.ensureSPSPPSOnKeyFrame(frame)
	if frame.IsKeyFrame {
		hasSPS, hasPPS := h264SPSPPSPresent(videoPayload)
		if !hasSPS || !hasPPS {
			r.DroppedVideoFrames++
			getLogger().Warn("recorder drop keyframe without SPS/PPS",
				zap.String("output_id", r.id),
				zap.Int64("sequence_id", frame.SequenceID),
				zap.Bool("has_sps", hasSPS),
				zap.Bool("has_pps", hasPPS))
			return
		}
	}

	if err := r.currentSegmentWriter.WriteH264(r.videoTrack, pts90k, dts90k, videoPayload); err != nil {
		r.DroppedVideoFrames++
		getLogger().Warn("recorder write video failed", zap.String("output_id", r.id), zap.Error(err))
		return
	}

	r.TotalVideoFrames++
	r.lastVideoWrite = time.Now()
	r.currentSegmentLast90k = rawPTS90k
	r.currentSegmentLastPTS = ticks90kToDuration(rawPTS90k)
}

func (r *Recorder) handleAudioFrame(frame *shared.Frame) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	if r.currentSegmentWriter == nil {
		r.DroppedAudioFrames++
		return
	}

	if err := r.ensureSegmentLocked(frame.PTS); err != nil {
		r.DroppedAudioFrames++
		getLogger().Warn("recorder ensure segment failed", zap.String("output_id", r.id), zap.Error(err))
		return
	}

	if len(frame.Payload) == 0 {
		r.DroppedAudioFrames++
		return
	}

	rawPTS90k := durationTo90k(frame.PTS)
	pts90k := rawPTS90k - r.timelineBase90k
	if pts90k < 0 {
		pts90k = 0
	}
	pts90k = r.normalizeAudioTimestamp90k(pts90k)

	if err := r.currentSegmentWriter.WriteMPEG4Audio(r.audioTrack, pts90k, frame.Payload); err != nil {
		r.DroppedAudioFrames++
		getLogger().Warn("recorder write audio failed", zap.String("output_id", r.id), zap.Error(err))
		return
	}

	r.TotalAudioFrames++
	r.lastAudioWrite = time.Now()
	if rawPTS90k > r.currentSegmentLast90k {
		r.currentSegmentLast90k = rawPTS90k
		r.currentSegmentLastPTS = ticks90kToDuration(rawPTS90k)
	}
}

func (r *Recorder) beginSessionLocked(startUnix int64) {
	_ = r.finalizeSessionLocked()

	r.sessionUnix = startUnix
	r.sessionID = fmt.Sprintf("%s_%d", r.id, r.sessionUnix)
	r.sessionFolder = r.outputRoot.Folder(r.sessionID)
	r.sessionPlaylistName = recordPlaylistName
	r.sessionPlaylistPath = path.Join(r.sessionID, r.sessionPlaylistName)

	r.segmentIndex = 0
	r.entries = nil
	r.currentSegmentFile = nil
	r.currentSegmentWriter = nil
	r.currentSegmentFileName = ""
	r.currentSegmentStartPTS = 0
	r.currentSegmentLastPTS = 0
	r.currentSegmentStart90k = 0
	r.currentSegmentLast90k = 0
	r.segmentOutput = nil
	r.videoTrack = nil
	r.audioTrack = nil
	r.cachedSPS = nil
	r.cachedPPS = nil
	r.hasTimelineBase90k = false
	r.timelineBase90k = 0
	r.hasLastAudioPTS90k = false
	r.lastAudioPTS90k = 0
	r.hasLastVideoPTS90k = false
	r.lastVideoPTS90k = 0
	r.hasLastVideoDTS90k = false
	r.lastVideoDTS90k = 0
}

func (r *Recorder) finalizeSessionLocked() error {
	if r.currentSegmentFile == nil {
		return nil
	}

	if err := r.closeCurrentSegmentLocked(); err != nil {
		return err
	}

	r.currentSegmentWriter = nil
	r.segmentOutput = nil
	r.videoTrack = nil
	r.audioTrack = nil
	return nil
}

func (r *Recorder) ensureSegmentLocked(pts time.Duration) error {
	if r.currentSegmentWriter != nil {
		return nil
	}
	return r.openSegmentLocked(pts)
}

func (r *Recorder) rotateSegmentLocked(nextPTS time.Duration) error {
	if err := r.closeCurrentSegmentLocked(); err != nil {
		return err
	}
	return r.openSegmentLocked(nextPTS)
}

func (r *Recorder) openSegmentLocked(startPTS time.Duration) error {
	if r.sessionFolder == nil {
		return fmt.Errorf("recorder session folder is nil")
	}

	fileName := fmt.Sprintf("seg_%06d.ts", r.segmentIndex)
	f, err := r.sessionFolder.Create(fileName)
	if err != nil {
		return err
	}

	if r.currentSegmentWriter == nil {
		r.videoTrack = &mediats.Track{Codec: &mediatscodecs.H264{}}
		r.audioTrack = &mediats.Track{
			Codec: &mediatscodecs.MPEG4Audio{
				Config: mpeg4audio.Config{
					Type:          mpeg4audio.ObjectTypeAACLC,
					SampleRate:    defaultAudioRate,
					ChannelConfig: uint8(defaultAudioChannels),
					ChannelCount:  defaultAudioChannels,
				},
			},
		}
		r.segmentOutput = &segmentFileOutput{current: f}
		r.currentSegmentWriter = mediats.NewWriter(r.segmentOutput, []*mediats.Track{r.videoTrack, r.audioTrack})
	} else {
		r.segmentOutput.Switch(f)
	}

	if _, err := r.currentSegmentWriter.WriteTables(); err != nil {
		_ = f.Close()
		return err
	}

	r.currentSegmentFile = f
	r.currentSegmentFileName = fileName
	r.currentSegmentStartPTS = startPTS
	r.currentSegmentLastPTS = startPTS
	r.currentSegmentStart90k = durationTo90k(startPTS)
	r.currentSegmentLast90k = r.currentSegmentStart90k
	if !r.hasTimelineBase90k {
		r.hasTimelineBase90k = true
		r.timelineBase90k = r.currentSegmentStart90k
	}
	r.segmentIndex++

	if len(r.entries) == 0 {
		return r.writePlaylistLocked()
	}

	return nil
}

func (r *Recorder) closeCurrentSegmentLocked() error {
	if r.currentSegmentFile == nil {
		return nil
	}

	duration := r.segmentDuration.Seconds()
	if r.currentSegmentLast90k >= r.currentSegmentStart90k {
		segDur90k := r.currentSegmentLast90k - r.currentSegmentStart90k
		if segDur90k > 0 {
			duration = float64(segDur90k) / 90000.0
		}
	}
	if duration <= 0 {
		duration = r.segmentDuration.Seconds()
	}

	r.entries = append(r.entries, segmentEntry{
		Seq:      r.segmentIndex - 1,
		FileName: r.currentSegmentFileName,
		Duration: duration,
	})

	errClose := r.currentSegmentFile.Close()
	r.currentSegmentFile = nil
	r.currentSegmentFileName = ""

	errPlaylist := r.writePlaylistLocked()
	if errClose != nil {
		return errClose
	}
	return errPlaylist
}

func (r *Recorder) writePlaylistLocked() error {
	if r.sessionFolder == nil {
		return fmt.Errorf("recorder session folder is nil")
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:3\n")
	// Recorder playlists are always emitted as VOD-style snapshots.
	b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", r.computeTargetDuration()))
	b.WriteString("#EXT-X-MEDIA-SEQUENCE:0\n")

	for _, entry := range r.entries {
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", entry.Duration))
		b.WriteString(r.sessionObjectURLOrPath(entry.FileName) + "\n")
	}

	// Keep ENDLIST on every write so the current snapshot is always VOD playable.
	b.WriteString("#EXT-X-ENDLIST\n")

	return shared.WriteFileAtomic(r.sessionFolder, r.sessionPlaylistName, []byte(b.String()))
}

func (r *Recorder) computeTargetDuration() int {
	target := r.targetDuration
	if target < 1 {
		target = 1
	}
	for _, entry := range r.entries {
		ceil := int(math.Ceil(entry.Duration))
		if ceil > target {
			target = ceil
		}
	}
	return target
}

func (r *Recorder) normalizeAudioTimestamp90k(pts int64) int64 {
	if !r.hasLastAudioPTS90k {
		r.hasLastAudioPTS90k = true
		r.lastAudioPTS90k = pts
		return pts
	}

	if pts <= r.lastAudioPTS90k {
		pts = r.lastAudioPTS90k + 1
	}

	r.lastAudioPTS90k = pts
	return pts
}

func (r *Recorder) normalizeVideoTimestamps90k(pts, dts int64) (int64, int64) {
	if !r.hasLastVideoDTS90k {
		r.hasLastVideoDTS90k = true
		r.lastVideoDTS90k = dts
	} else if dts <= r.lastVideoDTS90k {
		dts = r.lastVideoDTS90k + 1
		r.lastVideoDTS90k = dts
	} else {
		r.lastVideoDTS90k = dts
	}

	if pts < dts {
		pts = dts
	}

	if !r.hasLastVideoPTS90k {
		r.hasLastVideoPTS90k = true
		r.lastVideoPTS90k = pts
		return pts, dts
	}

	if pts <= r.lastVideoPTS90k {
		pts = r.lastVideoPTS90k + 1
		if pts < dts {
			pts = dts
		}
	}

	r.lastVideoPTS90k = pts
	return pts, dts
}

func (r *Recorder) ensureSPSPPSOnKeyFrame(frame *shared.Frame) [][]byte {
	if frame == nil || !frame.IsKeyFrame {
		return frame.Payload
	}
	return h264EnsureSPSPPSOnKeyFrame(frame.Payload, true, r.cachedSPS, r.cachedPPS)
}

func (r *Recorder) cacheH264ParameterSets(nalus [][]byte) {
	sps, pps := h264ExtractSPSPPS(nalus)
	if len(sps) > 0 {
		r.cachedSPS = sps
	}
	if len(pps) > 0 {
		r.cachedPPS = pps
	}
}

func (r *Recorder) emitRecordStartedLocked() {
	meta := r.buildRecordStartedMetaLocked()
	if meta == nil {
		return
	}
	r.events.Emit(shared.Event{
		Type:       shared.EventTypeRecordStarted,
		StreamID:   r.id,
		StreamType: r.Type(),
		Message:    "recording session started",
		Meta:       *meta,
	})
}

func (r *Recorder) buildRecordStartedMetaLocked() *shared.RecordStartedMeta {
	if r.sessionID == "" || r.sessionFolder == nil || strings.TrimSpace(r.sessionPlaylistName) == "" {
		return nil
	}

	meta := &shared.RecordStartedMeta{
		SessionID:     r.sessionID,
		PlaylistName:  r.sessionPlaylistName,
		PlaylistURL:   r.sessionObjectURLOrPath(r.sessionPlaylistName),
		SegmentCount:  len(r.entries),
		SegmentURLs:   make([]string, 0, len(r.entries)),
		StartedAtUnix: r.sessionUnix,
	}
	for _, entry := range r.entries {
		if strings.TrimSpace(entry.FileName) == "" {
			continue
		}
		meta.SegmentURLs = append(meta.SegmentURLs, r.sessionObjectURLOrPath(entry.FileName))
	}
	return meta
}

func (r *Recorder) resolveSessionObjectURL(relPath string) string {
	if r.sessionFolder == nil {
		return ""
	}
	url, err := shared.ResolveObjectURL(r.sessionFolder, relPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(url)
}

func (r *Recorder) sessionObjectURLOrPath(relPath string) string {
	relPath = strings.TrimSpace(relPath)
	if relPath == "" {
		return ""
	}
	if strings.TrimSpace(r.pathPrefix) != "" {
		if r.sessionID == "" {
			return shared.JoinURLPrefix(r.pathPrefix, relPath)
		}
		return shared.JoinURLPrefix(r.pathPrefix, r.sessionID, relPath)
	}
	if absoluteURL := r.resolveSessionObjectURL(relPath); absoluteURL != "" {
		return absoluteURL
	}
	if r.sessionID == "" {
		return relPath
	}
	return path.Join(r.sessionID, relPath)
}

func (r *Recorder) sessionPlaylistURL() string {
	if strings.TrimSpace(r.sessionPlaylistName) == "" {
		return ""
	}
	return r.sessionObjectURLOrPath(r.sessionPlaylistName)
}

func (r *Recorder) recoverStaleSessionsLocked() error {
	if r.outputRoot == nil {
		return nil
	}

	entries, err := r.outputRoot.ReadDir()
	if err != nil {
		return err
	}

	prefix := r.id + "_"
	for _, entry := range entries {
		if entry == nil || !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if err := r.finalizeRecoveredSession(name); err != nil {
			return err
		}
	}
	return nil
}

func (r *Recorder) finalizeRecoveredSession(sessionID string) error {
	sessionFolder := r.outputRoot.Folder(sessionID)
	if sessionFolder == nil {
		return nil
	}

	f, err := sessionFolder.Open(recordPlaylistName)
	if err != nil {
		return nil
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	playlist := string(data)
	if strings.Contains(playlist, "#EXT-X-ENDLIST") {
		return nil
	}

	trimmed := strings.TrimRight(playlist, "\n")
	if trimmed == "" {
		return nil
	}
	trimmed += "\n#EXT-X-ENDLIST\n"
	return shared.WriteFileAtomic(sessionFolder, recordPlaylistName, []byte(trimmed))
}
