package outputs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	filters "restreamer/irajstreamer/core/filters"
	"restreamer/irajstreamer/core/shared"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	mediats "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts"
	mediatscodecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts/codecs"
	"go.uber.org/zap"
)

const (
	defaultHLSSegmentDuration = 2 * time.Second
	defaultHLSPlaylistSize    = 6
	defaultHLSTargetDuration  = 2
)

type hlsSegmentEntry struct {
	Seq      int
	FileName string
	Duration float64
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

type ioWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
}

type hlsLive struct {
	id           string
	url          string
	outputFolder shared.Folder

	gopBuffer *filters.GOPBuffer

	done    chan struct{}
	Started chan struct{}

	closeOnce sync.Once

	isStarted   bool
	isInitiated bool

	isLive          bool
	cleanInterval   time.Duration
	segmentDuration time.Duration
	playlistSize    int
	targetDuration  int
	pathPrefix      string

	segmentIndex int
	entries      []hlsSegmentEntry

	currentSegmentFile     ioWriteCloser
	currentSegmentWriter   *mediats.Writer
	currentSegmentFileName string
	currentSegmentStartPTS time.Duration
	currentSegmentLastPTS  time.Duration
	currentSegmentStart90k int64
	currentSegmentLast90k  int64

	hasTimelineBase90k bool
	timelineBase90k    int64
	segmentOutput      *segmentFileOutput

	videoTrack *mediats.Track
	audioTrack *mediats.Track
	cachedSPS  []byte
	cachedPPS  []byte

	writeMu sync.Mutex

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

type HLSLiveOption func(*hlsLive)

func WithHLSSegmentDuration(d time.Duration) HLSLiveOption {
	return func(o *hlsLive) {
		if d > 0 {
			o.segmentDuration = d
		}
	}
}

func WithHLSPlaylistSize(size int) HLSLiveOption {
	return func(o *hlsLive) {
		if size > 0 {
			o.playlistSize = size
		}
	}
}

func WithHLSTargetDuration(target int) HLSLiveOption {
	return func(o *hlsLive) {
		if target > 0 {
			o.targetDuration = target
		}
	}
}

func WithHLSLiveMode() HLSLiveOption {
	return func(o *hlsLive) {
		o.isLive = true
	}
}

func WithHLSCleanInterval(d time.Duration) HLSLiveOption {
	return func(o *hlsLive) {
		if d > 0 {
			o.cleanInterval = d
		}
	}
}

func WithHLSPlaylistPathPrefix(prefix string) HLSLiveOption {
	return func(o *hlsLive) {
		prefix = strings.TrimSpace(prefix)
		if prefix == "" {
			return
		}
		if strings.HasPrefix(prefix, "http://") || strings.HasPrefix(prefix, "https://") {
			o.pathPrefix = strings.TrimRight(prefix, "/")
			return
		}
		if !strings.HasPrefix(prefix, "/") {
			prefix = "/" + prefix
		}
		o.pathPrefix = strings.TrimRight(prefix, "/")
	}
}

func NewHLSLiveDestination(id string, outputFolder any, opts ...HLSLiveOption) (shared.Stream, error) {
	folder, err := shared.AdaptFolder(outputFolder)
	if err != nil || folder == nil {
		return nil, fmt.Errorf("hls destination requires output folder")
	}

	dest := &hlsLive{
		id:              id,
		url:             id,
		outputFolder:    folder,
		gopBuffer:       filters.NewGOPBufferWithOptions(true, true, true, true, true),
		done:            make(chan struct{}),
		Started:         make(chan struct{}),
		segmentDuration: defaultHLSSegmentDuration,
		playlistSize:    defaultHLSPlaylistSize,
		targetDuration:  defaultHLSTargetDuration,
		events:          shared.NewEventEmitter(256),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(dest)
		}
	}

	return dest, nil
}

func (o *hlsLive) GetVideoChan() chan *shared.Frame { return o.gopBuffer.VideoFrameChan }
func (o *hlsLive) GetAudioChan() chan *shared.Frame { return o.gopBuffer.AudioFrameChan }
func (o *hlsLive) GetID() string                    { return o.id }
func (o *hlsLive) Type() string                     { return "writer" }
func (o *hlsLive) IsRestartable() bool              { return false }
func (o *hlsLive) RestartInterval() time.Duration {
	return 0
}

func (o *hlsLive) Clone() (shared.Stream, error) {
	return nil, errors.New("hls destination cannot be cloned")
}

func (o *hlsLive) WaitForStart(ctx context.Context) error {
	select {
	case <-o.Started:
		return nil
	case <-o.done:
		return errors.New("stream is closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *hlsLive) EventChan() chan shared.Event {
	if o.events == nil {
		return nil
	}
	return o.events.Chan()
}

func (o *hlsLive) Start() {
	if o.isInitiated {
		o.isStarted = true
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "hls destination resumed", Meta: shared.StreamLifecycleMeta{URL: o.url}})
		return
	}

	o.isInitiated = true
	o.isStarted = true

	if err := o.outputFolder.RemoveAll(); err != nil {
		getLogger().Error("hls destination remove dir failed", zap.String("path", o.url), zap.Error(err))
		return
	}

	if o.isLive && o.cleanInterval > 0 {
		segmentTTL := o.segmentDuration * time.Duration(o.playlistSize+2)
		if err := o.outputFolder.StartCleaner(o.cleanInterval, segmentTTL); err != nil {
			getLogger().Warn("hls destination start cleaner failed", zap.String("output_id", o.id), zap.Error(err))
		}
	}

	close(o.Started)
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "hls destination started", Meta: shared.StreamLifecycleMeta{URL: o.url}})

	go o.run()
	go o.gopBuffer.Run()
}

func (o *hlsLive) Stop() {
	o.isStarted = false
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: o.id, StreamType: o.Type(), Message: "hls destination stopped"})
}

func (o *hlsLive) Close() {
	o.Stop()
	o.closeOnce.Do(func() {
		if o.gopBuffer != nil {
			o.gopBuffer.Close()
		}
		close(o.done)
		o.writeMu.Lock()
		defer o.writeMu.Unlock()
		_ = o.closeCurrentSegmentLocked(true)
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: o.id, StreamType: o.Type(), Message: "hls destination closed"})
		o.events.Close()
	})
}

func (o *hlsLive) State() *shared.State {
	lastIO := o.lastVideoWrite
	if o.lastAudioWrite.After(lastIO) {
		lastIO = o.lastAudioWrite
	}

	return &shared.State{
		IsStarted:          o.isStarted,
		IsResumable:        o.IsRestartable(),
		LastIO:             lastIO,
		StreamID:           o.id,
		Url:                o.url,
		Type:               o.Type(),
		TotalVideoFrames:   o.TotalVideoFrames,
		TotalAudioFrames:   o.TotalAudioFrames,
		DroppedAudioFrames: o.DroppedAudioFrames,
		DroppedVideoFrames: o.DroppedVideoFrames,
	}
}

func (o *hlsLive) run() {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		o.consumeVideoReady()
	}()

	go func() {
		defer wg.Done()
		o.consumeAudioReady()
	}()

	wg.Wait()
}

func (o *hlsLive) consumeVideoReady() {
	for {
		select {
		case frame, ok := <-o.gopBuffer.GetVideoReadyChan():
			if !ok {
				return
			}
			if frame == nil || !o.isStarted {
				continue
			}
			o.handleVideoFrame(frame)
		case <-o.done:
			return
		}
	}
}

func (o *hlsLive) consumeAudioReady() {
	for {
		select {
		case frame, ok := <-o.gopBuffer.GetAudioReadyChan():
			if !ok {
				return
			}
			if frame == nil || !o.isStarted {
				continue
			}
			o.handleAudioFrame(frame)
		case <-o.done:
			return
		}
	}
}

func (o *hlsLive) handleVideoFrame(frame *shared.Frame) {
	o.writeMu.Lock()
	defer o.writeMu.Unlock()

	o.cacheH264ParameterSets(frame.Payload)

	if err := o.ensureSegmentLocked(frame.PTS); err != nil {
		o.DroppedVideoFrames++
		getLogger().Warn("hls destination ensure segment failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}

	rawPTS90k := durationTo90k(frame.PTS)
	rawDTS90k := durationTo90k(frame.DTS)
	if rawDTS90k == 0 {
		rawDTS90k = rawPTS90k
	}

	if frame.IsKeyFrame && rawDTS90k-o.currentSegmentStart90k >= durationTo90k(o.segmentDuration) {
		if err := o.rotateSegmentLocked(ticks90kToDuration(rawPTS90k), false); err != nil {
			o.DroppedVideoFrames++
			getLogger().Warn("hls destination rotate failed", zap.String("output_id", o.id), zap.Error(err))
			return
		}
	}

	pts90k := rawPTS90k - o.timelineBase90k
	dts90k := rawDTS90k - o.timelineBase90k
	if pts90k < 0 {
		pts90k = 0
	}
	if dts90k < 0 {
		dts90k = 0
	}
	pts90k, dts90k = o.normalizeVideoTimestamps90k(pts90k, dts90k)

	videoPayload := o.ensureSPSPPSOnKeyFrame(frame)
	if frame.IsKeyFrame {
		hasSPS, hasPPS := filters.H264SPSPPSPresent(videoPayload)
		if !hasSPS || !hasPPS {
			o.DroppedVideoFrames++
			getLogger().Warn("hls destination drop keyframe without SPS/PPS",
				zap.String("output_id", o.id),
				zap.Int64("sequence_id", frame.SequenceID),
				zap.Bool("has_sps", hasSPS),
				zap.Bool("has_pps", hasPPS))
			return
		}
	}

	if err := o.currentSegmentWriter.WriteH264(o.videoTrack, pts90k, dts90k, videoPayload); err != nil {
		o.DroppedVideoFrames++
		getLogger().Warn("hls destination write video failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}

	o.TotalVideoFrames++
	o.lastVideoWrite = time.Now()
	o.currentSegmentLast90k = rawPTS90k
	o.currentSegmentLastPTS = ticks90kToDuration(rawPTS90k)
}

func (o *hlsLive) handleAudioFrame(frame *shared.Frame) {
	o.writeMu.Lock()
	defer o.writeMu.Unlock()

	// Avoid opening a fresh segment from audio-only frames. HLS MPEG-TS
	// segments should start from video keyframes for decoder stability.
	if o.currentSegmentWriter == nil {
		o.DroppedAudioFrames++
		return
	}

	if err := o.ensureSegmentLocked(frame.PTS); err != nil {
		o.DroppedAudioFrames++
		getLogger().Warn("hls destination ensure segment failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}

	if len(frame.Payload) == 0 {
		o.DroppedAudioFrames++
		return
	}

	rawPTS90k := durationTo90k(frame.PTS)
	pts90k := rawPTS90k - o.timelineBase90k
	if pts90k < 0 {
		pts90k = 0
	}
	pts90k = o.normalizeAudioTimestamp90k(pts90k)
	if err := o.currentSegmentWriter.WriteMPEG4Audio(o.audioTrack, pts90k, frame.Payload); err != nil {
		o.DroppedAudioFrames++
		getLogger().Warn("hls destination write audio failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}

	o.TotalAudioFrames++
	o.lastAudioWrite = time.Now()
	if rawPTS90k > o.currentSegmentLast90k {
		o.currentSegmentLast90k = rawPTS90k
		o.currentSegmentLastPTS = ticks90kToDuration(rawPTS90k)
	}
}

func (o *hlsLive) ensureSegmentLocked(pts time.Duration) error {
	if o.currentSegmentWriter != nil {
		return nil
	}

	return o.openSegmentLocked(pts)
}

func (o *hlsLive) rotateSegmentLocked(nextPTS time.Duration, endList bool) error {
	if err := o.closeCurrentSegmentLocked(endList); err != nil {
		return err
	}

	return o.openSegmentLocked(nextPTS)
}

func (o *hlsLive) openSegmentLocked(startPTS time.Duration) error {
	if o.outputFolder == nil {
		return fmt.Errorf("hls destination output folder is nil")
	}

	fileName := fmt.Sprintf("seg_%06d.ts", o.segmentIndex)
	f, err := o.outputFolder.Create(fileName)
	if err != nil {
		return err
	}

	if o.currentSegmentWriter == nil {
		o.videoTrack = &mediats.Track{
			Codec: &mediatscodecs.H264{},
		}
		o.audioTrack = &mediats.Track{
			Codec: &mediatscodecs.MPEG4Audio{
				Config: mpeg4audio.Config{
					Type:          mpeg4audio.ObjectTypeAACLC,
					SampleRate:    DefaultAudioRate,
					ChannelConfig: uint8(DefaultAudioChannels),
					ChannelCount:  DefaultAudioChannels,
				},
			},
		}
		o.segmentOutput = &segmentFileOutput{current: f}
		o.currentSegmentWriter = mediats.NewWriter(o.segmentOutput, []*mediats.Track{o.videoTrack, o.audioTrack})
	} else {
		o.segmentOutput.Switch(f)
	}

	if _, err := o.currentSegmentWriter.WriteTables(); err != nil {
		_ = f.Close()
		return err
	}

	o.currentSegmentFile = f
	o.currentSegmentStartPTS = startPTS
	o.currentSegmentLastPTS = startPTS
	o.currentSegmentStart90k = durationTo90k(startPTS)
	o.currentSegmentLast90k = o.currentSegmentStart90k
	if !o.hasTimelineBase90k {
		o.hasTimelineBase90k = true
		o.timelineBase90k = o.currentSegmentStart90k
	}
	o.currentSegmentFileName = fileName
	o.segmentIndex++

	if len(o.entries) == 0 {
		return o.writePlaylistLocked(false)
	}

	return nil
}

func (o *hlsLive) closeCurrentSegmentLocked(endList bool) error {
	if o.currentSegmentFile == nil {
		return nil
	}

	duration := o.segmentDuration.Seconds()
	if o.currentSegmentLast90k >= o.currentSegmentStart90k {
		segDur90k := o.currentSegmentLast90k - o.currentSegmentStart90k
		if segDur90k > 0 {
			duration = float64(segDur90k) / 90000.0
		}
	}
	if duration <= 0 {
		duration = o.segmentDuration.Seconds()
	}

	o.entries = append(o.entries, hlsSegmentEntry{
		Seq:      o.segmentIndex - 1,
		FileName: o.currentSegmentFileName,
		Duration: duration,
	})
	o.events.Emit(shared.Event{
		Type:       shared.EventTypeSegmentGenerated,
		StreamID:   o.id,
		StreamType: o.Type(),
		Message:    "hls segment generated",
		Meta: shared.SegmentGeneratedMeta{
			Sequence:        o.segmentIndex - 1,
			FileName:        o.currentSegmentFileName,
			SegmentURL:      o.eventObjectURLOrPath(o.currentSegmentFileName),
			PlaylistName:    "stream.m3u8",
			PlaylistURL:     o.eventObjectURLOrPath("stream.m3u8"),
			DurationSeconds: duration,
		},
	})
	if o.isLive && o.playlistSize > 0 && len(o.entries) > o.playlistSize {
		o.entries = o.entries[len(o.entries)-o.playlistSize:]
	}

	errClose := o.currentSegmentFile.Close()
	o.currentSegmentFile = nil
	o.currentSegmentFileName = ""

	errPlaylist := o.writePlaylistLocked(endList)
	if errClose != nil {
		return errClose
	}
	return errPlaylist
}

func (o *hlsLive) writePlaylistLocked(endList bool) error {
	if o.outputFolder == nil {
		return fmt.Errorf("hls destination output folder is nil")
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:3\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", o.computeTargetDuration()))

	mediaSeq := 0
	if len(o.entries) > 0 {
		mediaSeq = o.entries[0].Seq
	}
	b.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", mediaSeq))

	for _, entry := range o.entries {
		// Segment files are standalone TS files for HLS playback. Keep explicit
		// discontinuity markers at rollovers for conservative player compatibility.
		if entry.Seq != mediaSeq {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", entry.Duration))
		b.WriteString(o.objectURLOrPath(entry.FileName) + "\n")
	}

	if endList {
		b.WriteString("#EXT-X-ENDLIST\n")
	}

	playlist := b.String()
	return shared.WriteFileAtomic(o.outputFolder, "stream.m3u8", []byte(playlist))
}

func (o *hlsLive) segmentURI(fileName string) string {
	return shared.JoinURLPrefix(o.pathPrefix, strings.TrimLeft(fileName, "/"))
}

func (o *hlsLive) objectURLOrPath(fileName string) string {
	return shared.PreferredURL(o.pathPrefix, o.outputFolder, fileName)
}

func (o *hlsLive) eventObjectURLOrPath(fileName string) string {
	return o.objectURLOrPath(fileName)
}

func durationTo90k(d time.Duration) int64 {
	return int64(d) * 90000 / int64(time.Second)
}

func ticks90kToDuration(v int64) time.Duration {
	return time.Duration(v) * time.Second / 90000
}

func (o *hlsLive) computeTargetDuration() int {
	target := o.targetDuration
	if target < 1 {
		target = 1
	}
	for _, entry := range o.entries {
		ceil := int(math.Ceil(entry.Duration))
		if ceil > target {
			target = ceil
		}
	}
	return target
}

func (o *hlsLive) normalizeAudioTimestamp90k(pts int64) int64 {
	if !o.hasLastAudioPTS90k {
		o.hasLastAudioPTS90k = true
		o.lastAudioPTS90k = pts
		return pts
	}

	if pts <= o.lastAudioPTS90k {
		pts = o.lastAudioPTS90k + 1
	}

	o.lastAudioPTS90k = pts
	return pts
}

func (o *hlsLive) normalizeVideoTimestamps90k(pts, dts int64) (int64, int64) {
	if !o.hasLastVideoDTS90k {
		o.hasLastVideoDTS90k = true
		o.lastVideoDTS90k = dts
	} else if dts <= o.lastVideoDTS90k {
		dts = o.lastVideoDTS90k + 1
		o.lastVideoDTS90k = dts
	} else {
		o.lastVideoDTS90k = dts
	}

	if pts < dts {
		pts = dts
	}

	if !o.hasLastVideoPTS90k {
		o.hasLastVideoPTS90k = true
		o.lastVideoPTS90k = pts
		return pts, dts
	}

	if pts <= o.lastVideoPTS90k {
		pts = o.lastVideoPTS90k + 1
		if pts < dts {
			pts = dts
		}
	}

	o.lastVideoPTS90k = pts
	return pts, dts
}

func (o *hlsLive) ensureSPSPPSOnKeyFrame(frame *shared.Frame) [][]byte {
	if frame == nil || !frame.IsKeyFrame {
		return frame.Payload
	}
	return h264EnsureSPSPPSOnKeyFrame(frame.Payload, true, o.cachedSPS, o.cachedPPS)
}

func (o *hlsLive) cacheH264ParameterSets(nalus [][]byte) {
	sps, pps := h264ExtractSPSPPS(nalus)
	if len(sps) > 0 {
		o.cachedSPS = sps
	}
	if len(pps) > 0 {
		o.cachedPPS = pps
	}
}

func h264ExtractSPSPPS(nalus [][]byte) ([]byte, []byte) {
	var sps []byte
	var pps []byte

	for _, nalu := range nalus {
		switch h264NALTypeFromUnit(nalu) {
		case 7:
			sps = cloneBytes(nalu)
		case 8:
			pps = cloneBytes(nalu)
		}
	}

	return sps, pps
}

func stripAnnexBStartCode(nalu []byte) []byte {
	if len(nalu) >= 4 && nalu[0] == 0x00 && nalu[1] == 0x00 {
		if nalu[2] == 0x01 {
			return nalu[3:]
		}
		if len(nalu) >= 5 && nalu[2] == 0x00 && nalu[3] == 0x01 {
			return nalu[4:]
		}
	}
	return nalu
}

func h264NALTypeFromUnit(nalu []byte) byte {
	nalu = stripAnnexBStartCode(nalu)
	if len(nalu) == 0 {
		return 0
	}
	return nalu[0] & 0x1F
}

func h264EnsureSPSPPSOnKeyFrame(nalus [][]byte, isKeyFrame bool, cachedSPS, cachedPPS []byte) [][]byte {
	if !isKeyFrame {
		return nalus
	}

	hasSPS, hasPPS := h264SPSPPSPresent(nalus)
	if hasSPS && hasPPS {
		return nalus
	}

	out := make([][]byte, 0, len(nalus)+2)
	if !hasSPS && len(cachedSPS) > 0 {
		out = append(out, cloneBytes(cachedSPS))
	}
	if !hasPPS && len(cachedPPS) > 0 {
		out = append(out, cloneBytes(cachedPPS))
	}
	out = append(out, nalus...)
	return out
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func h264SPSPPSPresent(nalus [][]byte) (bool, bool) {
	hasSPS := false
	hasPPS := false
	for _, nalu := range nalus {
		switch h264NALTypeFromUnit(nalu) {
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
		if hasSPS && hasPPS {
			return true, true
		}
	}
	return hasSPS, hasPPS
}
