package outputs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	filters "github.com/tupicapp/restreamer/core/filters"
	"github.com/tupicapp/restreamer/core/shared"

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
	Seq           int
	FileName      string
	Duration      float64
	Discontinuity bool
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

	segmentIndex int
	entries      []hlsSegmentEntry

	discontinuitySequence int

	currentSegmentFile     ioWriteCloser
	currentSegmentWriter   *mediats.Writer
	currentSegmentFileName string
	currentSegmentInputID  string
	currentSegmentDisco    bool
	currentSegmentStartPTS time.Duration
	currentSegmentLastPTS  time.Duration
	currentSegmentStart90k int64
	currentSegmentLast90k  int64
	currentSegmentHasTime  bool
	forceDiscontinuityNext bool

	hasTimelineBase90k bool
	timelineBase90k    int64
	segmentOutput      *segmentFileOutput

	videoTrack       *mediats.Track
	audioTrack       *mediats.Track
	cachedSPSByInput map[string][]byte
	cachedPPSByInput map[string][]byte

	writeMu sync.Mutex

	TotalAudioFrames   int64
	TotalVideoFrames   int64
	DroppedAudioFrames float64
	DroppedVideoFrames float64
	lastAudioWrite     time.Time
	lastVideoWrite     time.Time

	hasLastAudioPTS90k  bool
	lastAudioPTS90k     int64
	audioClockRate      int
	audioClockRemainder int64

	audioSampleRate int // detected from first audio frame; 0 means use DefaultAudioRate
	activeAudioRate int
	inputAudioRates map[string]int

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

func WithHLSSidecars(sidecars ...shared.Stream) HLSLiveOption {
	return func(o *hlsLive) {}
}

func NewHLSLiveDestination(id string, outputFolder any, opts ...HLSLiveOption) (shared.Stream, error) {
	return newHLSLiveDestinationAsync(id, outputFolder, opts...)
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
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "hls destination resumed", Meta: shared.StreamLifecycleMeta{URL: o.playlistURL()}})
		return
	}

	o.isInitiated = true
	o.isStarted = true

	if err := o.outputFolder.RemoveAll(); err != nil {
		getLogger().Error("hls destination remove dir failed", zap.String("path", o.localPath()), zap.Error(err))
		return
	}

	if o.isLive && o.cleanInterval > 0 {
		segmentTTL := o.segmentDuration * time.Duration(o.playlistSize+2)
		if err := o.outputFolder.StartCleaner(o.cleanInterval, segmentTTL); err != nil {
			getLogger().Warn("hls destination start cleaner failed", zap.String("output_id", o.id), zap.Error(err))
		}
	}

	close(o.Started)
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "hls destination started", Meta: shared.StreamLifecycleMeta{URL: o.playlistURL()}})

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
		IsStarted: o.isStarted,
		LastIO:    lastIO,
		StreamID:  o.id,
		Url:       o.playlistURL(),
		LocalPath: o.localPath(),
		ServeType: "hls",
		ServeMode: "live",
		Served: []shared.ServedState{{
			StreamID:  o.id,
			Url:       o.playlistURL(),
			LocalPath: o.localPath(),
			ServeType: "hls",
			ServeMode: "live",
		}},
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

	o.cacheH264ParameterSets(frame.InputID, frame.Payload)

	dropFrame, err := o.handleVideoInputSwitchLocked(frame)
	if err != nil {
		o.DroppedVideoFrames++
		getLogger().Warn("hls destination switch boundary rotate failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}
	if dropFrame {
		o.DroppedVideoFrames++
		return
	}

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

	pts90k := rawPTS90k - o.timelineBase90k
	dts90k := rawDTS90k - o.timelineBase90k
	if pts90k < 0 {
		pts90k = 0
	}
	if dts90k < 0 {
		dts90k = 0
	}
	pts90k, dts90k = o.normalizeVideoTimestamps90k(pts90k, dts90k)
	if frame.IsKeyFrame && o.currentSegmentHasTime && dts90k-o.currentSegmentStart90k >= durationTo90k(o.segmentDuration) {
		if err := o.rotateSegmentLocked(ticks90kToDuration(rawPTS90k), false); err != nil {
			o.DroppedVideoFrames++
			getLogger().Warn("hls destination rotate failed", zap.String("output_id", o.id), zap.Error(err))
			return
		}
		o.currentSegmentHasTime = false
	}

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
	if !o.currentSegmentHasTime {
		o.currentSegmentHasTime = true
		o.currentSegmentStart90k = dts90k
		o.currentSegmentStartPTS = ticks90kToDuration(dts90k)
	}
	o.currentSegmentLast90k = pts90k
	o.currentSegmentLastPTS = ticks90kToDuration(pts90k)
}

func (o *hlsLive) handleAudioFrame(frame *shared.Frame) {
	o.writeMu.Lock()
	defer o.writeMu.Unlock()

	o.rememberInputAudioRate(frame.InputID, frame.SampleRate)

	if o.shouldDropAudioForInputLocked(frame.InputID) {
		o.DroppedAudioFrames++
		return
	}

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

	if frame.SampleRate > 0 && o.activeAudioRate != frame.SampleRate {
		o.audioSampleRate = frame.SampleRate
		o.activeAudioRate = frame.SampleRate
		if o.audioTrack != nil {
			if mc, ok := o.audioTrack.Codec.(*mediatscodecs.MPEG4Audio); ok {
				mc.SampleRate = frame.SampleRate
				mc.ChannelConfig = uint8(DefaultAudioChannels)
				mc.ChannelCount = DefaultAudioChannels
			}
		}
	}

	rawPTS90k := durationTo90k(frame.PTS)
	pts90k := rawPTS90k - o.timelineBase90k
	if pts90k < 0 {
		pts90k = 0
	}
	pts90k = o.normalizeAudioTimestamp90k(pts90k, frame.SampleRate)
	if err := o.currentSegmentWriter.WriteMPEG4Audio(o.audioTrack, pts90k, frame.Payload); err != nil {
		o.DroppedAudioFrames++
		getLogger().Warn("hls destination write audio failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}

	o.TotalAudioFrames++
	o.lastAudioWrite = time.Now()
	if !o.currentSegmentHasTime {
		o.currentSegmentHasTime = true
		o.currentSegmentStart90k = pts90k
		o.currentSegmentStartPTS = ticks90kToDuration(pts90k)
	}
	if pts90k > o.currentSegmentLast90k {
		o.currentSegmentLast90k = pts90k
		o.currentSegmentLastPTS = ticks90kToDuration(pts90k)
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

func (o *hlsLive) handleVideoInputSwitchLocked(frame *shared.Frame) (bool, error) {
	if frame == nil {
		return false, nil
	}
	inputID := frame.InputID
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return false, nil
	}

	if o.currentSegmentInputID == "" {
		o.currentSegmentInputID = inputID
		o.setActiveInputAudioRate(inputID)
		return false, nil
	}

	if o.currentSegmentInputID == inputID {
		return false, nil
	}

	// Keep switch boundary aligned to the first keyframe from the new source.
	// Dropping pre-keyframe switched video avoids decoding artifacts and
	// timeline warnings in downstream HLS consumers.
	if !frame.IsKeyFrame {
		return true, nil
	}
	if !o.canStartSwitchedSegmentLocked(frame) {
		return true, nil
	}

	o.currentSegmentInputID = inputID
	o.setActiveInputAudioRate(inputID)
	if o.currentSegmentWriter == nil {
		return false, nil
	}
	o.forceDiscontinuityNext = true
	return false, o.rotateSegmentLocked(frame.PTS, false)
}

func (o *hlsLive) shouldDropAudioForInputLocked(inputID string) bool {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" || o.currentSegmentInputID == "" {
		return false
	}
	return inputID != o.currentSegmentInputID
}

func (o *hlsLive) canStartSwitchedSegmentLocked(frame *shared.Frame) bool {
	if frame == nil || !frame.IsKeyFrame {
		return false
	}
	videoPayload := o.ensureSPSPPSOnKeyFrame(frame)
	hasSPS, hasPPS := filters.H264SPSPPSPresent(videoPayload)
	return hasSPS && hasPPS
}

func (o *hlsLive) rememberInputAudioRate(inputID string, sampleRate int) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" || sampleRate <= 0 {
		return
	}
	if o.inputAudioRates == nil {
		o.inputAudioRates = make(map[string]int)
	}
	o.inputAudioRates[inputID] = sampleRate
}

func (o *hlsLive) setActiveInputAudioRate(inputID string) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return
	}
	if sampleRate := o.inputAudioRates[inputID]; sampleRate > 0 {
		o.audioSampleRate = sampleRate
		o.activeAudioRate = sampleRate
		return
	}
	o.audioSampleRate = 0
	o.activeAudioRate = 0
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
		audioRate := DefaultAudioRate
		if o.audioSampleRate > 0 {
			audioRate = o.audioSampleRate
		}
		o.videoTrack = &mediats.Track{
			Codec: &mediatscodecs.H264{},
		}
		tracks := []*mediats.Track{o.videoTrack}
		o.audioTrack = &mediats.Track{
			Codec: &mediatscodecs.MPEG4Audio{
				Config: mpeg4audio.Config{
					Type:          mpeg4audio.ObjectTypeAACLC,
					SampleRate:    audioRate,
					ChannelConfig: uint8(DefaultAudioChannels),
					ChannelCount:  DefaultAudioChannels,
				},
			},
		}
		tracks = append(tracks, o.audioTrack)
		o.segmentOutput = &segmentFileOutput{current: f}
		o.currentSegmentWriter = mediats.NewWriter(o.segmentOutput, tracks)
	} else {
		o.segmentOutput.Switch(f)
	}

	if _, err := o.currentSegmentWriter.WriteTables(); err != nil {
		_ = f.Close()
		return err
	}

	o.currentSegmentFile = f
	o.currentSegmentDisco = o.forceDiscontinuityNext
	o.forceDiscontinuityNext = false
	o.currentSegmentStartPTS = 0
	o.currentSegmentLastPTS = 0
	o.currentSegmentStart90k = 0
	o.currentSegmentLast90k = 0
	o.currentSegmentHasTime = false
	if !o.hasTimelineBase90k {
		o.hasTimelineBase90k = true
		o.timelineBase90k = durationTo90k(startPTS)
	}
	o.currentSegmentFileName = fileName
	o.segmentIndex++

	return nil
}

func (o *hlsLive) closeCurrentSegmentLocked(endList bool) error {
	if o.currentSegmentFile == nil {
		return nil
	}

	duration := o.segmentDuration.Seconds()
	if o.currentSegmentHasTime && o.currentSegmentLast90k >= o.currentSegmentStart90k {
		segDur90k := o.currentSegmentLast90k - o.currentSegmentStart90k
		if segDur90k > 0 {
			duration = float64(segDur90k) / 90000.0
		}
	}
	if duration <= 0 {
		duration = o.segmentDuration.Seconds()
	}

	o.entries = append(o.entries, hlsSegmentEntry{
		Seq:           o.segmentIndex - 1,
		FileName:      o.currentSegmentFileName,
		Duration:      duration,
		Discontinuity: o.currentSegmentDisco,
	})
	o.events.Emit(shared.Event{
		Type:       shared.EventTypeSegmentGenerated,
		StreamID:   o.id,
		StreamType: o.Type(),
		Message:    "hls segment generated",
		Meta: shared.SegmentGeneratedMeta{
			Sequence:        o.segmentIndex - 1,
			FileName:        o.currentSegmentFileName,
			SegmentURL:      o.objectURL(o.currentSegmentFileName),
			PlaylistName:    "stream.m3u8",
			PlaylistURL:     o.playlistURL(),
			DurationSeconds: duration,
		},
	})
	if o.isLive && o.playlistSize > 0 && len(o.entries) > o.playlistSize {
		trimmed := o.entries[:len(o.entries)-o.playlistSize]
		for _, entry := range trimmed {
			if entry.Discontinuity {
				o.discontinuitySequence++
			}
		}
		o.entries = o.entries[len(o.entries)-o.playlistSize:]
	}

	errClose := o.currentSegmentFile.Close()
	o.currentSegmentFile = nil
	if endList {
		o.currentSegmentWriter = nil
		o.segmentOutput = nil
		o.videoTrack = nil
		o.audioTrack = nil
	}
	o.currentSegmentFileName = ""
	o.currentSegmentDisco = false
	o.currentSegmentHasTime = false

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
	if len(o.entries) == 0 {
		return nil
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
	if o.discontinuitySequence > 0 {
		b.WriteString(fmt.Sprintf("#EXT-X-DISCONTINUITY-SEQUENCE:%d\n", o.discontinuitySequence))
	}

	prevSeq := mediaSeq - 1
	for _, entry := range o.entries {
		// Mark discontinuities both for true segment sequence gaps and explicit
		// source-switch boundaries.
		if entry.Discontinuity || entry.Seq != prevSeq+1 {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		prevSeq = entry.Seq
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", entry.Duration))
		b.WriteString(entry.FileName + "\n")
	}

	if endList {
		b.WriteString("#EXT-X-ENDLIST\n")
	}

	playlist := b.String()
	return shared.WriteFileAtomic(o.outputFolder, "stream.m3u8", []byte(playlist))
}

func (o *hlsLive) objectURL(fileName string) string {
	return shared.PreferredURL("", o.outputFolder, fileName)
}

func (o *hlsLive) playlistURL() string {
	return o.objectURL("stream.m3u8")
}

func (o *hlsLive) localPath() string {
	path, err := shared.ResolveLocalPath(o.outputFolder)
	if err != nil {
		return ""
	}
	return path
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
	if target > o.targetDuration {
		o.targetDuration = target
	}
	if o.targetDuration < 1 {
		o.targetDuration = 1
	}
	return o.targetDuration
}

func (o *hlsLive) normalizeAudioTimestamp90k(pts int64, sampleRate int) int64 {
	if sampleRate <= 0 {
		sampleRate = o.activeAudioRate
	}
	if sampleRate <= 0 {
		sampleRate = DefaultAudioRate
	}
	if o.audioClockRate != sampleRate {
		o.audioClockRate = sampleRate
		o.audioClockRemainder = 0
	}

	if !o.hasLastAudioPTS90k {
		o.hasLastAudioPTS90k = true
		o.lastAudioPTS90k = pts
		return pts
	}

	expected := o.lastAudioPTS90k + o.nextAACAudioStep90k(sampleRate)
	if pts < expected {
		pts = expected
	}
	if pts <= o.lastAudioPTS90k {
		pts = o.lastAudioPTS90k + 1
	}

	o.lastAudioPTS90k = pts
	return pts
}

func (o *hlsLive) nextAACAudioStep90k(sampleRate int) int64 {
	if sampleRate <= 0 {
		sampleRate = DefaultAudioRate
	}

	numerator := int64(1024*90000) + o.audioClockRemainder
	step := numerator / int64(sampleRate)
	o.audioClockRemainder = numerator % int64(sampleRate)
	if step <= 0 {
		return 1
	}
	return step
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
	o.hasLastVideoPTS90k = true
	o.lastVideoPTS90k = pts
	return pts, dts
}

func (o *hlsLive) ensureSPSPPSOnKeyFrame(frame *shared.Frame) [][]byte {
	if frame == nil || !frame.IsKeyFrame {
		return frame.Payload
	}
	sps, pps := o.h264ParameterSetsForInput(frame.InputID)
	return h264EnsureSPSPPSOnKeyFrame(frame.Payload, true, sps, pps)
}

func (o *hlsLive) cacheH264ParameterSets(inputID string, nalus [][]byte) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return
	}
	if o.cachedSPSByInput == nil {
		o.cachedSPSByInput = make(map[string][]byte)
	}
	if o.cachedPPSByInput == nil {
		o.cachedPPSByInput = make(map[string][]byte)
	}
	sps, pps := h264ExtractSPSPPS(nalus)
	if len(sps) > 0 {
		o.cachedSPSByInput[inputID] = sps
	}
	if len(pps) > 0 {
		o.cachedPPSByInput[inputID] = pps
	}
}

func (o *hlsLive) h264ParameterSetsForInput(inputID string) ([]byte, []byte) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return nil, nil
	}
	return cloneBytes(o.cachedSPSByInput[inputID]), cloneBytes(o.cachedPPSByInput[inputID])
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

func extractH264ParamsFromAccessUnitForHLS(nalus [][]byte) ([]byte, []byte) {
	return h264ExtractSPSPPS(nalus)
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

	frameSPS, framePPS := extractH264ParamsFromAccessUnitForHLS(nalus)
	if len(frameSPS) == 0 {
		frameSPS = cloneBytes(cachedSPS)
	}
	if len(framePPS) == 0 {
		framePPS = cloneBytes(cachedPPS)
	}

	out := make([][]byte, 0, len(nalus)+2)
	if len(frameSPS) > 0 {
		out = append(out, cloneBytes(frameSPS))
	}
	if len(framePPS) > 0 {
		out = append(out, cloneBytes(framePPS))
	}

	for _, nalu := range nalus {
		switch h264NALTypeFromUnit(nalu) {
		case 7, 8:
			continue
		default:
			out = append(out, cloneBytes(nalu))
		}
	}

	if len(out) == 0 {
		return nalus
	}
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
