package outputs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/tupicapp/restreamer/core/shared"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	mediats "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts"
	mediatscodecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts/codecs"
	"go.uber.org/zap"
)

type hlsBuiltSegment struct {
	Seq           int
	FileName      string
	Duration      float64
	Discontinuity bool
	Data          []byte
}

type segmentBufferOutput struct {
	current *bytes.Buffer
}

func (o *segmentBufferOutput) Write(p []byte) (int, error) {
	if o.current == nil {
		return 0, errors.New("segment buffer output is closed")
	}
	return o.current.Write(p)
}

func (o *segmentBufferOutput) Switch(buf *bytes.Buffer) {
	o.current = buf
}

type hlsLiveAsync struct {
	id           string
	url          string
	outputFolder shared.Folder

	gopBuffer *hlsGOPBuffer

	done    chan struct{}
	Started chan struct{}

	closeOnce sync.Once
	runWg     sync.WaitGroup

	stateMu sync.RWMutex

	isStarted   bool
	isInitiated bool

	isLive          bool
	cleanInterval   time.Duration
	segmentDuration time.Duration
	playlistSize    int
	targetDuration  int

	uploadCh chan *hlsBuiltSegment

	segmentIndex int
	entries      []hlsSegmentEntry

	discontinuitySequence int

	currentSegmentFileName string

	currentSegmentDisco    bool
	currentSegmentStartPTS time.Duration
	currentSegmentLastPTS  time.Duration
	currentSegmentStart90k int64
	currentSegmentLast90k  int64
	currentSegmentHasTime  bool
	currentSegmentHasAudio bool
	forceDiscontinuityNext bool
	currentSegmentInputID  string

	currentSegmentBuffer *bytes.Buffer
	currentSegmentOutput *segmentBufferOutput
	currentSegmentWriter *mediats.Writer

	hasTimelineBase90k bool
	timelineBase90k    int64

	videoTrack *mediats.Track
	audioTrack *mediats.Track
	cachedSPS  []byte
	cachedPPS  []byte

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

	hasLastVideoPTS90k bool
	lastVideoPTS90k    int64
	hasLastVideoDTS90k bool
	lastVideoDTS90k    int64

	audioSampleRate   int
	activeAudioRate   int
	inputAudioRates   map[string]int
	pendingStartAudio []*shared.Frame
	events            *shared.EventEmitter
}

const maxPendingHLSStartAudio = 256

func newHLSLiveDestinationAsync(id string, outputFolder any, opts ...HLSLiveOption) (shared.Stream, error) {
	folder, err := shared.AdaptFolder(outputFolder)
	if err != nil || folder == nil {
		return nil, fmt.Errorf("hls destination requires output folder")
	}

	legacy := &hlsLive{
		segmentDuration: defaultHLSSegmentDuration,
		playlistSize:    defaultHLSPlaylistSize,
		targetDuration:  defaultHLSTargetDuration,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(legacy)
		}
	}

	dest := &hlsLiveAsync{
		id:              id,
		url:             id,
		outputFolder:    folder,
		gopBuffer:       newHLSGOPBuffer(),
		done:            make(chan struct{}),
		Started:         make(chan struct{}),
		isLive:          legacy.isLive,
		cleanInterval:   legacy.cleanInterval,
		segmentDuration: legacy.segmentDuration,
		playlistSize:    legacy.playlistSize,
		targetDuration:  legacy.targetDuration,
		uploadCh:        make(chan *hlsBuiltSegment, defaultHLSPlaylistSize+2),
		events:          shared.NewEventEmitter(256),
	}

	return dest, nil
}

func (o *hlsLiveAsync) GetVideoChan() chan *shared.Frame      { return o.gopBuffer.VideoFrameChan }
func (o *hlsLiveAsync) GetAudioChan() chan *shared.Frame      { return o.gopBuffer.AudioFrameChan }
func (o *hlsLiveAsync) GetID() string                         { return o.id }
func (o *hlsLiveAsync) Type() string                          { return "writer" }
func (o *hlsLiveAsync) AddSidecars(sidecars ...shared.Stream) {}
func (o *hlsLiveAsync) IsRestartable() bool                   { return false }
func (o *hlsLiveAsync) RestartInterval() time.Duration        { return 0 }

func (o *hlsLiveAsync) Clone() (shared.Stream, error) {
	return nil, errors.New("hls destination cannot be cloned")
}

func (o *hlsLiveAsync) WaitForStart(ctx context.Context) error {
	select {
	case <-o.Started:
		return nil
	case <-o.done:
		return errors.New("stream is closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *hlsLiveAsync) EventChan() chan shared.Event {
	if o.events == nil {
		return nil
	}
	return o.events.Chan()
}

func (o *hlsLiveAsync) OnSwitch() {
	if o.gopBuffer != nil {
		o.gopBuffer.OnSwitch()
	}
}

func (o *hlsLiveAsync) Start() {
	o.stateMu.Lock()
	defer o.stateMu.Unlock()

	if o.isInitiated {
		o.isStarted = true
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "hls destination resumed", Meta: shared.StreamLifecycleMeta{URL: o.playlistURL()}})
		return
	}

	o.isInitiated = true
	o.isStarted = true

	if err := o.outputFolder.RemoveAll(); err != nil {
		getLogger().Error("hls destination remove dir failed", zap.String("path", o.localPath()), zap.Error(err))
		o.isStarted = false
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

	o.runWg.Add(3)
	go o.runBuilder()
	go o.runUploader()
	go func() {
		defer o.runWg.Done()
		o.gopBuffer.Run()
	}()
}

func (o *hlsLiveAsync) Stop() {
	o.stateMu.Lock()
	o.isStarted = false
	o.stateMu.Unlock()
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: o.id, StreamType: o.Type(), Message: "hls destination stopped"})
}

func (o *hlsLiveAsync) Close() {
	o.Stop()
	o.closeOnce.Do(func() {
		if o.gopBuffer != nil {
			o.gopBuffer.Close()
		}
		close(o.done)
		o.runWg.Wait()
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: o.id, StreamType: o.Type(), Message: "hls destination closed"})
		o.events.Close()
	})
}

func (o *hlsLiveAsync) State() *shared.State {
	o.stateMu.RLock()
	defer o.stateMu.RUnlock()

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

func (o *hlsLiveAsync) runBuilder() {
	defer o.runWg.Done()
	defer close(o.uploadCh)

	for frame := range o.gopBuffer.GetReadyChan() {
		if !o.started() {
			continue
		}
		if frame == nil {
			continue
		}
		if frame.Codec == "h264" || frame.Codec == "h265" {
			o.handleVideoFrame(frame)
			continue
		}
		if frame.Codec == "aac" || frame.Codec == "opus" {
			o.handleAudioFrame(frame)
		}
	}

	if seg, err := o.buildCurrentSegmentLocked(true); err != nil {
		getLogger().Warn("hls destination flush segment failed", zap.String("output_id", o.id), zap.Error(err))
	} else if seg != nil {
		o.uploadCh <- seg
	}
}

func (o *hlsLiveAsync) runUploader() {
	defer o.runWg.Done()

	for seg := range o.uploadCh {
		if seg == nil {
			continue
		}
		if err := o.uploadSegment(seg); err != nil {
			getLogger().Warn("hls destination upload failed", zap.String("output_id", o.id), zap.Error(err), zap.Int("sequence", seg.Seq))
		}
	}

	if err := o.writePlaylistLocked(true); err != nil {
		getLogger().Warn("hls destination final playlist write failed", zap.String("output_id", o.id), zap.Error(err))
	}
}

func (o *hlsLiveAsync) started() bool {
	o.stateMu.RLock()
	defer o.stateMu.RUnlock()
	return o.isStarted
}

func (o *hlsLiveAsync) handleVideoFrame(frame *shared.Frame) {
	o.cacheH264ParameterSets(frame.Payload)
	if shouldDrop, err := o.handleVideoDiscontinuityLocked(frame); err != nil {
		o.stateMu.Lock()
		o.DroppedVideoFrames++
		o.stateMu.Unlock()
		getLogger().Warn("hls destination switch rotate failed", zap.String("output_id", o.id), zap.Error(err))
		return
	} else if shouldDrop {
		o.stateMu.Lock()
		o.DroppedVideoFrames++
		o.stateMu.Unlock()
		return
	}

	startPTS := frame.PTS
	if o.currentSegmentWriter == nil {
		startPTS = o.pendingStartPTSForInput(frame.InputID, startPTS)
	}
	if err := o.ensureSegmentLocked(startPTS); err != nil {
		o.stateMu.Lock()
		o.DroppedVideoFrames++
		o.stateMu.Unlock()
		getLogger().Warn("hls destination ensure segment failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}
	if err := o.flushPendingStartAudio(frame.InputID); err != nil {
		o.stateMu.Lock()
		o.DroppedVideoFrames++
		o.stateMu.Unlock()
		getLogger().Warn("hls destination flush startup audio failed", zap.String("output_id", o.id), zap.Error(err))
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
		if !o.shouldDelaySegmentRotationForAudioLocked(dts90k) {
			if err := o.rotateSegmentLocked(frame.PTS, false); err != nil {
				o.stateMu.Lock()
				o.DroppedVideoFrames++
				o.stateMu.Unlock()
				getLogger().Warn("hls destination rotate failed", zap.String("output_id", o.id), zap.Error(err))
				return
			}
			o.currentSegmentHasTime = false
		}
	}

	videoPayload := o.ensureSPSPPSOnKeyFrame(frame)
	if frame.IsKeyFrame {
		hasSPS, hasPPS := h264SPSPPSPresent(videoPayload)
		if !hasSPS || !hasPPS {
			o.stateMu.Lock()
			o.DroppedVideoFrames++
			o.stateMu.Unlock()
			getLogger().Warn("hls destination drop keyframe without SPS/PPS",
				zap.String("output_id", o.id),
				zap.Int64("sequence_id", frame.SequenceID),
				zap.Bool("has_sps", hasSPS),
				zap.Bool("has_pps", hasPPS))
			return
		}
	}

	if err := o.currentSegmentWriter.WriteH264(o.videoTrack, pts90k, dts90k, videoPayload); err != nil {
		o.stateMu.Lock()
		o.DroppedVideoFrames++
		o.stateMu.Unlock()
		getLogger().Warn("hls destination write video failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}

	o.stateMu.Lock()
	o.TotalVideoFrames++
	o.lastVideoWrite = time.Now()
	o.stateMu.Unlock()

	if !o.currentSegmentHasTime {
		o.currentSegmentHasTime = true
		o.currentSegmentStart90k = dts90k
		o.currentSegmentStartPTS = ticks90kToDuration(dts90k)
	}
	o.currentSegmentLast90k = pts90k
	o.currentSegmentLastPTS = ticks90kToDuration(pts90k)
}

func (o *hlsLiveAsync) handleAudioFrame(frame *shared.Frame) {
	o.rememberInputAudioRate(frame.InputID, frame.SampleRate)
	if o.shouldDropAudioForInputLocked(frame.InputID) {
		o.stateMu.Lock()
		o.DroppedAudioFrames++
		o.stateMu.Unlock()
		return
	}

	if o.currentSegmentWriter == nil {
		o.bufferPendingStartAudio(frame)
		return
	}

	if err := o.writeAudioFrame(frame); err != nil {
		o.stateMu.Lock()
		o.DroppedAudioFrames++
		o.stateMu.Unlock()
		getLogger().Warn("hls destination write audio failed", zap.String("output_id", o.id), zap.Error(err))
		return
	}
}

func (o *hlsLiveAsync) writeAudioFrame(frame *shared.Frame) error {
	if frame == nil {
		return nil
	}

	if err := o.ensureSegmentLocked(frame.PTS); err != nil {
		return err
	}

	if len(frame.Payload) == 0 {
		return nil
	}

	if frame.SampleRate > 0 && o.activeAudioRate != frame.SampleRate {
		o.audioSampleRate = frame.SampleRate
		o.activeAudioRate = frame.SampleRate
		if o.audioTrack != nil {
			if mc, ok := o.audioTrack.Codec.(*mediatscodecs.MPEG4Audio); ok {
				mc.Config.SampleRate = frame.SampleRate
				mc.Config.ChannelConfig = uint8(DefaultAudioChannels)
				mc.Config.ChannelCount = DefaultAudioChannels
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
		return err
	}

	o.stateMu.Lock()
	o.TotalAudioFrames++
	o.lastAudioWrite = time.Now()
	o.stateMu.Unlock()

	if !o.currentSegmentHasTime {
		o.currentSegmentHasTime = true
		o.currentSegmentStart90k = pts90k
		o.currentSegmentStartPTS = ticks90kToDuration(pts90k)
	}
	if pts90k > o.currentSegmentLast90k {
		o.currentSegmentLast90k = pts90k
		o.currentSegmentLastPTS = ticks90kToDuration(pts90k)
	}
	o.currentSegmentHasAudio = true
	return nil
}

func (o *hlsLiveAsync) ensureSegmentLocked(pts time.Duration) error {
	if o.currentSegmentWriter != nil {
		return nil
	}
	return o.openSegmentLocked(pts)
}

func (o *hlsLiveAsync) rotateSegmentLocked(nextPTS time.Duration, endList bool) error {
	if err := o.closeCurrentSegmentLocked(endList); err != nil {
		return err
	}
	return o.openSegmentLocked(nextPTS)
}

func (o *hlsLiveAsync) openSegmentLocked(startPTS time.Duration) error {
	buf := &bytes.Buffer{}

	if o.currentSegmentWriter == nil {
		audioRate := DefaultAudioRate
		if o.audioSampleRate > 0 {
			audioRate = o.audioSampleRate
		}
		o.videoTrack = &mediats.Track{Codec: &mediatscodecs.H264{}}
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
		o.currentSegmentOutput = &segmentBufferOutput{current: buf}
		o.currentSegmentWriter = mediats.NewWriter(o.currentSegmentOutput, []*mediats.Track{o.videoTrack, o.audioTrack})
	} else {
		o.currentSegmentOutput.Switch(buf)
	}

	if _, err := o.currentSegmentWriter.WriteTables(); err != nil {
		return err
	}

	o.currentSegmentBuffer = buf
	o.currentSegmentFileName = fmt.Sprintf("seg_%06d.ts", o.segmentIndex)
	o.currentSegmentDisco = o.forceDiscontinuityNext
	o.forceDiscontinuityNext = false
	o.currentSegmentStartPTS = 0
	o.currentSegmentLastPTS = 0
	o.currentSegmentStart90k = 0
	o.currentSegmentLast90k = 0
	o.currentSegmentHasTime = false
	o.currentSegmentHasAudio = false
	if !o.hasTimelineBase90k {
		o.hasTimelineBase90k = true
		o.timelineBase90k = durationTo90k(startPTS)
	}
	o.segmentIndex++

	return nil
}

func (o *hlsLiveAsync) buildCurrentSegmentLocked(endList bool) (*hlsBuiltSegment, error) {
	if o.currentSegmentBuffer == nil {
		if endList {
			o.currentSegmentWriter = nil
			o.currentSegmentOutput = nil
			o.videoTrack = nil
			o.audioTrack = nil
		}
		return nil, nil
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

	seg := &hlsBuiltSegment{
		Seq:           o.segmentIndex - 1,
		FileName:      o.currentSegmentFileName,
		Duration:      duration,
		Discontinuity: o.currentSegmentDisco,
		Data:          bytes.Clone(o.currentSegmentBuffer.Bytes()),
	}

	o.currentSegmentBuffer = nil
	o.currentSegmentFileName = ""
	o.currentSegmentDisco = false
	o.currentSegmentHasTime = false
	o.currentSegmentHasAudio = false

	if endList {
		o.currentSegmentWriter = nil
		o.currentSegmentOutput = nil
		o.videoTrack = nil
		o.audioTrack = nil
	}

	return seg, nil
}

func (o *hlsLiveAsync) closeCurrentSegmentLocked(endList bool) error {
	seg, err := o.buildCurrentSegmentLocked(endList)
	if err != nil {
		return err
	}
	if seg == nil {
		return o.writePlaylistLocked(endList)
	}
	if err := o.uploadSegment(seg); err != nil {
		return err
	}
	if endList {
		return o.writePlaylistLocked(true)
	}
	return nil
}

func (o *hlsLiveAsync) uploadSegment(seg *hlsBuiltSegment) error {
	if seg == nil {
		return nil
	}
	if o.outputFolder == nil {
		return fmt.Errorf("hls destination output folder is nil")
	}

	var expirationTime *time.Time
	if o.isLive {
		expiresAt := time.Now().Add(hlsLiveSegmentExpiration)
		expirationTime = &expiresAt
	}

	f, err := o.outputFolder.Create(seg.FileName, expirationTime)
	if err != nil {
		return err
	}

	_, writeErr := f.Write(seg.Data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}

	o.entries = append(o.entries, hlsSegmentEntry{
		Seq:           seg.Seq,
		FileName:      seg.FileName,
		Duration:      seg.Duration,
		Discontinuity: seg.Discontinuity,
	})
	o.events.Emit(shared.Event{
		Type:       shared.EventTypeSegmentGenerated,
		StreamID:   o.id,
		StreamType: o.Type(),
		Message:    "hls segment generated",
		Meta: shared.SegmentGeneratedMeta{
			Sequence:        seg.Seq,
			FileName:        seg.FileName,
			SegmentURL:      o.objectURL(seg.FileName),
			PlaylistName:    "stream.m3u8",
			PlaylistURL:     o.playlistURL(),
			DurationSeconds: seg.Duration,
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

	return o.writePlaylistLocked(false)
}

func (o *hlsLiveAsync) writePlaylistLocked(endList bool) error {
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

	return shared.WriteFileAtomic(o.outputFolder, "stream.m3u8", []byte(b.String()))
}

func (o *hlsLiveAsync) objectURL(fileName string) string {
	return shared.PreferredURL("", o.outputFolder, fileName)
}

func (o *hlsLiveAsync) playlistURL() string {
	return o.objectURL("stream.m3u8")
}

func (o *hlsLiveAsync) localPath() string {
	path, err := shared.ResolveLocalPath(o.outputFolder)
	if err != nil {
		return ""
	}
	return path
}

func (o *hlsLiveAsync) computeTargetDuration() int {
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

func (o *hlsLiveAsync) shouldDelaySegmentRotationForAudioLocked(nextDTS90k int64) bool {
	if o.currentSegmentHasAudio {
		return false
	}

	o.stateMu.RLock()
	totalAudioFrames := o.TotalAudioFrames
	o.stateMu.RUnlock()

	if !o.currentSegmentDisco && totalAudioFrames == 0 && o.activeAudioRate == 0 {
		return false
	}

	elapsed90k := nextDTS90k - o.currentSegmentStart90k
	if elapsed90k < 0 {
		return false
	}

	return elapsed90k < durationTo90k(2*o.segmentDuration)
}

func (o *hlsLiveAsync) handleVideoDiscontinuityLocked(frame *shared.Frame) (bool, error) {
	if frame == nil {
		return false, nil
	}

	inputID := strings.TrimSpace(frame.InputID)
	if inputID == "" {
		return false, nil
	}

	if o.currentSegmentInputID == "" {
		o.currentSegmentInputID = inputID
		o.setActiveInputAudioRate(inputID)
		return false, nil
	}

	if !frame.Discontinuity {
		return inputID != o.currentSegmentInputID, nil
	}

	o.currentSegmentInputID = inputID
	o.setActiveInputAudioRate(inputID)
	if o.currentSegmentWriter == nil {
		return false, nil
	}

	o.forceDiscontinuityNext = true
	return false, o.rotateSegmentLocked(frame.PTS, false)
}

func (o *hlsLiveAsync) shouldDropAudioForInputLocked(inputID string) bool {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" || o.currentSegmentInputID == "" {
		return false
	}
	return inputID != o.currentSegmentInputID
}

func (o *hlsLiveAsync) rememberInputAudioRate(inputID string, sampleRate int) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" || sampleRate <= 0 {
		return
	}
	if o.inputAudioRates == nil {
		o.inputAudioRates = make(map[string]int)
	}
	o.inputAudioRates[inputID] = sampleRate
}

func (o *hlsLiveAsync) setActiveInputAudioRate(inputID string) {
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

func (o *hlsLiveAsync) bufferPendingStartAudio(frame *shared.Frame) {
	if frame == nil {
		return
	}

	cloned := cloneSharedFrame(frame)
	if cloned == nil {
		return
	}
	if len(o.pendingStartAudio) >= maxPendingHLSStartAudio {
		copy(o.pendingStartAudio, o.pendingStartAudio[1:])
		o.pendingStartAudio = o.pendingStartAudio[:maxPendingHLSStartAudio-1]
	}
	o.pendingStartAudio = append(o.pendingStartAudio, cloned)
}

func (o *hlsLiveAsync) pendingStartPTSForInput(inputID string, fallback time.Duration) time.Duration {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return fallback
	}

	start := fallback
	for _, frame := range o.pendingStartAudio {
		if frame == nil || strings.TrimSpace(frame.InputID) != inputID {
			continue
		}
		if frame.PTS < start {
			start = frame.PTS
		}
	}
	return start
}

func (o *hlsLiveAsync) flushPendingStartAudio(inputID string) error {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" || len(o.pendingStartAudio) == 0 {
		return nil
	}

	pending := o.pendingStartAudio
	o.pendingStartAudio = nil
	for _, frame := range pending {
		if frame == nil {
			continue
		}
		if strings.TrimSpace(frame.InputID) != inputID {
			o.pendingStartAudio = append(o.pendingStartAudio, frame)
			continue
		}
		if err := o.writeAudioFrame(frame); err != nil {
			return err
		}
	}
	return nil
}

func (o *hlsLiveAsync) normalizeAudioTimestamp90k(pts int64, sampleRate int) int64 {
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

	step := o.nextAACAudioStep90k(sampleRate)
	expected := o.lastAudioPTS90k + step
	if pts+1 < expected {
		pts = expected
	}
	maxForward := expected + 3*step
	if pts > maxForward {
		pts = maxForward
	}
	if pts <= o.lastAudioPTS90k {
		pts = o.lastAudioPTS90k + 1
	}

	o.lastAudioPTS90k = pts
	return pts
}

func (o *hlsLiveAsync) nextAACAudioStep90k(sampleRate int) int64 {
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

func (o *hlsLiveAsync) normalizeVideoTimestamps90k(pts, dts int64) (int64, int64) {
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

func (o *hlsLiveAsync) ensureSPSPPSOnKeyFrame(frame *shared.Frame) [][]byte {
	if frame == nil || !frame.IsKeyFrame {
		return frame.Payload
	}
	sps, pps := o.h264ParameterSets()
	return h264EnsureSPSPPSOnKeyFrame(frame.Payload, true, sps, pps)
}

func (o *hlsLiveAsync) cacheH264ParameterSets(nalus [][]byte) {
	sps, pps := h264ExtractSPSPPS(nalus)
	if len(sps) > 0 {
		o.cachedSPS = sps
	}
	if len(pps) > 0 {
		o.cachedPPS = pps
	}
}

func (o *hlsLiveAsync) h264ParameterSets() ([]byte, []byte) {
	return cloneBytes(o.cachedSPS), cloneBytes(o.cachedPPS)
}

func cloneSharedFrame(frame *shared.Frame) *shared.Frame {
	if frame == nil {
		return nil
	}
	cloned := *frame
	if len(frame.Payload) > 0 {
		cloned.Payload = make([][]byte, 0, len(frame.Payload))
		for _, nalu := range frame.Payload {
			cloned.Payload = append(cloned.Payload, cloneBytes(nalu))
		}
	}
	return &cloned
}
