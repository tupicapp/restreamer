package inputs

import (
	"context"
	"sync"
	"time"
)

const (
	defaultCompatAudioTimeout  = 250 * time.Millisecond
	defaultCompatVideoTimeout  = 250 * time.Millisecond
	defaultCompatAudioInterval = time.Duration(1024) * time.Second / DefaultAudioRate
	defaultCompatVideoInterval = 33 * time.Millisecond
)

var defaultCompatVideoTemplate = &Frame{
	Payload: [][]byte{
		{0x67, 0x42, 0x00, 0x1f, 0xe5, 0x88, 0x68, 0x50, 0x1e, 0xd0},
		{0x68, 0xce, 0x06, 0xe2},
		{0x65, 0x88, 0x84, 0x00, 0x00, 0x00},
	},
	Codec:      "h264",
	PacketType: "I",
	IsKeyFrame: true,
	IsFile:     false,
}

type CompatInputOption func(*compatInputConfig)

type compatInputConfig struct {
	audioTimeout     time.Duration
	videoTimeout     time.Duration
	audioInterval    time.Duration
	videoInterval    time.Duration
	runtimeDetection bool
}

func WithCompatAudioTimeout(d time.Duration) CompatInputOption {
	return func(cfg *compatInputConfig) {
		if d > 0 {
			cfg.audioTimeout = d
		}
	}
}

func WithCompatVideoTimeout(d time.Duration) CompatInputOption {
	return func(cfg *compatInputConfig) {
		if d > 0 {
			cfg.videoTimeout = d
		}
	}
}

func WithCompatAudioInterval(d time.Duration) CompatInputOption {
	return func(cfg *compatInputConfig) {
		if d > 0 {
			cfg.audioInterval = d
		}
	}
}

func WithCompatVideoInterval(d time.Duration) CompatInputOption {
	return func(cfg *compatInputConfig) {
		if d > 0 {
			cfg.videoInterval = d
		}
	}
}

func WithCompatRuntimeDetection(enabled bool) CompatInputOption {
	return func(cfg *compatInputConfig) {
		cfg.runtimeDetection = enabled
	}
}

type compatInputStream struct {
	id     string
	source Stream
	cfg    compatInputConfig

	videoChan chan *Frame
	audioChan chan *Frame

	done      chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	runWG     sync.WaitGroup

	activeMu sync.RWMutex
	active   bool

	trackInfoMu sync.RWMutex
	trackInfo   InputTrackInfo
	trackInfoAt time.Time

	sequenceMu     sync.Mutex
	videoSequence  int64
	audioSequence  int64
	lastVideoGOPID int64

	templateMu      sync.RWMutex
	lastVideoKeyAU  *Frame
	lastAudioAU     *Frame

	timingMu           sync.RWMutex
	lastVideoPTS       time.Duration
	lastAudioPTS       time.Duration
	lastRealVideoAt    time.Time
	lastRealAudioAt    time.Time
	nextSyntheticVideo time.Time
	nextSyntheticAudio time.Time
}

func NewCompatibleInput(source Stream, opts ...CompatInputOption) Stream {
	if source == nil {
		return nil
	}

	cfg := compatInputConfig{
		audioTimeout:     defaultCompatAudioTimeout,
		videoTimeout:     defaultCompatVideoTimeout,
		audioInterval:    defaultCompatAudioInterval,
		videoInterval:    defaultCompatVideoInterval,
		runtimeDetection: true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	c := &compatInputStream{
		id:        source.GetID(),
		source:    source,
		cfg:       cfg,
		videoChan: make(chan *Frame, 256),
		audioChan: make(chan *Frame, 256),
		done:      make(chan struct{}),
	}

	if provider, ok := source.(TrackInfoProvider); ok {
		c.applyTrackInfo(provider.TrackInfoSnapshot())
	}

	return c
}

func (c *compatInputStream) GetVideoChan() chan *Frame { return c.videoChan }

func (c *compatInputStream) GetAudioChan() chan *Frame { return c.audioChan }

func (c *compatInputStream) GetID() string { return c.id }

func (c *compatInputStream) Type() string { return c.source.Type() }

func (c *compatInputStream) Start() {
	c.startOnce.Do(func() {
		c.runWG.Add(4)
		go c.forwardVideo()
		go c.forwardAudio()
		go c.trackInfoLoop()
		go c.syntheticLoop()
	})
	c.setActive(true)
	c.source.Start()
}

func (c *compatInputStream) Stop() {
	c.setActive(false)
	c.source.Stop()
}

func (c *compatInputStream) Close() {
	c.closeOnce.Do(func() {
		c.flushSyntheticBeforeClose()
		close(c.done)
		c.source.Close()
		c.runWG.Wait()
		close(c.videoChan)
		close(c.audioChan)
	})
}

func (c *compatInputStream) State() *State {
	return c.source.State()
}

func (c *compatInputStream) Clone() (Stream, error) {
	cloned, err := c.source.Clone()
	if err != nil {
		return nil, err
	}
	return NewCompatibleInput(
		cloned,
		WithCompatAudioTimeout(c.cfg.audioTimeout),
		WithCompatVideoTimeout(c.cfg.videoTimeout),
		WithCompatAudioInterval(c.cfg.audioInterval),
		WithCompatVideoInterval(c.cfg.videoInterval),
		WithCompatRuntimeDetection(c.cfg.runtimeDetection),
	), nil
}

func (c *compatInputStream) WaitForStart(ctx context.Context) error {
	return c.source.WaitForStart(ctx)
}

func (c *compatInputStream) IsRestartable() bool {
	return c.source.IsRestartable()
}

func (c *compatInputStream) RestartInterval() time.Duration {
	return c.source.RestartInterval()
}

func (c *compatInputStream) EventChan() chan Event {
	return c.source.EventChan()
}

func (c *compatInputStream) ShouldPauseWhenInactive() bool {
	return false
}

func (c *compatInputStream) forwardVideo() {
	defer c.runWG.Done()

	for {
		select {
		case <-c.done:
			return
		case frame, ok := <-c.source.GetVideoChan():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}

			out := c.prepareVideoFrame(frame, false)
			if out == nil {
				continue
			}
			c.cacheRealVideoTemplate(out)
			if !c.isActive() {
				continue
			}

			c.timingMu.Lock()
			c.lastRealVideoAt = time.Now()
			c.nextSyntheticVideo = c.lastRealVideoAt.Add(c.cfg.videoTimeout)
			c.timingMu.Unlock()

			if !c.emitVideo(out) {
				return
			}
		}
	}
}

func (c *compatInputStream) forwardAudio() {
	defer c.runWG.Done()

	for {
		select {
		case <-c.done:
			return
		case frame, ok := <-c.source.GetAudioChan():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}

			out := c.prepareAudioFrame(frame, false)
			if out == nil {
				continue
			}
			c.cacheRealAudioTemplate(out)
			if !c.isActive() {
				continue
			}

			c.timingMu.Lock()
			c.lastRealAudioAt = time.Now()
			c.nextSyntheticAudio = c.lastRealAudioAt.Add(c.cfg.audioTimeout)
			c.timingMu.Unlock()

			if !c.emitAudio(out) {
				return
			}
		}
	}
}

func (c *compatInputStream) trackInfoLoop() {
	defer c.runWG.Done()

	provider, ok := c.source.(TrackInfoProvider)
	if !ok {
		return
	}

	for {
		select {
		case <-c.done:
			return
		case info, ok := <-provider.TrackInfoChan():
			if !ok {
				return
			}
			c.applyTrackInfo(info)
		}
	}
}

func (c *compatInputStream) syntheticLoop() {
	defer c.runWG.Done()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.emitSyntheticIfNeeded(time.Now())
		}
	}
}

func (c *compatInputStream) emitSyntheticIfNeeded(now time.Time) {
	if !c.isActive() {
		return
	}

	info, infoAt, ok := c.trackInfoSnapshot()
	if !ok {
		return
	}

	c.emitSyntheticVideoBurst(info, infoAt, now)
	c.emitSyntheticAudioBurst(info, infoAt, now)
}

func (c *compatInputStream) shouldGenerateVideo(info InputTrackInfo, infoAt, now time.Time) bool {
	if !info.Initialized {
		return false
	}

	c.timingMu.RLock()
	lastReal := c.lastRealVideoAt
	nextSynthetic := c.nextSyntheticVideo
	c.timingMu.RUnlock()

	if nextSynthetic.IsZero() {
		if !info.HasVideo {
			return true
		}
		if !c.cfg.runtimeDetection {
			return false
		}
		if infoAt.IsZero() {
			return false
		}
		return now.Sub(maxCompatTime(lastReal, infoAt)) >= c.cfg.videoTimeout
	}

	if !c.cfg.runtimeDetection && info.HasVideo {
		return false
	}

	return !now.Before(nextSynthetic)
}

func (c *compatInputStream) shouldGenerateAudio(info InputTrackInfo, infoAt, now time.Time) bool {
	if !info.Initialized {
		return false
	}

	c.timingMu.RLock()
	lastReal := c.lastRealAudioAt
	nextSynthetic := c.nextSyntheticAudio
	c.timingMu.RUnlock()

	if nextSynthetic.IsZero() {
		if !info.HasAudio {
			return true
		}
		if !c.cfg.runtimeDetection {
			return false
		}
		if infoAt.IsZero() {
			return false
		}
		return now.Sub(maxCompatTime(lastReal, infoAt)) >= c.cfg.audioTimeout
	}

	if !c.cfg.runtimeDetection && info.HasAudio {
		return false
	}

	return !now.Before(nextSynthetic)
}

func (c *compatInputStream) emitSyntheticVideoBurst(info InputTrackInfo, infoAt, now time.Time) {
	for i := 0; i < 8 && c.shouldGenerateVideo(info, infoAt, now); i++ {
		if frame := c.syntheticVideoFrame(c.syntheticFrameTime(true, info, infoAt, now)); frame != nil {
			if !c.emitVideo(frame) {
				return
			}
		} else {
			return
		}
	}
}

func (c *compatInputStream) emitSyntheticAudioBurst(info InputTrackInfo, infoAt, now time.Time) {
	for i := 0; i < 32 && c.shouldGenerateAudio(info, infoAt, now); i++ {
		if frame := c.syntheticAudioFrame(c.syntheticFrameTime(false, info, infoAt, now)); frame != nil {
			if !c.emitAudio(frame) {
				return
			}
		} else {
			return
		}
	}
}

func (c *compatInputStream) syntheticFrameTime(video bool, info InputTrackInfo, infoAt, now time.Time) time.Time {
	c.timingMu.RLock()
	lastRealVideo := c.lastRealVideoAt
	lastRealAudio := c.lastRealAudioAt
	nextVideo := c.nextSyntheticVideo
	nextAudio := c.nextSyntheticAudio
	c.timingMu.RUnlock()

	if video {
		if !nextVideo.IsZero() {
			return nextVideo
		}
		if !info.HasVideo {
			return now
		}
		if base := maxCompatTime(lastRealVideo, infoAt); !base.IsZero() {
			return base.Add(c.cfg.videoTimeout)
		}
		return now
	}

	if !nextAudio.IsZero() {
		return nextAudio
	}
	if !info.HasAudio {
		return now
	}
	if base := maxCompatTime(lastRealAudio, infoAt); !base.IsZero() {
		return base.Add(c.cfg.audioTimeout)
	}
	return now
}

func (c *compatInputStream) syntheticVideoFrame(now time.Time) *Frame {
	template := c.videoTemplate()
	if template == nil {
		template = cloneCompatFrame(defaultCompatVideoTemplate)
	}
	if template == nil {
		return nil
	}
	template.Timestamp = now
	template.InputID = c.id
	return c.prepareVideoFrame(template, true)
}

func (c *compatInputStream) syntheticAudioFrame(now time.Time) *Frame {
	if template := c.audioTemplate(); template != nil {
		template.Timestamp = now
		template.InputID = c.id
		return c.prepareAudioFrame(template, true)
	}

	frame := &Frame{
		Payload:    [][]byte{{33, 16, 4, 96, 140, 28}},
		Codec:      "aac",
		PacketType: "",
		Timestamp:  now,
		InputID:    c.id,
		IsKeyFrame: true,
		IsFile:     false,
		SampleRate: c.syntheticAudioSampleRate(),
	}
	return c.prepareAudioFrame(frame, true)
}

func (c *compatInputStream) prepareVideoFrame(frame *Frame, synthetic bool) *Frame {
	out := cloneCompatFrame(frame)
	if out == nil {
		return nil
	}

	c.sequenceMu.Lock()
	defer c.sequenceMu.Unlock()

	c.videoSequence++
	out.SequenceID = c.videoSequence
	if out.InputID == "" {
		out.InputID = c.id
	}
	if out.IsKeyFrame {
		c.lastVideoGOPID = out.SequenceID
	}
	out.GOPID = c.lastVideoGOPID
	if out.GOPID == 0 {
		out.GOPID = out.SequenceID
	}

	c.timingMu.Lock()
	defer c.timingMu.Unlock()

	if synthetic {
		pts := c.nextPTSLocked(true, c.cfg.videoInterval)
		out.PTS = pts
		out.DTS = pts
		out.Duration = c.cfg.videoInterval
		out.PacketType = "I"
		c.nextSyntheticVideo = out.Timestamp.Add(c.cfg.videoInterval)
	} else {
		if out.Timestamp.IsZero() {
			out.Timestamp = time.Now()
		}
		c.nextSyntheticVideo = out.Timestamp.Add(c.cfg.videoTimeout)
	}

	if out.PTS > c.lastVideoPTS {
		c.lastVideoPTS = out.PTS
	}

	return out
}

func (c *compatInputStream) prepareAudioFrame(frame *Frame, synthetic bool) *Frame {
	out := cloneCompatFrame(frame)
	if out == nil {
		return nil
	}

	c.sequenceMu.Lock()
	defer c.sequenceMu.Unlock()

	c.audioSequence++
	out.SequenceID = c.audioSequence
	if out.InputID == "" {
		out.InputID = c.id
	}
	out.IsKeyFrame = true
	out.GOPID = out.SequenceID

	c.timingMu.Lock()
	defer c.timingMu.Unlock()

	if synthetic {
		pts := c.nextPTSLocked(false, c.cfg.audioInterval)
		out.PTS = pts
		out.DTS = pts
		out.Duration = c.cfg.audioInterval
		if out.SampleRate == 0 {
			out.SampleRate = c.syntheticAudioSampleRate()
		}
		c.nextSyntheticAudio = out.Timestamp.Add(c.cfg.audioInterval)
	} else {
		if out.Timestamp.IsZero() {
			out.Timestamp = time.Now()
		}
		c.nextSyntheticAudio = out.Timestamp.Add(c.cfg.audioTimeout)
	}

	if out.PTS > c.lastAudioPTS {
		c.lastAudioPTS = out.PTS
	}

	return out
}

func (c *compatInputStream) nextPTSLocked(video bool, step time.Duration) time.Duration {
	var trackPTS time.Duration
	var otherPTS time.Duration

	if video {
		trackPTS = c.lastVideoPTS
		otherPTS = c.lastAudioPTS
	} else {
		trackPTS = c.lastAudioPTS
		otherPTS = c.lastVideoPTS
	}

	next := time.Duration(0)
	if trackPTS > 0 {
		next = trackPTS + step
	} else if otherPTS > 0 {
		next = otherPTS
	}

	// Keep the synthetic track close to the real track timeline so HLS/GOP
	// consumers do not end up with an audio-only or video-only tail segment.
	if otherPTS > step {
		minNext := otherPTS - step
		if next < minNext {
			next = minNext
		}
	}

	return next
}

func (c *compatInputStream) emitVideo(frame *Frame) bool {
	select {
	case c.videoChan <- frame:
		return true
	case <-c.done:
		return false
	}
}

func (c *compatInputStream) emitAudio(frame *Frame) bool {
	select {
	case c.audioChan <- frame:
		return true
	case <-c.done:
		return false
	}
}

func (c *compatInputStream) tryEmitVideo(frame *Frame) bool {
	select {
	case c.videoChan <- frame:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

func (c *compatInputStream) tryEmitAudio(frame *Frame) bool {
	select {
	case c.audioChan <- frame:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

func (c *compatInputStream) applyTrackInfo(info InputTrackInfo) {
	if len(info.AudioConfig) > 0 {
		info.AudioConfig = append([]byte(nil), info.AudioConfig...)
	}

	now := time.Now()

	c.trackInfoMu.Lock()
	c.trackInfo = info
	if info.Initialized {
		c.trackInfoAt = now
	}
	c.trackInfoMu.Unlock()

	c.timingMu.Lock()
	if !info.HasVideo {
		c.nextSyntheticVideo = now
	} else if c.nextSyntheticVideo.IsZero() {
		c.nextSyntheticVideo = now.Add(c.cfg.videoTimeout)
	}
	if !info.HasAudio {
		c.nextSyntheticAudio = now
	} else if c.nextSyntheticAudio.IsZero() {
		c.nextSyntheticAudio = now.Add(c.cfg.audioTimeout)
	}
	c.timingMu.Unlock()
}

func (c *compatInputStream) trackInfoSnapshot() (InputTrackInfo, time.Time, bool) {
	c.trackInfoMu.RLock()
	defer c.trackInfoMu.RUnlock()

	info := c.trackInfo
	if len(info.AudioConfig) > 0 {
		info.AudioConfig = append([]byte(nil), info.AudioConfig...)
	}
	return info, c.trackInfoAt, info.Initialized
}

func cloneCompatFrame(frame *Frame) *Frame {
	if frame == nil {
		return nil
	}

	out := *frame
	if len(frame.Payload) > 0 {
		out.Payload = make([][]byte, len(frame.Payload))
		for i, payload := range frame.Payload {
			out.Payload[i] = append([]byte(nil), payload...)
		}
	}
	return &out
}

func maxCompatTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (c *compatInputStream) syntheticAudioSampleRate() int {
	if template := c.audioTemplate(); template != nil && template.SampleRate > 0 {
		return template.SampleRate
	}
	info, _, ok := c.trackInfoSnapshot()
	if ok && info.AudioSampleRate > 0 {
		return info.AudioSampleRate
	}
	return DefaultAudioRate
}

func (c *compatInputStream) cacheRealVideoTemplate(frame *Frame) {
	if frame == nil || !frame.IsKeyFrame {
		return
	}
	c.storeVideoTemplate(frame)
}

func (c *compatInputStream) cacheRealAudioTemplate(frame *Frame) {
	if frame == nil || frame.Codec != "aac" {
		return
	}
	c.storeAudioTemplate(frame)
}

func (c *compatInputStream) isActive() bool {
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	return c.active
}

func (c *compatInputStream) setActive(active bool) {
	c.activeMu.Lock()
	c.active = active
	c.activeMu.Unlock()

	now := time.Now()
	info, _, ok := c.trackInfoSnapshot()

	c.timingMu.Lock()
	defer c.timingMu.Unlock()

	if !active {
		c.nextSyntheticVideo = time.Time{}
		c.nextSyntheticAudio = time.Time{}
		return
	}

	if !ok || !info.Initialized {
		return
	}

	if !info.HasVideo || c.lastRealVideoAt.IsZero() || now.Sub(c.lastRealVideoAt) >= c.cfg.videoTimeout {
		c.nextSyntheticVideo = now
	} else {
		c.nextSyntheticVideo = c.lastRealVideoAt.Add(c.cfg.videoTimeout)
	}

	if !info.HasAudio || c.lastRealAudioAt.IsZero() || now.Sub(c.lastRealAudioAt) >= c.cfg.audioTimeout {
		c.nextSyntheticAudio = now
	} else {
		c.nextSyntheticAudio = c.lastRealAudioAt.Add(c.cfg.audioTimeout)
	}
}

func (c *compatInputStream) flushSyntheticBeforeClose() {
	info, infoAt, ok := c.trackInfoSnapshot()
	if !ok || !info.Initialized {
		return
	}

	now := time.Now()
	if c.shouldFlushSyntheticVideoOnClose(info) {
		for i := 0; i < 3; i++ {
			frameTime := c.syntheticFrameTime(true, info, infoAt, now.Add(time.Duration(i)*c.cfg.videoInterval))
			frame := c.syntheticVideoFrame(frameTime)
			if frame == nil || !c.tryEmitVideo(frame) {
				break
			}
		}
	}

	if c.shouldFlushSyntheticAudioOnClose(info) {
		for i := 0; i < 6; i++ {
			frameTime := c.syntheticFrameTime(false, info, infoAt, now.Add(time.Duration(i)*c.cfg.audioInterval))
			frame := c.syntheticAudioFrame(frameTime)
			if frame == nil || !c.tryEmitAudio(frame) {
				break
			}
		}
	}
}

func (c *compatInputStream) shouldFlushSyntheticVideoOnClose(info InputTrackInfo) bool {
	if !info.Initialized {
		return false
	}
	if !info.HasVideo {
		return true
	}
	if !c.cfg.runtimeDetection {
		return false
	}

	c.timingMu.RLock()
	lastReal := c.lastRealVideoAt
	c.timingMu.RUnlock()
	return lastReal.IsZero() || time.Since(lastReal) >= c.cfg.videoTimeout
}

func (c *compatInputStream) shouldFlushSyntheticAudioOnClose(info InputTrackInfo) bool {
	if !info.Initialized {
		return false
	}
	if !info.HasAudio {
		return true
	}
	if !c.cfg.runtimeDetection {
		return false
	}

	c.timingMu.RLock()
	lastReal := c.lastRealAudioAt
	c.timingMu.RUnlock()
	return lastReal.IsZero() || time.Since(lastReal) >= c.cfg.audioTimeout
}

func (c *compatInputStream) storeVideoTemplate(frame *Frame) {
	if c == nil || frame == nil {
		return
	}
	c.templateMu.Lock()
	defer c.templateMu.Unlock()
	c.lastVideoKeyAU = cloneCompatFrame(frame)
}

func (c *compatInputStream) storeAudioTemplate(frame *Frame) {
	if c == nil || frame == nil {
		return
	}
	c.templateMu.Lock()
	defer c.templateMu.Unlock()
	c.lastAudioAU = cloneCompatFrame(frame)
}

func (c *compatInputStream) videoTemplate() *Frame {
	if c == nil {
		return nil
	}
	c.templateMu.RLock()
	defer c.templateMu.RUnlock()
	return cloneCompatFrame(c.lastVideoKeyAU)
}

func (c *compatInputStream) audioTemplate() *Frame {
	if c == nil {
		return nil
	}
	c.templateMu.RLock()
	defer c.templateMu.RUnlock()
	return cloneCompatFrame(c.lastAudioAU)
}
