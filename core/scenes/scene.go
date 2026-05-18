package scenes

import (
	"fmt"
	"time"

	"restreamer/irajstreamer/core/decoder"
	"restreamer/irajstreamer/core/encoder"
	"restreamer/irajstreamer/core/raw"
	shared "restreamer/irajstreamer/core/shared"
)

func NewScene(id string, canvas raw.CanvasSpec, inputs []Input, opts ...Option) (*Scene, error) {
	if id == "" {
		return nil, fmt.Errorf("scene id is required")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("scene requires at least one input")
	}
	if _, err := raw.ExpectedYUV420PSize(canvas.Width, canvas.Height); err != nil {
		return nil, fmt.Errorf("invalid scene canvas: %w", err)
	}

	cfg := config{
		outputFPS:            25,
		videoBuffer:          100,
		audioBuffer:          100,
		audioPassthroughFrom: 0,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.outputFPS <= 0 {
		cfg.outputFPS = 25
	}
	if cfg.videoBuffer <= 0 {
		cfg.videoBuffer = 100
	}
	if cfg.audioBuffer <= 0 {
		cfg.audioBuffer = 100
	}
	if cfg.audioPassthroughFrom < 0 || cfg.audioPassthroughFrom >= len(inputs) {
		cfg.audioPassthroughFrom = 0
	}
	audioMixRatios, err := normalizeAudioMixRatios(len(inputs), cfg.audioMixRatios)
	if err != nil {
		return nil, err
	}
	cfg.audioMixRatios = audioMixRatios

	scene := &Scene{
		id:        id,
		canvas:    canvas,
		inputs:    append([]Input(nil), inputs...),
		cfg:       cfg,
		videoChan: make(chan *shared.Frame, cfg.videoBuffer),
		audioChan: make(chan *shared.Frame, cfg.audioBuffer),
		done:      make(chan struct{}),
		started:   make(chan struct{}),
		events:    shared.NewEventEmitter(128),
	}

	return scene, nil
}

func (s *Scene) Start() {
	s.startOnce.Do(func() {
		s.startErr = s.init()
	})
	if s.startErr == nil {
		s.events.Emit(shared.Event{
			Type:       shared.EventTypeStreamStarted,
			StreamID:   s.id,
			StreamType: s.Type(),
			Message:    "scene started",
		})
	} else {
		s.events.Emit(shared.Event{
			Type:       shared.EventTypeStreamError,
			StreamID:   s.id,
			StreamType: s.Type(),
			Message:    "scene failed to start",
			Error:      s.startErr,
		})
	}
}

func (s *Scene) init() error {
	s.runtimes = make([]*inputRuntime, 0, len(s.inputs))

	for idx, input := range s.inputs {
		if input.Stream == nil {
			return fmt.Errorf("scene input %d stream is nil", idx)
		}
		if err := input.Layout.Validate(); err != nil {
			return fmt.Errorf("scene input %d layout invalid: %w", idx, err)
		}

		rt := &inputRuntime{spec: input}
		s.runtimes = append(s.runtimes, rt)
	}

	encoderInput := make(chan *raw.VideoFrame, s.cfg.videoBuffer)
	videoEncoder, err := encoder.NewH264Encoder(
		s.id+"-encoder",
		encoderInput,
		encoder.WithH264EncoderFPS(s.cfg.outputFPS),
		encoder.WithH264EncoderGOPSize(s.cfg.outputFPS),
		encoder.WithH264EncoderOutputBuffer(s.cfg.videoBuffer),
	)
	if err != nil {
		return fmt.Errorf("scene encoder setup failed: %w", err)
	}
	if err := videoEncoder.Start(); err != nil {
		return fmt.Errorf("scene encoder start failed: %w", err)
	}

	s.encoder = encoderRuntime{
		input:   encoderInput,
		encoder: videoEncoder,
	}

	if s.shouldMixAudio() {
		if err := s.initAudioMixer(); err != nil {
			return fmt.Errorf("scene audio mixer setup failed: %w", err)
		}
	}

	for _, input := range s.inputs {
		input.Stream.Start()
	}

	for idx, rt := range s.runtimes {
		go s.consumeInputVideo(idx, rt)
	}
	if s.shouldMixAudio() {
		for idx, rt := range s.runtimes {
			go s.consumeInputAudio(idx, rt)
		}
		go s.mixAudioLoop()
		go s.consumeAudioEncoderOutput()
		go s.consumeAudioEncoderErrors()
	} else {
		go s.consumeAudio()
	}
	go s.consumeEncoderOutput()
	go s.consumeEncoderErrors()
	go s.composeLoop()

	return nil
}

func (s *Scene) Stop() {
	for _, input := range s.inputs {
		input.Stream.Stop()
	}
	s.events.Emit(shared.Event{
		Type:       shared.EventTypeStreamStopped,
		StreamID:   s.id,
		StreamType: s.Type(),
		Message:    "scene stopped",
	})
}

func (s *Scene) Close() {
	s.closeOnce.Do(func() {
		close(s.done)

		for _, input := range s.inputs {
			input.Stream.Close()
		}
		for _, rt := range s.runtimes {
			if rt.videoDecoder != nil {
				_ = rt.videoDecoder.Close()
			}
			if rt.audioDecoder != nil {
				_ = rt.audioDecoder.Close()
			}
		}
		if s.encoder.encoder != nil {
			_ = s.encoder.encoder.Close()
		}
		if s.audio.encoder != nil {
			_ = s.audio.encoder.Close()
		}
		s.events.Emit(shared.Event{
			Type:       shared.EventTypeStreamClosed,
			StreamID:   s.id,
			StreamType: s.Type(),
			Message:    "scene closed",
		})
		s.events.Close()
	})
}

func (s *Scene) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *Scene) State() *shared.State {
	s.lastIOMu.RLock()
	lastIO := s.lastIO
	s.lastIOMu.RUnlock()

	return &shared.State{
		IsStarted:          !lastIO.IsZero(),
		IsResumable:        false,
		RunnerDetails:      "scene compositor",
		LastIO:             lastIO,
		StreamID:           s.id,
		Type:               s.Type(),
		Url:                "",
		TotalVideoFrames:   s.totalVideoFrames,
		TotalAudioFrames:   s.totalAudioFrames,
		DroppedVideoFrames: s.droppedVideoFrames,
		DroppedAudioFrames: s.droppedAudioFrames,
	}
}

func (s *Scene) Clone() (shared.Stream, error) {
	clonedInputs := make([]Input, 0, len(s.inputs))
	for _, input := range s.inputs {
		cloned, err := input.Stream.Clone()
		if err != nil {
			return nil, fmt.Errorf("clone scene input %s: %w", input.Stream.GetID(), err)
		}
		clonedInputs = append(clonedInputs, Input{
			Stream: cloned,
			Layout: input.Layout,
		})
	}

	return NewScene(
		s.id,
		s.canvas,
		clonedInputs,
		WithOutputFPS(s.cfg.outputFPS),
		WithVideoBuffer(s.cfg.videoBuffer),
		WithAudioBuffer(s.cfg.audioBuffer),
		WithAudioPassthroughFrom(s.cfg.audioPassthroughFrom),
		WithAudioMixRatios(s.cfg.audioMixRatios),
	)
}

func (s *Scene) composeLoop() {
	frameDuration := time.Second / time.Duration(s.cfg.outputFPS)
	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()
	nextPTS := time.Duration(0)

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			placements, ok := s.snapshotPlacements()
			if !ok {
				// fmt.Println("scene: inputs not ready yet")
				continue
			}

			merged, err := raw.ComposeYUV420P(s.canvas, placements)
			if err != nil {
				fmt.Println("scene: compose error:", err)
				continue
			}
			s.prepareComposedVideoFrame(merged, nextPTS, frameDuration, time.Now())
			nextPTS += frameDuration

			// fmt.Println("scene: sending to encoder, input buffer usage:", len(s.encoder.input), "/", cap(s.encoder.input))
			select {
			case s.encoder.input <- merged:
				// fmt.Println("scene: frame sent to encoder successfully")
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
				// fmt.Println("scene: encoder input timeout, dropped frame")
				s.droppedVideoFrames++
			}
		}
	}
}

func (s *Scene) prepareComposedVideoFrame(frame *raw.VideoFrame, pts time.Duration, duration time.Duration, timestamp time.Time) {
	if frame == nil || frame.Frame == nil {
		return
	}

	// Scene output must use its own stable timing and stream identity instead of
	// inheriting metadata from whichever input happened to be sampled first.
	frame.Frame.PTS = pts
	frame.Frame.DTS = pts
	frame.Frame.Duration = duration
	frame.Frame.Timestamp = timestamp
	frame.Frame.InputID = s.id
}

func (s *Scene) snapshotPlacements() ([]raw.VideoPlacement, bool) {
	placements := make([]raw.VideoPlacement, 0, len(s.runtimes))
	for _, rt := range s.runtimes {
		rt.latestMu.RLock()
		frame := rt.latestFrame
		ready := rt.ready
		rt.latestMu.RUnlock()
		if !ready || frame == nil {
			return nil, false
		}

		frameCopy := *frame
		placements = append(placements, raw.VideoPlacement{
			Input:  frameCopy,
			Layout: rt.spec.Layout,
		})
	}

	return placements, true
}

func (s *Scene) consumeInputVideo(index int, rt *inputRuntime) {
	for {
		select {
		case <-s.done:
			return
		case frame, ok := <-rt.spec.Stream.GetVideoChan():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}

			if frame.Codec == raw.VideoCodec {
				rawFrame := &raw.VideoFrame{
					Frame:  frame,
					Width:  rt.spec.Layout.Width,
					Height: rt.spec.Layout.Height,
					PixFmt: raw.YUV420PPixFmt,
				}
				if frame.PacketType == raw.YUV420PPixFmt {
					rawFrame.PixFmt = frame.PacketType
				}
				if err := rawFrame.Validate(); err != nil {
					s.droppedVideoFrames++
					continue
				}
				s.setLatestFrame(rt, rawFrame)
				continue
			}

			// Skip P/B-frames before the decoder has seen its first IDR.
			// rtmpInputStream.prepareH264AccessUnit guarantees IDR frames include SPS+PPS,
			// so the decoder's first packet is always a valid access unit.
			if rt.videoDecoder == nil && !frame.IsKeyFrame {
				s.droppedVideoFrames++
				continue
			}

			if err := s.ensureDecoder(rt); err != nil {
				s.droppedVideoFrames++
				continue
			}

			if rt.videoDecoder != nil {
				select {
				case rt.videoDecoderIn <- frame:
				case <-s.done:
					return
				case <-time.After(250 * time.Millisecond):
					s.droppedVideoFrames++
				}
			}
		}
	}
}

func (s *Scene) ensureDecoder(rt *inputRuntime) error {
	rt.videoDecoderMu.Lock()
	defer rt.videoDecoderMu.Unlock()

	if rt.videoDecoder != nil {
		return nil
	}

	rt.videoDecoderIn = make(chan *shared.Frame, s.cfg.videoBuffer)
	videoDecoder, err := decoder.NewH264Decoder(
		s.id+"-decoder-"+rt.spec.Stream.GetID(),
		rt.videoDecoderIn,
		decoder.WithH264DecoderOutputResolution(rt.spec.Layout.Width, rt.spec.Layout.Height),
	)
	if err != nil {
		return err
	}
	if err := videoDecoder.Start(); err != nil {
		return err
	}
	rt.videoDecoder = videoDecoder
	go s.consumeDecodedFrames(rt)
	return nil
}

func (s *Scene) consumeDecodedFrames(rt *inputRuntime) {
	for {
		select {
		case <-s.done:
			return
		case err, ok := <-rt.videoDecoder.Errors():
			if !ok {
				return
			}
			if err != nil {
				s.droppedVideoFrames++
			}
		case frame, ok := <-rt.videoDecoder.Output():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			s.setLatestFrame(rt, frame)
		}
	}
}

func (s *Scene) setLatestFrame(rt *inputRuntime, frame *raw.VideoFrame) {
	rt.latestMu.Lock()
	rt.latestFrame = frame
	rt.ready = true
	rt.latestMu.Unlock()
}

func (s *Scene) consumeEncoderOutput() {
	for {
		select {
		case <-s.done:
			return
		case frame, ok := <-s.encoder.encoder.Output():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			// fmt.Println("scene: encoder output frame, pts:", frame.PTS, "is_keyframe:", frame.IsKeyFrame, "codec:", frame.Codec, "input_id:", frame.InputID, "total frames:", s.totalVideoFrames+1)
			select {
			case s.videoChan <- frame:
				s.totalVideoFrames++
				s.touchLastIO()
				select {
				case <-s.started:
				default:
					close(s.started)
				}
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
				// fmt.Println("scene: encoder output channel timeout, dropped frame")
				s.droppedVideoFrames++
			}
		}
	}
}

func (s *Scene) consumeEncoderErrors() {
	for {
		select {
		case <-s.done:
			return
		case _, ok := <-s.encoder.encoder.Errors():
			if !ok {
				return
			}
			s.droppedVideoFrames++
		}
	}
}

func (s *Scene) touchLastIO() {
	s.lastIOMu.Lock()
	s.lastIO = time.Now()
	s.lastIOMu.Unlock()
}

var _ shared.Stream = (*Scene)(nil)
