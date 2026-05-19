package rawstreamer

import (
	"fmt"
	"strings"
	"time"

	"restreamer/core/avsync"
	"restreamer/core/decoder"
	"restreamer/core/encoder"
	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

func New(
	id string,
	canvas raw.CanvasSpec,
	inputs []Input,
	processorFactory ProcessorFactory,
	opts ...Option,
) (*RawStreamer, error) {
	if id == "" {
		return nil, fmt.Errorf("raw streamer id is required")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("raw streamer requires at least one input")
	}
	if processorFactory == nil {
		return nil, fmt.Errorf("raw streamer processor factory is required")
	}
	if _, err := raw.ExpectedYUV420PSize(canvas.Width, canvas.Height); err != nil {
		return nil, fmt.Errorf("invalid raw streamer canvas: %w", err)
	}

	cfg := config{
		outputFPS:            25,
		videoBuffer:          20,
		audioBuffer:          20,
		audioPassthroughFrom: 0,
		streamType:           "raw_streamer",
		outputVideoCodec:     "h264",
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
	if strings.TrimSpace(cfg.streamType) == "" {
		cfg.streamType = "raw_streamer"
	}
	if strings.TrimSpace(cfg.outputVideoCodec) == "" {
		cfg.outputVideoCodec = "h264"
	}

	audioMixRatios, err := NormalizeAudioMixRatios(len(inputs), cfg.audioMixRatios)
	if err != nil {
		return nil, err
	}
	cfg.audioMixRatios = audioMixRatios

	timeline, err := avsync.NewTimeline(cfg.outputFPS, mixedAudioSampleRate, mixedAudioSamplesPerAU)
	if err != nil {
		return nil, err
	}

	streamer := &RawStreamer{
		id:               id,
		canvas:           canvas,
		inputs:           append([]Input(nil), inputs...),
		cfg:              cfg,
		processorFactory: processorFactory,
		videoChan:        make(chan *shared.Frame, cfg.videoBuffer),
		audioChan:        make(chan *shared.Frame, cfg.audioBuffer),
		done:             make(chan struct{}),
		started:          make(chan struct{}),
		events:           shared.NewEventEmitter(128),
		timeline:         timeline,
	}

	return streamer, nil
}

func (s *RawStreamer) Start() {
	s.startOnce.Do(func() {
		s.startErr = s.init()
	})
	if s.startErr == nil {
		s.events.Emit(shared.Event{
			Type:       shared.EventTypeStreamStarted,
			StreamID:   s.id,
			StreamType: s.Type(),
			Message:    "raw streamer started",
		})
	} else {
		s.events.Emit(shared.Event{
			Type:       shared.EventTypeStreamError,
			StreamID:   s.id,
			StreamType: s.Type(),
			Message:    "raw streamer failed to start",
			Error:      s.startErr,
		})
	}
}

func (s *RawStreamer) init() error {
	s.processor = s.processorFactory()
	if s.processor == nil {
		return fmt.Errorf("raw streamer processor factory returned nil")
	}

	s.runtimes = make([]*inputRuntime, 0, len(s.inputs))
	for idx, input := range s.inputs {
		if input.Stream == nil {
			return fmt.Errorf("raw streamer input %d stream is nil", idx)
		}
		if err := input.Layout.Validate(); err != nil {
			return fmt.Errorf("raw streamer input %d layout invalid: %w", idx, err)
		}
		s.runtimes = append(s.runtimes, &inputRuntime{spec: input})
	}

	videoEncoder, encoderInput, err := s.newVideoEncoder()
	if err != nil {
		return err
	}
	if err := videoEncoder.Start(); err != nil {
		return fmt.Errorf("raw streamer encoder start failed: %w", err)
	}
	s.encoder = encoderRuntime{
		input:   encoderInput,
		encoder: videoEncoder,
	}

	if s.shouldMixAudio() {
		if err := s.initAudioMixer(); err != nil {
			return fmt.Errorf("raw streamer audio mixer setup failed: %w", err)
		}
	} else {
		if err := s.initAudioPassthroughTranscoder(); err != nil {
			return fmt.Errorf("raw streamer audio passthrough setup failed: %w", err)
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
	} else {
		go s.consumeAudio()
		go s.passthroughAudioLoop()
	}
	if s.audio.encoder != nil {
		go s.consumeAudioEncoderOutput()
		go s.consumeAudioEncoderErrors()
	}
	go s.consumeEncoderOutput()
	go s.consumeEncoderErrors()
	go s.processLoop()

	return nil
}

func (s *RawStreamer) newVideoEncoder() (videoEncoder, chan *raw.VideoFrame, error) {
	input := make(chan *raw.VideoFrame, s.cfg.videoBuffer)

	switch strings.ToLower(strings.TrimSpace(s.cfg.outputVideoCodec)) {
	case "h264":
		videoEncoder, err := encoder.NewH264Encoder(
			s.id+"-encoder",
			input,
			encoder.WithH264EncoderFPS(s.cfg.outputFPS),
			encoder.WithH264EncoderGOPSize(s.cfg.outputFPS),
			encoder.WithH264EncoderOutputBuffer(s.cfg.videoBuffer),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("raw streamer encoder setup failed: %w", err)
		}
		return videoEncoder, input, nil
	default:
		return nil, nil, fmt.Errorf("unsupported raw streamer output video codec %q", s.cfg.outputVideoCodec)
	}
}

func (s *RawStreamer) Stop() {
	for _, input := range s.inputs {
		input.Stream.Stop()
	}
	s.events.Emit(shared.Event{
		Type:       shared.EventTypeStreamStopped,
		StreamID:   s.id,
		StreamType: s.Type(),
		Message:    "raw streamer stopped",
	})
}

func (s *RawStreamer) Close() {
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
			if rt.audioResampler != nil {
				_ = rt.audioResampler.Close()
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
			Message:    "raw streamer closed",
		})
		s.events.Close()
	})
}

func (s *RawStreamer) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *RawStreamer) State() *shared.State {
	s.lastIOMu.RLock()
	lastIO := s.lastIO
	s.lastIOMu.RUnlock()

	return &shared.State{
		IsStarted:          !lastIO.IsZero(),
		IsResumable:        false,
		RunnerDetails:      "raw streamer",
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

func (s *RawStreamer) Clone() (shared.Stream, error) {
	clonedInputs := make([]Input, 0, len(s.inputs))
	for _, input := range s.inputs {
		cloned, err := input.Stream.Clone()
		if err != nil {
			return nil, fmt.Errorf("clone raw streamer input %s: %w", input.Stream.GetID(), err)
		}
		clonedInputs = append(clonedInputs, Input{
			Stream: cloned,
			Layout: input.Layout,
		})
	}

	return New(
		s.id,
		s.canvas,
		clonedInputs,
		s.processorFactory,
		WithOutputFPS(s.cfg.outputFPS),
		WithVideoBuffer(s.cfg.videoBuffer),
		WithAudioBuffer(s.cfg.audioBuffer),
		WithAudioPassthroughFrom(s.cfg.audioPassthroughFrom),
		WithAudioMixRatios(s.cfg.audioMixRatios),
		WithStreamType(s.cfg.streamType),
		WithOutputVideoCodec(s.cfg.outputVideoCodec),
	)
}

func (s *RawStreamer) processLoop() {
	frameDuration := time.Second / time.Duration(s.cfg.outputFPS)
	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			placements, ok := s.snapshotPlacements()
			if !ok {
				continue
			}

			processed, err := s.processor.Process(raw.ProcessRequest{
				Canvas:     s.canvas,
				Placements: placements,
			})
			if err != nil {
				s.droppedVideoFrames++
				continue
			}
			timing := s.timeline.NextVideo(now)
			s.prepareOutputVideoFrame(processed, timing)

			select {
			case s.encoder.input <- processed:
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
				s.droppedVideoFrames++
			}
		}
	}
}

func (s *RawStreamer) prepareOutputVideoFrame(frame *raw.VideoFrame, timing avsync.FrameTiming) {
	if frame == nil || frame.Frame == nil {
		return
	}

	frame.Frame.PTS = timing.PTS
	frame.Frame.DTS = timing.DTS
	frame.Frame.Duration = timing.Duration
	frame.Frame.Timestamp = timing.Timestamp
	frame.Frame.InputID = s.id
}

func (s *RawStreamer) snapshotPlacements() ([]raw.VideoPlacement, bool) {
	placements := make([]raw.VideoPlacement, 0, len(s.runtimes))
	for _, rt := range s.runtimes {
		rt.latestMu.RLock()
		frame := rt.latestFrame
		ready := rt.ready
		rt.latestMu.RUnlock()
		if !ready || frame == nil {
			continue
		}

		frameCopy := *frame
		placements = append(placements, raw.VideoPlacement{
			Input:  frameCopy,
			Layout: rt.spec.Layout,
		})
	}
	return placements, len(placements) > 0
}

func (s *RawStreamer) consumeInputVideo(index int, rt *inputRuntime) {
	_ = index

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

			codec := strings.ToLower(strings.TrimSpace(frame.Codec))
			switch codec {
			case "", raw.VideoCodec:
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
			case "h264":
				rt.videoHeaders = updateH264Headers(rt.videoHeaders, frame.Payload)
				if rt.videoDecoder == nil && !frame.IsKeyFrame {
					s.droppedVideoFrames++
					continue
				}
				if frame.IsKeyFrame {
					frame = cloneFrameWithH264Headers(frame, rt.videoHeaders)
				}
				if err := s.ensureVideoDecoder(rt, codec); err != nil {
					s.droppedVideoFrames++
					continue
				}
				select {
				case rt.videoDecoderIn <- frame:
				case <-s.done:
					return
				case <-time.After(250 * time.Millisecond):
					s.droppedVideoFrames++
				}
			default:
				s.droppedVideoFrames++
			}
		}
	}
}

func (s *RawStreamer) ensureVideoDecoder(rt *inputRuntime, codec string) error {
	rt.videoDecoderMu.Lock()
	defer rt.videoDecoderMu.Unlock()

	if rt.videoDecoder != nil {
		if rt.videoCodec != codec {
			return fmt.Errorf(
				"raw streamer video input %s codec changed from %s to %s",
				rt.spec.Stream.GetID(),
				rt.videoCodec,
				codec,
			)
		}
		return nil
	}

	rt.videoDecoderIn = make(chan *shared.Frame, s.cfg.videoBuffer)

	var (
		videoDecoder videoDecoder
		err          error
	)
	switch codec {
	case "h264":
		videoDecoder, err = decoder.NewH264Decoder(
			s.id+"-decoder-"+rt.spec.Stream.GetID(),
			rt.videoDecoderIn,
			decoder.WithH264DecoderOutputResolution(rt.spec.Layout.Width, rt.spec.Layout.Height),
			decoder.WithH264DecoderOutputBuffer(s.cfg.videoBuffer),
		)
	default:
		err = fmt.Errorf("unsupported video codec %q", codec)
	}
	if err != nil {
		return err
	}
	if err := videoDecoder.Start(); err != nil {
		return err
	}

	rt.videoCodec = codec
	rt.videoDecoder = videoDecoder
	go s.consumeDecodedFrames(rt)
	return nil
}

func (s *RawStreamer) consumeDecodedFrames(rt *inputRuntime) {
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

func (s *RawStreamer) setLatestFrame(rt *inputRuntime, frame *raw.VideoFrame) {
	rt.latestMu.Lock()
	rt.latestFrame = frame
	rt.ready = true
	rt.latestMu.Unlock()
}

func (s *RawStreamer) consumeEncoderOutput() {
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
				s.droppedVideoFrames++
			}
		}
	}
}

func (s *RawStreamer) consumeEncoderErrors() {
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

func (s *RawStreamer) touchLastIO() {
	s.lastIOMu.Lock()
	s.lastIO = time.Now()
	s.lastIOMu.Unlock()
}

func updateH264Headers(existing [][]byte, nalus [][]byte) [][]byte {
	if len(nalus) == 0 {
		return existing
	}

	var (
		sps []byte
		pps []byte
	)
	for _, nalu := range existing {
		switch h264NALType(nalu) {
		case 7:
			sps = append([]byte(nil), trimAnnexBStartCode(nalu)...)
		case 8:
			pps = append([]byte(nil), trimAnnexBStartCode(nalu)...)
		}
	}

	for _, nalu := range nalus {
		trimmed := trimAnnexBStartCode(nalu)
		switch h264NALType(trimmed) {
		case 7:
			sps = append([]byte(nil), trimmed...)
		case 8:
			pps = append([]byte(nil), trimmed...)
		}
	}

	if len(sps) == 0 && len(pps) == 0 {
		return existing
	}

	out := make([][]byte, 0, 2)
	if len(sps) > 0 {
		out = append(out, sps)
	}
	if len(pps) > 0 {
		out = append(out, pps)
	}
	return out
}

func cloneFrameWithH264Headers(frame *shared.Frame, headers [][]byte) *shared.Frame {
	if frame == nil {
		return nil
	}

	out := *frame
	out.Payload = prependH264Headers(frame.Payload, headers)
	return &out
}

func prependH264Headers(nalus [][]byte, headers [][]byte) [][]byte {
	if len(nalus) == 0 {
		return nil
	}
	if len(headers) == 0 {
		return cloneNALUs(nalus)
	}

	hasSPS := false
	hasPPS := false
	for _, nalu := range nalus {
		switch h264NALType(nalu) {
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
	}
	if hasSPS && hasPPS {
		return cloneNALUs(nalus)
	}

	out := make([][]byte, 0, len(headers)+len(nalus))
	for _, header := range headers {
		trimmed := trimAnnexBStartCode(header)
		switch h264NALType(trimmed) {
		case 7:
			if hasSPS {
				continue
			}
		case 8:
			if hasPPS {
				continue
			}
		default:
			continue
		}
		out = append(out, append([]byte(nil), trimmed...))
	}
	out = append(out, cloneNALUs(nalus)...)
	return out
}

func cloneNALUs(nalus [][]byte) [][]byte {
	out := make([][]byte, 0, len(nalus))
	for _, nalu := range nalus {
		out = append(out, append([]byte(nil), nalu...))
	}
	return out
}

func h264NALType(nalu []byte) byte {
	trimmed := trimAnnexBStartCode(nalu)
	if len(trimmed) == 0 {
		return 0
	}
	return trimmed[0] & 0x1F
}

func trimAnnexBStartCode(nalu []byte) []byte {
	if len(nalu) >= 4 && nalu[0] == 0x00 && nalu[1] == 0x00 {
		if nalu[2] == 0x01 {
			return nalu[3:]
		}
		if nalu[2] == 0x00 && nalu[3] == 0x01 {
			return nalu[4:]
		}
	}
	return nalu
}

var _ shared.Stream = (*RawStreamer)(nil)
