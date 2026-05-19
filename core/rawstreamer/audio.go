package rawstreamer

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"restreamer/core/decoder"
	"restreamer/core/encoder"
	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

const (
	mixedAudioSampleRate   = 48000
	mixedAudioChannels     = 2
	mixedAudioSamplesPerAU = 1024
	mixedAudioMaxBacklogAU = 8
)

func (s *RawStreamer) consumeAudio() {
	if len(s.runtimes) == 0 {
		return
	}

	index := s.audioPassthroughIndex()
	if index < 0 || index >= len(s.runtimes) {
		index = 0
	}

	rt := s.runtimes[index]

	for {
		select {
		case <-s.done:
			return
		case frame, ok := <-rt.spec.Stream.GetAudioChan():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			if frame.Codec != "" && frame.Codec != "aac" {
				s.droppedAudioFrames++
				continue
			}

			out := s.prepareOutputAudioFrame(frame)
			if !s.enqueueLatestAudio(out) {
				return
			}
		}
	}
}

func (s *RawStreamer) audioPassthroughIndex() int {
	if index := s.audioMixPassthroughIndex(); index >= 0 {
		return index
	}
	return s.cfg.audioPassthroughFrom
}

func (s *RawStreamer) audioPassthroughConfig() []byte {
	index := s.audioPassthroughIndex()
	if index < 0 || index >= len(s.runtimes) {
		return nil
	}
	return inputAudioSpecificConfig(s.runtimes[index].spec.Stream)
}

func (s *RawStreamer) prepareOutputAudioFrame(frame *shared.Frame) *shared.Frame {
	if frame == nil {
		return nil
	}

	out := *frame
	out.InputID = s.id
	if len(frame.Payload) > 0 {
		out.Payload = make([][]byte, 0, len(frame.Payload))
		for _, payload := range frame.Payload {
			out.Payload = append(out.Payload, append([]byte(nil), payload...))
		}
	}
	return &out
}

func (s *RawStreamer) initAudioPassthroughTranscoder() error {
	if s.audio.encoder != nil {
		return nil
	}

	audioInput := make(chan *raw.AudioFrame, s.cfg.audioBuffer)
	audioEncoder, err := encoder.NewAACEncoder(
		s.id+"-audio-encoder",
		audioInput,
		encoder.WithAACEncoderOutputBuffer(s.cfg.audioBuffer),
		encoder.WithAACEncoderTransport(encoder.AACTransportRaw),
		encoder.WithAACEncoderSampleRate(mixedAudioSampleRate),
		encoder.WithAACEncoderChannels(mixedAudioChannels),
	)
	if err != nil {
		return err
	}
	if err := audioEncoder.Start(); err != nil {
		return err
	}

	s.audio = audioEncoderRuntime{
		input:   audioInput,
		encoder: audioEncoder,
	}

	go s.consumeAudioEncoderOutput()
	go s.consumeAudioEncoderErrors()
	return nil
}

func (s *RawStreamer) enqueueLatestAudio(frame *shared.Frame) bool {
	if frame == nil {
		return true
	}

	for {
		select {
		case <-s.done:
			return false
		case s.audioChan <- frame:
			s.totalAudioFrames++
			s.touchLastIO()
			select {
			case <-s.started:
			default:
				close(s.started)
			}
			return true
		default:
		}

		select {
		case <-s.done:
			return false
		case <-s.audioChan:
			s.droppedAudioFrames++
		case <-time.After(250 * time.Millisecond):
			s.droppedAudioFrames++
			return false
		}
	}
}

func (s *RawStreamer) shouldMixAudio() bool {
	return len(s.cfg.audioMixRatios) > 0 && s.audioMixPassthroughIndex() < 0
}

func (s *RawStreamer) audioMixPassthroughIndex() int {
	if len(s.cfg.audioMixRatios) == 0 {
		return -1
	}

	index := -1
	for i, ratio := range s.cfg.audioMixRatios {
		if ratio == 0 {
			continue
		}
		if ratio == 100 && index < 0 {
			index = i
			continue
		}
		return -1
	}

	return index
}

func (s *RawStreamer) initAudioMixer() error {
	activeInputs := 0
	for _, ratio := range s.cfg.audioMixRatios {
		if ratio > 0 {
			activeInputs++
		}
	}
	if activeInputs == 0 {
		return fmt.Errorf("raw streamer audio mix requires at least one non-zero ratio")
	}

	audioInput := make(chan *raw.AudioFrame, s.cfg.audioBuffer)
	audioEncoder, err := encoder.NewAACEncoder(
		s.id+"-audio-encoder",
		audioInput,
		encoder.WithAACEncoderOutputBuffer(s.cfg.audioBuffer),
		encoder.WithAACEncoderTransport(encoder.AACTransportRaw),
		encoder.WithAACEncoderSampleRate(mixedAudioSampleRate),
		encoder.WithAACEncoderChannels(mixedAudioChannels),
	)
	if err != nil {
		return err
	}
	if err := audioEncoder.Start(); err != nil {
		return err
	}

	s.audio = audioEncoderRuntime{
		input:   audioInput,
		encoder: audioEncoder,
	}
	s.decodedAudio = make(chan decodedAudioFrame, max(1, activeInputs*s.cfg.audioBuffer))
	return nil
}

func (s *RawStreamer) consumeInputAudio(index int, rt *inputRuntime) {
	if s.cfg.audioMixRatios[index] == 0 {
		return
	}

	for {
		select {
		case <-s.done:
			return
		case frame, ok := <-rt.spec.Stream.GetAudioChan():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			if frame.Codec != "" && frame.Codec != "aac" {
				s.droppedAudioFrames++
				continue
			}

			transport := rawStreamerAACTransport(frame.PacketType)
			if err := s.ensureAudioDecoder(rt, transport); err != nil {
				s.droppedAudioFrames++
				continue
			}

			select {
			case rt.audioDecoderIn <- frame:
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
				s.droppedAudioFrames++
			}
		}
	}
}

func (s *RawStreamer) ensureAudioDecoder(rt *inputRuntime, transport decoder.AACTransport) error {
	rt.audioDecoderMu.Lock()
	defer rt.audioDecoderMu.Unlock()

	if rt.audioDecoder != nil {
		if rt.audioTransport != string(transport) {
			return fmt.Errorf(
				"raw streamer audio input %s transport changed from %s to %s",
				rt.spec.Stream.GetID(),
				rt.audioTransport,
				transport,
			)
		}
		return nil
	}

	rt.audioDecoderIn = make(chan *shared.Frame, s.cfg.audioBuffer)
	opts := []decoder.AACDecoderOption{
		decoder.WithAACDecoderOutputBuffer(s.cfg.audioBuffer),
		decoder.WithAACDecoderTransport(transport),
	}
	if transport == decoder.AACTransportRaw {
		config := inputAudioSpecificConfig(rt.spec.Stream)
		if len(config) > 0 {
			opts = append(opts, decoder.WithAACDecoderAudioSpecificConfig(config))
		} else {
			opts = append(opts, decoder.WithAACDecoderMPEG4AudioConfig(mixedAudioSampleRate, mixedAudioChannels))
		}
	}

	audioDecoder, err := decoder.NewAACDecoder(
		s.id+"-audio-decoder-"+rt.spec.Stream.GetID(),
		rt.audioDecoderIn,
		opts...,
	)
	if err != nil {
		return err
	}
	if err := audioDecoder.Start(); err != nil {
		return err
	}

	rt.audioDecoder = audioDecoder
	rt.audioTransport = string(transport)
	go s.consumeDecodedAudio(rt)
	return nil
}

func (s *RawStreamer) consumeDecodedAudio(rt *inputRuntime) {
	index := -1
	for i := range s.runtimes {
		if s.runtimes[i] == rt {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}

	for {
		select {
		case <-s.done:
			return
		case err, ok := <-rt.audioDecoder.Errors():
			if !ok {
				return
			}
			if err != nil {
				s.droppedAudioFrames++
			}
		case frame, ok := <-rt.audioDecoder.Output():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}

			if !s.shouldMixAudio() && index == s.audioPassthroughIndex() {
				converted, err := prepareMixAudioFrame(frame)
				if err != nil {
					s.droppedAudioFrames++
					continue
				}

				select {
				case s.audio.input <- converted:
				case <-s.done:
					return
				case <-time.After(250 * time.Millisecond):
					s.droppedAudioFrames++
				}
				continue
			}

			select {
			case s.decodedAudio <- decodedAudioFrame{index: index, frame: frame}:
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
				s.droppedAudioFrames++
			}
		}
	}
}

func (s *RawStreamer) mixAudioLoop() {
	samplesPerTick := mixedAudioSamplesPerAU * mixedAudioChannels
	maxBufferedSamples := samplesPerTick * mixedAudioMaxBacklogAU
	frameDuration := time.Duration(mixedAudioSamplesPerAU) * time.Second / time.Duration(mixedAudioSampleRate)
	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()

	buffers := make([][]int16, len(s.runtimes))
	started := false
	nextPTS := time.Duration(0)

	for {
		select {
		case <-s.done:
			return
		case decoded, ok := <-s.decodedAudio:
			if !ok {
				return
			}
			if decoded.frame == nil {
				continue
			}

			mixFrame, err := prepareMixAudioFrame(decoded.frame)
			if err != nil {
				s.droppedAudioFrames++
				continue
			}

			samples, err := decodePCM16Payload(mixFrame.Frame.Payload[0])
			if err != nil {
				s.droppedAudioFrames++
				continue
			}
			if len(samples) == 0 {
				continue
			}

			buffers[decoded.index] = append(buffers[decoded.index], samples...)
			if len(buffers[decoded.index]) > maxBufferedSamples {
				drop := len(buffers[decoded.index]) - maxBufferedSamples
				buffers[decoded.index] = append(buffers[decoded.index][:0], buffers[decoded.index][drop:]...)
				s.droppedAudioFrames += float64(drop) / float64(samplesPerTick)
			}

			if !started && hasBufferedMixedAudioSamples(s.cfg.audioMixRatios, buffers, samplesPerTick) {
				started = true
				nextPTS = mixFrame.Frame.PTS
				if nextPTS < 0 {
					nextPTS = 0
				}
			}
		case now := <-ticker.C:
			if !started {
				continue
			}

			mixed := buildBufferedMixedPCM16AudioFrame(
				s.id,
				s.cfg.audioMixRatios,
				buffers,
				mixedAudioSamplesPerAU,
				nextPTS,
				now,
			)
			nextPTS += frameDuration

			select {
			case s.audio.input <- mixed:
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
				s.droppedAudioFrames++
			}
		}
	}
}

type audioConfigProvider interface {
	AudioSpecificConfig() []byte
}

func inputAudioSpecificConfig(stream shared.Stream) []byte {
	if stream == nil {
		return nil
	}
	provider, ok := stream.(audioConfigProvider)
	if !ok {
		return nil
	}
	return provider.AudioSpecificConfig()
}

func prepareMixAudioFrame(frame *raw.AudioFrame) (*raw.AudioFrame, error) {
	if frame == nil {
		return nil, fmt.Errorf("raw audio frame is nil")
	}
	if frame.SampleFormat != raw.AudioCodecPCMS16LE {
		return nil, fmt.Errorf("unsupported raw streamer mix sample format %q", frame.SampleFormat)
	}
	if frame.SampleRate == mixedAudioSampleRate && frame.Channels == mixedAudioChannels {
		if err := frame.Validate(); err != nil {
			return nil, err
		}
		return frame, nil
	}
	return raw.ConvertPCM16AudioFrame(frame, mixedAudioSampleRate, mixedAudioChannels)
}

func hasBufferedMixedAudioSamples(ratios []int, buffers [][]int16, minSamples int) bool {
	for i, ratio := range ratios {
		if ratio == 0 {
			continue
		}
		if len(buffers[i]) >= minSamples {
			return true
		}
	}
	return false
}

func decodePCM16Payload(payload []byte) ([]int16, error) {
	if len(payload)%2 != 0 {
		return nil, fmt.Errorf("pcm16 payload length %d is not sample aligned", len(payload))
	}

	samples := make([]int16, len(payload)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(payload[i*2:]))
	}
	return samples, nil
}

func buildBufferedMixedPCM16AudioFrame(
	streamID string,
	ratios []int,
	buffers [][]int16,
	samplesPerChannel int,
	pts time.Duration,
	timestamp time.Time,
) *raw.AudioFrame {
	totalSamples := samplesPerChannel * mixedAudioChannels
	accum := make([]int32, totalSamples)

	for i, ratio := range ratios {
		if ratio == 0 {
			continue
		}

		available := min(totalSamples, len(buffers[i]))
		for sampleIndex := 0; sampleIndex < available; sampleIndex++ {
			accum[sampleIndex] += int32(buffers[i][sampleIndex]) * int32(ratio)
		}

		if available == 0 {
			continue
		}

		remaining := buffers[i][available:]
		if len(remaining) == 0 {
			buffers[i] = buffers[i][:0]
			continue
		}
		buffers[i] = append(buffers[i][:0], remaining...)
	}

	payload := make([]byte, totalSamples*2)
	for i, sample := range accum {
		mixed := sample / 100
		if mixed > math.MaxInt16 {
			mixed = math.MaxInt16
		}
		if mixed < math.MinInt16 {
			mixed = math.MinInt16
		}
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(int16(mixed)))
	}

	duration := time.Duration(samplesPerChannel) * time.Second / time.Duration(mixedAudioSampleRate)
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	return &raw.AudioFrame{
		Frame: &shared.Frame{
			PTS:        pts,
			DTS:        pts,
			Duration:   duration,
			Payload:    [][]byte{payload},
			Codec:      raw.AudioCodecPCMS16LE,
			PacketType: raw.AudioCodecPCMS16LE,
			Timestamp:  timestamp,
			InputID:    streamID,
			IsKeyFrame: true,
		},
		SampleRate:        mixedAudioSampleRate,
		Channels:          mixedAudioChannels,
		SampleFormat:      raw.AudioCodecPCMS16LE,
		SamplesPerChannel: samplesPerChannel,
	}
}

func (s *RawStreamer) consumeAudioEncoderOutput() {
	for {
		select {
		case <-s.done:
			return
		case frame, ok := <-s.audio.encoder.Output():
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			if !s.enqueueLatestAudio(frame) {
				return
			}
		}
	}
}

func (s *RawStreamer) consumeAudioEncoderErrors() {
	for {
		select {
		case <-s.done:
			return
		case _, ok := <-s.audio.encoder.Errors():
			if !ok {
				return
			}
			s.droppedAudioFrames++
		}
	}
}

func rawStreamerAACTransport(packetType string) decoder.AACTransport {
	switch strings.ToLower(strings.TrimSpace(packetType)) {
	case string(decoder.AACTransportADTS):
		return decoder.AACTransportADTS
	default:
		return decoder.AACTransportRaw
	}
}
