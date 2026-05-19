package scenes

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/tupicapp/restreamer/core/decoder"
	"github.com/tupicapp/restreamer/core/encoder"
	"github.com/tupicapp/restreamer/core/raw"
	shared "github.com/tupicapp/restreamer/core/shared"
)

const (
	sceneMixedAudioSampleRate   = 44100
	sceneMixedAudioChannels     = 2
	sceneMixedAudioSamplesPerAU = 1024
	sceneMixedAudioMaxBacklogAU = 8
)

func NormalizeAudioMixRatiosForCLI(inputCount int, ratios []int) ([]int, error) {
	return normalizeAudioMixRatios(inputCount, ratios)
}

func normalizeAudioMixRatios(inputCount int, ratios []int) ([]int, error) {
	if inputCount <= 0 {
		return nil, fmt.Errorf("scene requires at least one input")
	}
	if len(ratios) == 0 {
		return nil, nil
	}
	if len(ratios) != inputCount {
		return nil, fmt.Errorf("--audio-ratio count must match input count")
	}

	normalized := make([]int, len(ratios))
	total := 0
	for i, ratio := range ratios {
		if ratio < 0 || ratio > 100 {
			return nil, fmt.Errorf("--audio-ratio values must be between 0 and 100")
		}
		normalized[i] = ratio
		total += ratio
	}
	if total != 100 {
		return nil, fmt.Errorf("--audio-ratio values must sum to 100")
	}

	return normalized, nil
}

func (s *Scene) shouldMixAudio() bool {
	return len(s.cfg.audioMixRatios) > 0 && s.audioMixPassthroughIndex() < 0
}

func (s *Scene) audioMixPassthroughIndex() int {
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

func (s *Scene) initAudioMixer() error {
	activeInputs := 0
	for _, ratio := range s.cfg.audioMixRatios {
		if ratio > 0 {
			activeInputs++
		}
	}
	if activeInputs == 0 {
		return fmt.Errorf("scene audio mix requires at least one non-zero ratio")
	}

	audioInput := make(chan *raw.AudioFrame, s.cfg.audioBuffer)
	audioEncoder, err := encoder.NewAACEncoder(
		s.id+"-audio-encoder",
		audioInput,
		encoder.WithAACEncoderOutputBuffer(s.cfg.audioBuffer),
		encoder.WithAACEncoderTransport(encoder.AACTransportRaw),
		encoder.WithAACEncoderSampleRate(sceneMixedAudioSampleRate),
		encoder.WithAACEncoderChannels(sceneMixedAudioChannels),
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

func (s *Scene) consumeInputAudio(index int, rt *inputRuntime) {
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

			transport := sceneAACTransport(frame.PacketType)
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

func (s *Scene) ensureAudioDecoder(rt *inputRuntime, transport decoder.AACTransport) error {
	rt.audioDecoderMu.Lock()
	defer rt.audioDecoderMu.Unlock()

	if rt.audioDecoder != nil {
		if rt.audioTransport != string(transport) {
			return fmt.Errorf(
				"scene audio input %s transport changed from %s to %s",
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
		config := sceneInputAudioSpecificConfig(rt.spec.Stream)
		if len(config) > 0 {
			opts = append(opts, decoder.WithAACDecoderAudioSpecificConfig(config))
		} else {
			opts = append(opts, decoder.WithAACDecoderMPEG4AudioConfig(sceneMixedAudioSampleRate, sceneMixedAudioChannels))
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

func (s *Scene) consumeDecodedAudio(rt *inputRuntime) {
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

func (s *Scene) mixAudioLoop() {
	samplesPerTick := sceneMixedAudioSamplesPerAU * sceneMixedAudioChannels
	maxBufferedSamples := samplesPerTick * sceneMixedAudioMaxBacklogAU
	frameDuration := time.Duration(sceneMixedAudioSamplesPerAU) * time.Second / time.Duration(sceneMixedAudioSampleRate)
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
			mixFrame, err := prepareSceneMixAudioFrame(decoded.frame)
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
				sceneMixedAudioSamplesPerAU,
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

type sceneAudioConfigProvider interface {
	AudioSpecificConfig() []byte
}

func sceneInputAudioSpecificConfig(stream shared.Stream) []byte {
	if stream == nil {
		return nil
	}
	provider, ok := stream.(sceneAudioConfigProvider)
	if !ok {
		return nil
	}
	return provider.AudioSpecificConfig()
}

func prepareSceneMixAudioFrame(frame *raw.AudioFrame) (*raw.AudioFrame, error) {
	if frame == nil {
		return nil, fmt.Errorf("raw audio frame is nil")
	}
	if frame.SampleFormat != raw.AudioCodecPCMS16LE {
		return nil, fmt.Errorf("unsupported scene mix sample format %q", frame.SampleFormat)
	}
	if frame.SampleRate == sceneMixedAudioSampleRate && frame.Channels == sceneMixedAudioChannels {
		if err := frame.Validate(); err != nil {
			return nil, err
		}
		return frame, nil
	}
	return raw.ConvertPCM16AudioFrame(frame, sceneMixedAudioSampleRate, sceneMixedAudioChannels)
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
	sceneID string,
	ratios []int,
	buffers [][]int16,
	samplesPerChannel int,
	pts time.Duration,
	timestamp time.Time,
) *raw.AudioFrame {
	totalSamples := samplesPerChannel * sceneMixedAudioChannels
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

	duration := time.Duration(samplesPerChannel) * time.Second / time.Duration(sceneMixedAudioSampleRate)
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
			InputID:    sceneID,
			IsKeyFrame: true,
		},
		SampleRate:        sceneMixedAudioSampleRate,
		Channels:          sceneMixedAudioChannels,
		SampleFormat:      raw.AudioCodecPCMS16LE,
		SamplesPerChannel: samplesPerChannel,
	}
}

func (s *Scene) consumeAudioEncoderOutput() {
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

func (s *Scene) consumeAudioEncoderErrors() {
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

func nextMixedAudioTargetIndex(ratios []int, queues [][]*raw.AudioFrame) int {
	targetIndex := -1
	var targetPTS time.Duration

	for i, ratio := range ratios {
		if ratio == 0 || len(queues[i]) == 0 || queues[i][0] == nil || queues[i][0].Frame == nil {
			continue
		}
		pts := queues[i][0].Frame.PTS
		if targetIndex < 0 || pts < targetPTS {
			targetIndex = i
			targetPTS = pts
		}
	}

	return targetIndex
}

func mixPCM16AudioFrames(sceneID string, ratios []int, frames []*raw.AudioFrame) (*raw.AudioFrame, error) {
	if len(ratios) != len(frames) {
		return nil, fmt.Errorf("audio ratio count %d does not match frame count %d", len(ratios), len(frames))
	}

	baseIndex := -1
	for i, frame := range frames {
		if ratios[i] == 0 || frame == nil {
			continue
		}
		if !sceneAudioFrameMatchesMixFormat(frame) {
			return nil, fmt.Errorf("audio frame %d does not match the scene mix format", i)
		}
		baseIndex = i
		break
	}
	if baseIndex < 0 {
		return nil, fmt.Errorf("no audio frames available for mixing")
	}

	base := frames[baseIndex]
	basePayload := base.Frame.Payload[0]
	accum := make([]int32, len(basePayload)/2)

	for i, frame := range frames {
		if ratios[i] == 0 || frame == nil {
			continue
		}
		if !audioFramesCompatibleForMix(base, frame) {
			return nil, fmt.Errorf("audio frame %d is not compatible with the mix reference frame", i)
		}

		payload := frame.Frame.Payload[0]
		for offset := 0; offset < len(payload); offset += 2 {
			sample := int16(binary.LittleEndian.Uint16(payload[offset : offset+2]))
			accum[offset/2] += int32(sample) * int32(ratios[i])
		}
	}

	outPayload := make([]byte, len(basePayload))
	for i, sample := range accum {
		mixed := sample / 100
		if mixed > math.MaxInt16 {
			mixed = math.MaxInt16
		}
		if mixed < math.MinInt16 {
			mixed = math.MinInt16
		}
		binary.LittleEndian.PutUint16(outPayload[i*2:], uint16(int16(mixed)))
	}

	duration := base.Frame.Duration
	if duration <= 0 && base.SamplesPerChannel > 0 && base.SampleRate > 0 {
		duration = time.Duration(base.SamplesPerChannel) * time.Second / time.Duration(base.SampleRate)
	}

	timestamp := base.Frame.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	out := &raw.AudioFrame{
		Frame: &shared.Frame{
			PTS:        base.Frame.PTS,
			DTS:        base.Frame.DTS,
			Duration:   duration,
			Payload:    [][]byte{outPayload},
			Codec:      raw.AudioCodecPCMS16LE,
			PacketType: raw.AudioCodecPCMS16LE,
			Timestamp:  timestamp,
			InputID:    sceneID,
			IsKeyFrame: true,
			GOPID:      base.Frame.GOPID,
			SequenceID: base.Frame.SequenceID,
			IsFile:     base.Frame.IsFile,
		},
		SampleRate:        base.SampleRate,
		Channels:          base.Channels,
		SampleFormat:      base.SampleFormat,
		SamplesPerChannel: base.SamplesPerChannel,
	}
	if out.Frame.DTS == 0 {
		out.Frame.DTS = out.Frame.PTS
	}

	return out, nil
}

func sceneAudioFrameMatchesMixFormat(frame *raw.AudioFrame) bool {
	if frame == nil || frame.Frame == nil {
		return false
	}
	if frame.SampleRate != sceneMixedAudioSampleRate || frame.Channels != sceneMixedAudioChannels {
		return false
	}
	if frame.SampleFormat != raw.AudioCodecPCMS16LE {
		return false
	}
	return frame.Validate() == nil
}

func audioFramesCompatibleForMix(base *raw.AudioFrame, candidate *raw.AudioFrame) bool {
	if !sceneAudioFrameMatchesMixFormat(base) || !sceneAudioFrameMatchesMixFormat(candidate) {
		return false
	}
	return base.SampleRate == candidate.SampleRate &&
		base.Channels == candidate.Channels &&
		base.SampleFormat == candidate.SampleFormat &&
		base.SamplesPerChannel == candidate.SamplesPerChannel &&
		len(base.Frame.Payload) == len(candidate.Frame.Payload) &&
		len(base.Frame.Payload[0]) == len(candidate.Frame.Payload[0])
}

func audioMixTolerance(frame *raw.AudioFrame) time.Duration {
	if frame == nil || frame.Frame == nil {
		return 0
	}
	if frame.Frame.Duration > 0 {
		return frame.Frame.Duration / 2
	}
	if frame.SamplesPerChannel > 0 && frame.SampleRate > 0 {
		return time.Duration(frame.SamplesPerChannel) * time.Second / time.Duration(frame.SampleRate) / 2
	}
	return 15 * time.Millisecond
}

func absDuration(v time.Duration) time.Duration {
	if v < 0 {
		return -v
	}
	return v
}

func sceneAACTransport(packetType string) decoder.AACTransport {
	switch strings.ToLower(strings.TrimSpace(packetType)) {
	case string(decoder.AACTransportADTS):
		return decoder.AACTransportADTS
	default:
		return decoder.AACTransportRaw
	}
}
