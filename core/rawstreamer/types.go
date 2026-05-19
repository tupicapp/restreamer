package rawstreamer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"restreamer/core/avsync"
	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

type Input struct {
	Stream shared.Stream
	Layout raw.VideoLayout
}

type ProcessorFactory func() raw.Processor

type config struct {
	outputFPS            int
	videoBuffer          int
	audioBuffer          int
	audioPassthroughFrom int
	audioMixRatios       []int
	streamType           string
	outputVideoCodec     string
}

type Option func(*config)

func WithOutputFPS(fps int) Option {
	return func(cfg *config) {
		cfg.outputFPS = fps
	}
}

func WithVideoBuffer(size int) Option {
	return func(cfg *config) {
		cfg.videoBuffer = size
	}
}

func WithAudioBuffer(size int) Option {
	return func(cfg *config) {
		cfg.audioBuffer = size
	}
}

func WithAudioPassthroughFrom(index int) Option {
	return func(cfg *config) {
		cfg.audioPassthroughFrom = index
	}
}

func WithAudioMixRatios(ratios []int) Option {
	return func(cfg *config) {
		if len(ratios) == 0 {
			cfg.audioMixRatios = nil
			return
		}
		cfg.audioMixRatios = append([]int(nil), ratios...)
	}
}

func WithStreamType(streamType string) Option {
	return func(cfg *config) {
		cfg.streamType = streamType
	}
}

func WithOutputVideoCodec(codec string) Option {
	return func(cfg *config) {
		cfg.outputVideoCodec = codec
	}
}

type RawStreamer struct {
	id               string
	canvas           raw.CanvasSpec
	inputs           []Input
	cfg              config
	processorFactory ProcessorFactory
	processor        raw.Processor

	videoChan chan *shared.Frame
	audioChan chan *shared.Frame

	done      chan struct{}
	started   chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	startErr  error
	events    *shared.EventEmitter

	lastIOMu sync.RWMutex
	lastIO   time.Time

	totalVideoFrames   int64
	totalAudioFrames   int64
	droppedVideoFrames float64
	droppedAudioFrames float64

	runtimes []*inputRuntime
	encoder  encoderRuntime
	audio    audioEncoderRuntime
	timeline *avsync.Timeline

	decodedAudio chan decodedAudioFrame
}

type inputRuntime struct {
	spec Input

	videoCodec     string
	videoDecoderIn chan *shared.Frame
	videoDecoder   videoDecoder
	videoDecoderMu sync.Mutex
	videoHeaders   [][]byte

	audioDecoderIn  chan *shared.Frame
	audioDecoder    audioDecoder
	audioDecoderMu  sync.Mutex
	audioTransport  string
	audioResampler  raw.PCM16Resampler
	audioResampleMu sync.Mutex

	latestMu    sync.RWMutex
	latestFrame *raw.VideoFrame
	ready       bool
}

type videoDecoder interface {
	Start() error
	Output() <-chan *raw.VideoFrame
	Errors() <-chan error
	Close() error
}

type videoEncoder interface {
	Start() error
	Output() <-chan *shared.Frame
	Errors() <-chan error
	Close() error
}

type audioDecoder interface {
	Start() error
	Output() <-chan *raw.AudioFrame
	Errors() <-chan error
	Close() error
}

type audioEncoder interface {
	Start() error
	Output() <-chan *shared.Frame
	Errors() <-chan error
	Close() error
	AudioSpecificConfig() []byte
}

type encoderRuntime struct {
	input   chan *raw.VideoFrame
	encoder videoEncoder
}

type audioEncoderRuntime struct {
	input   chan *raw.AudioFrame
	encoder audioEncoder
}

type decodedAudioFrame struct {
	index int
	frame *raw.AudioFrame
}

func NormalizeAudioMixRatios(inputCount int, ratios []int) ([]int, error) {
	if inputCount <= 0 {
		return nil, fmt.Errorf("raw streamer requires at least one input")
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

func (s *RawStreamer) GetVideoChan() chan *shared.Frame { return s.videoChan }

func (s *RawStreamer) GetAudioChan() chan *shared.Frame { return s.audioChan }

func (s *RawStreamer) GetID() string { return s.id }

func (s *RawStreamer) AudioSpecificConfig() []byte {
	if s == nil {
		return nil
	}
	if s.audio.encoder != nil {
		return s.audio.encoder.AudioSpecificConfig()
	}
	return s.audioPassthroughConfig()
}

func (s *RawStreamer) Type() string { return s.cfg.streamType }

func (s *RawStreamer) IsRestartable() bool { return false }

func (s *RawStreamer) RestartInterval() time.Duration { return 0 }

func (s *RawStreamer) WaitForStart(ctx context.Context) error {
	if s.startErr != nil {
		return s.startErr
	}
	select {
	case <-s.started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return context.Canceled
	}
}
