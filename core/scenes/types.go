package scenes

import (
	"context"
	"sync"
	"time"

	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

type Input struct {
	Stream shared.Stream
	Layout raw.VideoLayout
}

type config struct {
	outputFPS            int
	videoBuffer          int
	audioBuffer          int
	audioPassthroughFrom int
	audioMixRatios       []int
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

type Scene struct {
	id     string
	canvas raw.CanvasSpec
	inputs []Input
	cfg    config

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

	decodedAudio chan decodedAudioFrame
}

type inputRuntime struct {
	spec Input

	videoDecoderIn chan *shared.Frame
	videoDecoder   videoDecoder
	videoDecoderMu sync.Mutex

	audioDecoderIn chan *shared.Frame
	audioDecoder   audioDecoder
	audioDecoderMu sync.Mutex
	audioTransport string

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

func (s *Scene) GetVideoChan() chan *shared.Frame { return s.videoChan }

func (s *Scene) GetAudioChan() chan *shared.Frame { return s.audioChan }

func (s *Scene) GetID() string { return s.id }

func (s *Scene) Type() string { return "scene" }

func (s *Scene) IsRestartable() bool { return false }

func (s *Scene) RestartInterval() time.Duration { return 0 }

func (s *Scene) WaitForStart(ctx context.Context) error {
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
