package rawstreamer

import (
	"context"
	"fmt"
	"time"

	core "restreamer/core"
	"restreamer/core/raw"
	shared "restreamer/core/shared"
	"restreamer/core/streamfactory"
)

type Service struct{}

type audioConfigProviderSetter interface {
	SetAudioConfigProvider(interface{ AudioSpecificConfig() []byte })
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) RunComposer(ctx context.Context, spec Spec) error {
	if ctx == nil {
		ctx = context.Background()
	}

	activeStream, outputStream, cleanup, err := s.buildComposerPipeline(spec)
	if err != nil {
		return err
	}
	defer cleanup()

	streamer := core.NewStreamer()
	streamer.StartLife()
	defer streamer.Close()

	if err := streamer.UpdateStreams([]core.Stream{activeStream}, []core.Stream{outputStream}); err != nil {
		return err
	}
	if ok := streamer.Switch(activeStream.GetID()); !ok {
		return fmt.Errorf("failed to activate raw scene %q", activeStream.GetID())
	}

	streamer.Start()

	waitCtx, cancel := context.WithTimeout(ctx, spec.StartupTimeout)
	defer cancel()

	if err := activeStream.WaitForStart(waitCtx); err != nil {
		return fmt.Errorf("raw scene %q failed to start: %w", activeStream.GetID(), err)
	}
	if err := outputStream.WaitForStart(waitCtx); err != nil {
		return fmt.Errorf("output %q failed to start: %w", outputStream.GetID(), err)
	}

	<-ctx.Done()
	return nil
}

type Spec struct {
	StreamID       string
	InputURLs      []string
	Layouts        []shared.VideoLayout
	Canvas         shared.CanvasSpec
	OutputURL      string
	HLSOptions     *streamfactory.HLSOutputOptions
	AudioFrom      int
	AudioRatios    []int
	OutputFPS      int
	StartupTimeout time.Duration
}

func (s *Service) buildComposerPipeline(spec Spec) (core.Stream, core.Stream, func(), error) {
	created := make([]core.Stream, 0, len(spec.InputURLs)+2)
	cleanup := func() {
		for _, stream := range created {
			if stream != nil {
				stream.Close()
			}
		}
	}

	inputs := make([]Input, 0, len(spec.InputURLs))
	for idx, inputURL := range spec.InputURLs {
		streamID := fmt.Sprintf("%s-in-%d", spec.StreamID, idx+1)
		stream, err := streamfactory.NewInput(streamID, inputURL)
		if err != nil {
			cleanup()
			return nil, nil, func() {}, fmt.Errorf("create input %d: %w", idx+1, err)
		}
		created = append(created, stream)
		inputs = append(inputs, Input{
			Stream: stream,
			Layout: spec.Layouts[idx],
		})
	}

	rawStream, err := New(
		spec.StreamID,
		raw.CanvasSpec(spec.Canvas),
		inputs,
		raw.NewComposer,
		WithStreamType("raw_scene"),
		WithAudioPassthroughFrom(spec.AudioFrom),
		WithAudioMixRatios(spec.AudioRatios),
		WithOutputFPS(spec.OutputFPS),
	)
	if err != nil {
		cleanup()
		return nil, nil, func() {}, err
	}
	created = append(created, rawStream)

	outputStream, err := newOutput(spec.StreamID+"-out", spec.OutputURL, spec.HLSOptions)
	if err != nil {
		cleanup()
		return nil, nil, func() {}, err
	}
	if setter, ok := outputStream.(audioConfigProviderSetter); ok {
		setter.SetAudioConfigProvider(rawStream)
	}
	created = append(created, outputStream)

	return rawStream, outputStream, cleanup, nil
}

func newOutput(id, url string, hlsOpts *streamfactory.HLSOutputOptions) (core.Stream, error) {
	if streamfactory.IsHLSOutputPath(url) {
		opts := streamfactory.HLSOutputOptions{}
		if hlsOpts != nil {
			opts = *hlsOpts
		}
		return streamfactory.NewHLSOutput(id, url, opts)
	}
	return streamfactory.NewOutput(id, url)
}
