package scenes

import (
	"context"
	"fmt"

	core "restreamer/irajstreamer/core"
	"restreamer/irajstreamer/core/streamfactory"
)

type SwitcherRunner func(ctx context.Context, entries []SceneEntry, streamer *core.Streamer) error

// newOutput creates an output stream, using the HLS factory when hlsOpts is set.
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

type Service struct{}

func NewService() *Service {
	return &Service{}
}

func (s *Service) RunScene(ctx context.Context, spec SceneSpec, runSwitcher SwitcherRunner) error {
	if spec.Mode == SceneModePassthrough && len(spec.InputURLs) > 1 {
		return s.runPassthroughSwitcher(ctx, spec, runSwitcher)
	}

	activeStream, outputStream, cleanup, err := s.buildSingleScenePipeline(spec)
	if err != nil {
		return err
	}
	defer cleanup()

	streamer := core.NewStreamer(true, true, true)
	streamer.StartLife()
	defer streamer.Close()

	if err := streamer.UpdateStreams([]core.Stream{activeStream}, []core.Stream{outputStream}); err != nil {
		return err
	}
	if ok := streamer.Switch(activeStream.GetID()); !ok {
		return fmt.Errorf("failed to activate scene %q", activeStream.GetID())
	}

	streamer.Start()

	waitCtx, cancel := context.WithTimeout(ctx, spec.StartupTimeout)
	defer cancel()

	if err := activeStream.WaitForStart(waitCtx); err != nil {
		return fmt.Errorf("scene %q failed to start: %w", activeStream.GetID(), err)
	}
	if err := outputStream.WaitForStart(waitCtx); err != nil {
		return fmt.Errorf("output %q failed to start: %w", outputStream.GetID(), err)
	}

	<-ctx.Done()
	return nil
}

func (s *Service) RunMultiScene(ctx context.Context, spec MultiSceneSpec, runSwitcher SwitcherRunner) error {
	if len(spec.Definitions) == 0 {
		return fmt.Errorf("at least one scene definition is required")
	}
	if runSwitcher == nil {
		return fmt.Errorf("switcher runner is required")
	}

	var allStreams []core.Stream
	cleanup := func() {
		for _, stream := range allStreams {
			if stream != nil {
				stream.Close()
			}
		}
	}
	defer cleanup()

	sceneStreams := make([]core.Stream, 0, len(spec.Definitions))
	entries := make([]SceneEntry, 0, len(spec.Definitions))

	for idx, def := range spec.Definitions {
		sceneID := fmt.Sprintf("scene-%d", idx+1)
		sceneCanvas := spec.Canvas
		if !spec.HasCanvas {
			derivedCanvas, err := DeriveCanvas(def.Layouts)
			if err != nil {
				return fmt.Errorf("scene %d derive canvas: %w", idx+1, err)
			}
			sceneCanvas = derivedCanvas
		}

		sceneInputs := make([]Input, 0, len(def.InputURL))
		for i, inputURL := range def.InputURL {
			inputStream, err := streamfactory.NewInput(fmt.Sprintf("%s-in-%d", sceneID, i+1), inputURL)
			if err != nil {
				return fmt.Errorf("scene %d input %d (%s): %w", idx+1, i+1, inputURL, err)
			}
			allStreams = append(allStreams, inputStream)
			sceneInputs = append(sceneInputs, Input{
				Stream: inputStream,
				Layout: def.Layouts[i],
			})
		}

		audioFrom := spec.AudioFrom
		if audioFrom < 0 || audioFrom >= len(sceneInputs) {
			audioFrom = 0
		}
		audioRatios, err := NormalizeAudioMixRatiosForCLI(len(sceneInputs), spec.AudioRatios)
		if err != nil {
			return fmt.Errorf("scene %d: %w", idx+1, err)
		}

		sceneStream, err := NewScene(
			sceneID,
			sceneCanvas,
			sceneInputs,
			WithAudioPassthroughFrom(audioFrom),
			WithAudioMixRatios(audioRatios),
			WithOutputFPS(spec.OutputFPS),
		)
		if err != nil {
			return fmt.Errorf("scene %d: %w", idx+1, err)
		}
		allStreams = append(allStreams, sceneStream)
		sceneStreams = append(sceneStreams, sceneStream)
		entries = append(entries, SceneEntry{ID: sceneID, Name: def.Name})
	}

	outputStream, err := newOutput("multi-scene-out", spec.OutputURL, spec.HLSOptions)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	allStreams = append(allStreams, outputStream)

	streamer := core.NewStreamer(true, true, true)
	streamer.StartLife()
	defer streamer.Close()

	if err := streamer.UpdateStreams(sceneStreams, []core.Stream{outputStream}); err != nil {
		return fmt.Errorf("update streams: %w", err)
	}
	if ok := streamer.Switch(entries[0].ID); !ok {
		return fmt.Errorf("failed to activate scene %q", entries[0].Name)
	}
	streamer.Start()

	waitCtx, waitCancel := context.WithTimeout(ctx, spec.StartupTimeout)
	defer waitCancel()

	for i, sceneStream := range sceneStreams {
		if err := sceneStream.WaitForStart(waitCtx); err != nil {
			return fmt.Errorf("scene %q failed to start: %w", entries[i].Name, err)
		}
	}
	if err := outputStream.WaitForStart(waitCtx); err != nil {
		return fmt.Errorf("output failed to start: %w", err)
	}

	return runSwitcher(ctx, entries, streamer)
}

func (s *Service) runPassthroughSwitcher(ctx context.Context, spec SceneSpec, runSwitcher SwitcherRunner) error {
	if runSwitcher == nil {
		return fmt.Errorf("switcher runner is required")
	}

	inputStreams, entries, outputStream, cleanup, err := s.buildPassthroughSwitcherStreams(spec)
	if err != nil {
		return err
	}
	defer cleanup()

	streamer := core.NewStreamer(true, true, true)
	streamer.StartLife()
	defer streamer.Close()

	if err := streamer.UpdateStreams(inputStreams, []core.Stream{outputStream}); err != nil {
		return err
	}
	if ok := streamer.Switch(entries[0].ID); !ok {
		return fmt.Errorf("failed to activate input %q", entries[0].Name)
	}
	streamer.Start()

	waitCtx, waitCancel := context.WithTimeout(ctx, spec.StartupTimeout)
	defer waitCancel()

	for i, input := range inputStreams {
		if err := input.WaitForStart(waitCtx); err != nil {
			return fmt.Errorf("input %q failed to start: %w", entries[i].Name, err)
		}
	}
	if err := outputStream.WaitForStart(waitCtx); err != nil {
		return fmt.Errorf("output %q failed to start: %w", outputStream.GetID(), err)
	}

	return runSwitcher(ctx, entries, streamer)
}

func (s *Service) buildSingleScenePipeline(spec SceneSpec) (core.Stream, core.Stream, func(), error) {
	if spec.Mode == SceneModePassthrough {
		return s.buildPassthroughStreams(spec)
	}
	return s.buildSceneStreams(spec)
}

func (s *Service) buildSceneStreams(spec SceneSpec) (core.Stream, core.Stream, func(), error) {
	created := make([]core.Stream, 0, len(spec.InputURLs)+2)
	cleanup := func() {
		for _, stream := range created {
			if stream != nil {
				stream.Close()
			}
		}
	}

	sceneInputs := make([]Input, 0, len(spec.InputURLs))
	for idx, inputURL := range spec.InputURLs {
		streamID := fmt.Sprintf("%s-in-%d", spec.SceneID, idx+1)
		stream, err := streamfactory.NewInput(streamID, inputURL)
		if err != nil {
			cleanup()
			return nil, nil, func() {}, fmt.Errorf("create input %d: %w", idx+1, err)
		}
		created = append(created, stream)
		sceneInputs = append(sceneInputs, Input{Stream: stream, Layout: spec.Layouts[idx]})
	}

	sceneStream, err := NewScene(
		spec.SceneID,
		spec.Canvas,
		sceneInputs,
		WithAudioPassthroughFrom(spec.AudioFrom),
		WithAudioMixRatios(spec.AudioRatios),
		WithOutputFPS(spec.OutputFPS),
	)
	if err != nil {
		cleanup()
		return nil, nil, func() {}, err
	}
	created = append(created, sceneStream)

	outputStream, err := newOutput(spec.SceneID+"-out", spec.OutputURL, spec.HLSOptions)
	if err != nil {
		cleanup()
		return nil, nil, func() {}, err
	}
	created = append(created, outputStream)

	return sceneStream, outputStream, cleanup, nil
}

func (s *Service) buildPassthroughStreams(spec SceneSpec) (core.Stream, core.Stream, func(), error) {
	created := make([]core.Stream, 0, 2)
	cleanup := func() {
		for _, stream := range created {
			if stream != nil {
				stream.Close()
			}
		}
	}

	inputStream, err := streamfactory.NewInput(spec.SceneID+"-in-1", spec.InputURLs[0])
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("create input 1: %w", err)
	}
	created = append(created, inputStream)

	outputStream, err := newOutput(spec.SceneID+"-out", spec.OutputURL, spec.HLSOptions)
	if err != nil {
		cleanup()
		return nil, nil, func() {}, err
	}
	created = append(created, outputStream)

	return inputStream, outputStream, cleanup, nil
}

func (s *Service) buildPassthroughSwitcherStreams(spec SceneSpec) ([]core.Stream, []SceneEntry, core.Stream, func(), error) {
	created := make([]core.Stream, 0, len(spec.InputURLs)+1)
	cleanup := func() {
		for _, stream := range created {
			if stream != nil {
				stream.Close()
			}
		}
	}

	inputStreams := make([]core.Stream, 0, len(spec.InputURLs))
	entries := make([]SceneEntry, 0, len(spec.InputURLs))
	for idx, inputURL := range spec.InputURLs {
		streamID := fmt.Sprintf("%s-in-%d", spec.SceneID, idx+1)
		inputStream, err := streamfactory.NewInput(streamID, inputURL)
		if err != nil {
			cleanup()
			return nil, nil, nil, func() {}, fmt.Errorf("create input %d: %w", idx+1, err)
		}
		created = append(created, inputStream)
		inputStreams = append(inputStreams, inputStream)
		entries = append(entries, SceneEntry{
			ID:   streamID,
			Name: fmt.Sprintf("Input %d", idx+1),
		})
	}

	outputStream, err := newOutput(spec.SceneID+"-out", spec.OutputURL, spec.HLSOptions)
	if err != nil {
		cleanup()
		return nil, nil, nil, func() {}, err
	}
	created = append(created, outputStream)

	return inputStreams, entries, outputStream, cleanup, nil
}
