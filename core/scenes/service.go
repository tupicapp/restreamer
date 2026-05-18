package scenes

import (
	"fmt"
	"time"

	shared "restreamer/irajstreamer/core/shared"
	"restreamer/irajstreamer/core/streamfactory"
)

type SceneMode string

const (
	SceneModeCompose     SceneMode = "compose"
	SceneModePassthrough SceneMode = "passthrough"
)

type SceneSpec struct {
	Mode           SceneMode
	SceneID        string
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

type SceneEntry struct {
	ID   string
	Name string
}

type MultiSceneDefinition struct {
	Name     string
	InputURL []string
	Layouts  []shared.VideoLayout
}

type MultiSceneSpec struct {
	OutputURL      string
	HLSOptions     *streamfactory.HLSOutputOptions
	HasCanvas      bool
	Canvas         shared.CanvasSpec
	AudioFrom      int
	AudioRatios    []int
	OutputFPS      int
	StartupTimeout time.Duration
	Definitions    []MultiSceneDefinition
}

func DeriveCanvas(layouts []shared.VideoLayout) (shared.CanvasSpec, error) {
	if len(layouts) == 0 {
		return shared.CanvasSpec{}, fmt.Errorf("--canvas is required when no --layout values are provided")
	}

	maxX := 0
	maxY := 0
	for idx, layout := range layouts {
		if err := layout.Validate(); err != nil {
			return shared.CanvasSpec{}, fmt.Errorf("layout %d invalid: %w", idx+1, err)
		}
		right := layout.X + layout.Width
		bottom := layout.Y + layout.Height
		if right > maxX {
			maxX = right
		}
		if bottom > maxY {
			maxY = bottom
		}
	}

	if _, err := shared.ExpectedYUV420PSize(maxX, maxY); err != nil {
		return shared.CanvasSpec{}, fmt.Errorf("derived canvas invalid: %w", err)
	}
	return shared.NewBlackCanvasSpec(maxX, maxY), nil
}
