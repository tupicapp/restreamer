package cmd

import (
	"testing"
	"time"
)

func TestBuildSceneSpec_DefaultsSingleInputLayoutToCanvas(t *testing.T) {
	spec, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-one",
		inputs:         []string{"rtmp://localhost/live/a"},
		canvas:         "1280x720",
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("buildSceneSpec returned error: %v", err)
	}
	if spec.mode != sceneModeCompose {
		t.Fatalf("expected compose mode, got %q", spec.mode)
	}

	if len(spec.layouts) != 1 {
		t.Fatalf("expected 1 layout, got %d", len(spec.layouts))
	}
	if spec.layouts[0].Width != 1280 || spec.layouts[0].Height != 720 {
		t.Fatalf("expected full-canvas layout, got %+v", spec.layouts[0])
	}
	if spec.canvas.Width != 1280 || spec.canvas.Height != 720 {
		t.Fatalf("unexpected canvas: %+v", spec.canvas)
	}
}

func TestBuildSceneSpec_DerivesCanvasFromLayouts(t *testing.T) {
	spec, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-two",
		inputs:         []string{"rtmp://localhost/live/a", "rtmp://localhost/live/b"},
		layouts:        []string{"0,0,640,360", "640,0,640,360"},
		audioRatios:    []int{25, 75},
		output:         "rtmp://localhost/live/out",
		audioFrom:      1,
		outputFPS:      30,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("buildSceneSpec returned error: %v", err)
	}
	if spec.mode != sceneModeCompose {
		t.Fatalf("expected compose mode, got %q", spec.mode)
	}

	if spec.canvas.Width != 1280 || spec.canvas.Height != 360 {
		t.Fatalf("unexpected derived canvas: %+v", spec.canvas)
	}
	if spec.audioFrom != 1 {
		t.Fatalf("expected audioFrom=1, got %d", spec.audioFrom)
	}
	if len(spec.audioRatios) != 2 || spec.audioRatios[0] != 25 || spec.audioRatios[1] != 75 {
		t.Fatalf("unexpected audioRatios: %v", spec.audioRatios)
	}
}

func TestBuildSceneSpec_RejectsMismatchedInputLayoutCount(t *testing.T) {
	_, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-three",
		inputs:         []string{"rtmp://localhost/live/a", "rtmp://localhost/live/b"},
		layouts:        []string{"0,0,640,360"},
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
}

func TestBuildSceneSpec_RejectsInvalidAudioRatioCount(t *testing.T) {
	_, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-audio-count",
		inputs:         []string{"rtmp://localhost/live/a", "rtmp://localhost/live/b"},
		layouts:        []string{"0,0,640,360", "640,0,640,360"},
		audioRatios:    []int{100},
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err == nil {
		t.Fatal("expected invalid audio ratio count error, got nil")
	}
}

func TestBuildSceneSpec_RejectsInvalidAudioRatioSum(t *testing.T) {
	_, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-audio-sum",
		inputs:         []string{"rtmp://localhost/live/a", "rtmp://localhost/live/b"},
		layouts:        []string{"0,0,640,360", "640,0,640,360"},
		audioRatios:    []int{40, 40},
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err == nil {
		t.Fatal("expected invalid audio ratio sum error, got nil")
	}
}

func TestBuildSceneSpec_UsesPassthroughForSingleInputWithoutLayoutOrCanvas(t *testing.T) {
	spec, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-direct",
		inputs:         []string{"rtmp://localhost/live/a"},
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("buildSceneSpec returned error: %v", err)
	}

	if spec.mode != sceneModePassthrough {
		t.Fatalf("expected passthrough mode, got %q", spec.mode)
	}
	if len(spec.layouts) != 0 {
		t.Fatalf("expected no layouts in passthrough mode, got %d", len(spec.layouts))
	}
	if spec.canvas.Width != 0 || spec.canvas.Height != 0 {
		t.Fatalf("expected zero canvas in passthrough mode, got %+v", spec.canvas)
	}
}

func TestBuildSceneSpec_UsesPassthroughSwitcherForMultipleInputsWithoutLayoutOrCanvas(t *testing.T) {
	spec, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-switch",
		inputs:         []string{"rtmp://localhost/live/a", "rtmp://localhost/live/b"},
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("buildSceneSpec returned error: %v", err)
	}

	if spec.mode != sceneModePassthrough {
		t.Fatalf("expected passthrough mode, got %q", spec.mode)
	}
	if len(spec.inputURLs) != 2 {
		t.Fatalf("expected 2 inputs, got %d", len(spec.inputURLs))
	}
	if len(spec.layouts) != 0 {
		t.Fatalf("expected no layouts in passthrough mode, got %d", len(spec.layouts))
	}
}

func TestBuildSceneSpec_RejectsAudioRatioForPassthroughMode(t *testing.T) {
	_, err := buildSceneSpec(sceneCommandOptions{
		sceneID:        "scene-passthrough-audio",
		inputs:         []string{"rtmp://localhost/live/a"},
		audioRatios:    []int{100},
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err == nil {
		t.Fatal("expected passthrough audio ratio error, got nil")
	}
}

func TestParseVideoLayout_WithOptionalFields(t *testing.T) {
	layout, err := parseVideoLayout("10,20,640,360,3,0.25")
	if err != nil {
		t.Fatalf("parseVideoLayout returned error: %v", err)
	}

	if layout.X != 10 || layout.Y != 20 || layout.Width != 640 || layout.Height != 360 {
		t.Fatalf("unexpected layout geometry: %+v", layout)
	}
	if layout.ZIndex != 3 {
		t.Fatalf("expected z-index 3, got %d", layout.ZIndex)
	}
	if layout.Transparency != 0.25 {
		t.Fatalf("expected transparency 0.25, got %f", layout.Transparency)
	}
}

func TestShouldShowSceneHelp(t *testing.T) {
	if !shouldShowSceneHelp(sceneCommandOptions{}, nil) {
		t.Fatal("expected bare scene invocation to show help")
	}

	if shouldShowSceneHelp(sceneCommandOptions{
		inputs: []string{"rtmp://localhost/live/a"},
	}, nil) {
		t.Fatal("expected invocation with inputs to skip help shortcut")
	}
}

func TestSceneCommand_LayoutFlagPreservesCommaSeparatedValue(t *testing.T) {
	cmd := NewSceneCommand()

	if err := cmd.ParseFlags([]string{
		"-i", "rtmp://127.0.0.1:1938/live/1",
		"--layout", "0,0,640,360",
		"-i", "rtmp://127.0.0.1:1938/live/2",
		"--layout", "640,0,640,360",
		"--canvas", "1280x360",
	}); err != nil {
		t.Fatalf("ParseFlags returned error: %v", err)
	}

	layouts, err := cmd.Flags().GetStringArray("layout")
	if err != nil {
		t.Fatalf("GetStringArray returned error: %v", err)
	}
	if len(layouts) != 2 {
		t.Fatalf("expected 2 layout values, got %d (%v)", len(layouts), layouts)
	}
	if layouts[0] != "0,0,640,360" || layouts[1] != "640,0,640,360" {
		t.Fatalf("unexpected layout values: %v", layouts)
	}
}

func TestSceneCommand_AudioRatioFlagPreservesInputOrder(t *testing.T) {
	cmd := NewSceneCommand()

	if err := cmd.ParseFlags([]string{
		"-i", "rtmp://127.0.0.1:1938/live/1",
		"--layout", "0,0,640,360",
		"-i", "rtmp://127.0.0.1:1938/live/2",
		"--layout", "640,0,640,360",
		"--audio-ratio", "10",
		"--audio-ratio", "90",
		"--canvas", "1280x360",
	}); err != nil {
		t.Fatalf("ParseFlags returned error: %v", err)
	}

	ratios, err := cmd.Flags().GetIntSlice("audio-ratio")
	if err != nil {
		t.Fatalf("GetIntSlice returned error: %v", err)
	}
	if len(ratios) != 2 {
		t.Fatalf("expected 2 audio ratio values, got %d (%v)", len(ratios), ratios)
	}
	if ratios[0] != 10 || ratios[1] != 90 {
		t.Fatalf("unexpected audio ratio values: %v", ratios)
	}
}
