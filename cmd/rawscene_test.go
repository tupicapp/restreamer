package cmd

import (
	"testing"
	"time"
)

func TestBuildRawSceneSpec_DefaultsSingleInputLayoutToCanvas(t *testing.T) {
	spec, err := buildRawSceneSpec(rawSceneCommandOptions{
		streamID:       "raw-1",
		inputs:         []string{"rtmp://localhost/live/a"},
		canvas:         "1280x720",
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("buildRawSceneSpec returned error: %v", err)
	}

	if len(spec.layouts) != 1 {
		t.Fatalf("expected 1 layout, got %d", len(spec.layouts))
	}
	if spec.layouts[0].Width != 1280 || spec.layouts[0].Height != 720 {
		t.Fatalf("expected full-canvas layout, got %+v", spec.layouts[0])
	}
}

func TestBuildRawSceneSpec_DerivesCanvasFromLayouts(t *testing.T) {
	spec, err := buildRawSceneSpec(rawSceneCommandOptions{
		streamID:       "raw-2",
		inputs:         []string{"rtmp://localhost/live/a", "rtmp://localhost/live/b"},
		layouts:        []string{"0,0,640,360", "640,0,640,360"},
		output:         "rtmp://localhost/live/out",
		audioFrom:      1,
		audioRatios:    []int{30, 70},
		outputFPS:      30,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("buildRawSceneSpec returned error: %v", err)
	}

	if spec.canvas.Width != 1280 || spec.canvas.Height != 360 {
		t.Fatalf("unexpected derived canvas: %+v", spec.canvas)
	}
	if len(spec.audioRatios) != 2 || spec.audioRatios[0] != 30 || spec.audioRatios[1] != 70 {
		t.Fatalf("unexpected audio ratios: %v", spec.audioRatios)
	}
}

func TestBuildRawSceneSpec_PreservesOptionalLayoutFields(t *testing.T) {
	spec, err := buildRawSceneSpec(rawSceneCommandOptions{
		streamID:       "raw-optional-layout",
		inputs:         []string{"rtmp://localhost/live/a", "rtmp://localhost/live/b"},
		layouts:        []string{"0,0,640,360,1,0.10", "320,180,640,360,5,0.35"},
		output:         "rtmp://localhost/live/out",
		audioFrom:      0,
		outputFPS:      25,
		startupTimeout: 30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("buildRawSceneSpec returned error: %v", err)
	}

	if spec.layouts[0].ZIndex != 1 || spec.layouts[0].Transparency != 0.10 {
		t.Fatalf("unexpected first layout optional fields: %+v", spec.layouts[0])
	}
	if spec.layouts[1].ZIndex != 5 || spec.layouts[1].Transparency != 0.35 {
		t.Fatalf("unexpected second layout optional fields: %+v", spec.layouts[1])
	}
}

func TestBuildRawSceneSpec_RejectsMismatchedInputLayoutCount(t *testing.T) {
	_, err := buildRawSceneSpec(rawSceneCommandOptions{
		streamID:       "raw-3",
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

func TestShouldShowRawSceneHelp(t *testing.T) {
	if !shouldShowRawSceneHelp(rawSceneCommandOptions{}, nil) {
		t.Fatal("expected bare rawscene invocation to show help")
	}

	if shouldShowRawSceneHelp(rawSceneCommandOptions{
		inputs: []string{"rtmp://localhost/live/a"},
	}, nil) {
		t.Fatal("expected invocation with inputs to skip help shortcut")
	}
}

func TestRawSceneCommand_LayoutFlagPreservesCommaSeparatedValue(t *testing.T) {
	cmd := NewRawSceneCommand()

	if err := cmd.ParseFlags([]string{
		"-i", "rtmp://127.0.0.1:1938/live/1",
		"--layout", "0,0,640,360,1,0.10",
		"-i", "rtmp://127.0.0.1:1938/live/2",
		"--layout", "640,0,640,360,5,0.35",
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
	if layouts[0] != "0,0,640,360,1,0.10" || layouts[1] != "640,0,640,360,5,0.35" {
		t.Fatalf("unexpected layout values: %v", layouts)
	}
}
