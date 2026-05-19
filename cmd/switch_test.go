package cmd

import (
	"testing"
	"time"
)

func TestBuildSwitchSpec_ValidInputsAndOutputs(t *testing.T) {
	spec, err := buildSwitchSpec(switchCommandOptions{
		routeID:        "route-a",
		inputs:         []string{"rtmp://localhost/live/in1", "rtmp://localhost/live/in2"},
		outputs:        []string{"rtmp://localhost/live/out1"},
		startupTimeout: 20 * time.Second,
	}, []string{"rtmp://localhost/live/out2"})
	if err != nil {
		t.Fatalf("buildSwitchSpec returned error: %v", err)
	}
	if spec.routeID != "route-a" {
		t.Fatalf("expected routeID route-a, got %q", spec.routeID)
	}
	if len(spec.inputURLs) != 2 {
		t.Fatalf("expected 2 inputs, got %d", len(spec.inputURLs))
	}
	if len(spec.outputURLs) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(spec.outputURLs))
	}
}

func TestBuildSwitchSpec_AcceptsSingleInput(t *testing.T) {
	spec, err := buildSwitchSpec(switchCommandOptions{
		inputs:         []string{"rtmp://localhost/live/in1"},
		outputs:        []string{"rtmp://localhost/live/out1"},
		startupTimeout: 10 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("expected single input to be accepted, got error: %v", err)
	}
	if len(spec.inputURLs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(spec.inputURLs))
	}
}

func TestBuildSwitchSpec_RejectsNoInputs(t *testing.T) {
	_, err := buildSwitchSpec(switchCommandOptions{
		outputs:        []string{"rtmp://localhost/live/out1"},
		startupTimeout: 10 * time.Second,
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing inputs")
	}
}

func TestBuildSwitchSpec_RejectsNoOutputs(t *testing.T) {
	_, err := buildSwitchSpec(switchCommandOptions{
		inputs:         []string{"rtmp://localhost/live/in1", "rtmp://localhost/live/in2"},
		startupTimeout: 10 * time.Second,
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing outputs")
	}
}

func TestShouldShowSwitchHelp(t *testing.T) {
	if !shouldShowSwitchHelp(switchCommandOptions{}, nil) {
		t.Fatal("expected help for empty invocation")
	}
	if shouldShowSwitchHelp(switchCommandOptions{inputs: []string{"rtmp://localhost/live/in1"}}, nil) {
		t.Fatal("did not expect help when inputs are provided")
	}
}
