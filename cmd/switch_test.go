package cmd

import (
	"strings"
	"testing"
	"time"

	core "github.com/tupicapp/restreamer/core"
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

func TestParseSwitchArgs_AttachesLiveAndRecordSidecarsPerNextStream(t *testing.T) {
	spec, err := parseSwitchArgs([]string{
		"--live", "--record", "--segment-duration", "5s", "-i", "rtmp://localhost/live/in1",
		"--record", "-o", "rtmp://localhost/live/out1",
	})
	if err != nil {
		t.Fatalf("parseSwitchArgs returned error: %v", err)
	}

	if got := len(spec.inputs); got != 1 {
		t.Fatalf("expected 1 input, got %d", got)
	}
	if got := len(spec.inputs[0].sidecars); got != 2 {
		t.Fatalf("expected 2 input sidecars, got %d", got)
	}
	if spec.inputs[0].sidecars[0].mode != "live" || spec.inputs[0].sidecars[1].mode != "record" {
		t.Fatalf("unexpected input sidecar modes: %#v", spec.inputs[0].sidecars)
	}
	if spec.inputs[0].sidecars[0].segmentDuration != 5*time.Second {
		t.Fatalf("unexpected live segment duration: %v", spec.inputs[0].sidecars[0].segmentDuration)
	}

	if got := len(spec.outputs); got != 1 {
		t.Fatalf("expected 1 output, got %d", got)
	}
	if got := len(spec.outputs[0].sidecars); got != 1 {
		t.Fatalf("expected 1 output sidecar, got %d", got)
	}
	if spec.outputs[0].sidecars[0].mode != "record" {
		t.Fatalf("unexpected output sidecar mode: %#v", spec.outputs[0].sidecars[0])
	}
}

func TestBuildSwitchTUILines_ShowsServedStreamsFromStreamerState(t *testing.T) {
	state := core.StreamerState{
		CurrentInputID: "route-a-in-2",
		StreamInputs: []*core.State{
			{StreamID: "route-a-in-1", Url: "http://127.0.0.1:8091/milad-nob/milad.m3u8"},
			{
				StreamID: "route-a-in-2",
				Url:      "rtmp://127.0.0.1:1938/live/1",
				Served: []core.ServedState{
					{
						StreamID:  "route-a-in-2-live",
						Url:       "file:///tmp/in-2-live/stream.m3u8",
						LocalPath: "/tmp/in-2-live",
						ServeType: "hls",
						ServeMode: "live",
					},
					{
						StreamID:  "route-a-in-2-record",
						Url:       "file:///tmp/in-2-record/stream.m3u8",
						LocalPath: "/tmp/in-2-record",
						ServeType: "hls",
						ServeMode: "record",
					},
				},
			},
		},
		StreamOutputs: []*core.State{
			{
				StreamID: "route-a-out-1",
				Url:      "rtmp://localhost:1938/out",
				Served: []core.ServedState{{
					StreamID:  "route-a-out-1-record",
					Url:       "file:///tmp/out-1/stream.m3u8",
					LocalPath: "/tmp/out-1",
					ServeType: "hls",
					ServeMode: "record",
				}},
			},
		},
	}

	entries := buildSwitchEntries(state, map[string]string{
		"route-a-in-1": "Input 1",
		"route-a-in-2": "Input 2",
	})
	lines := buildSwitchTUILines(entries, state, "route-a-in-1", &switchFileServer{
		baseURL: "http://127.0.0.1:8092",
		rootDir: "/tmp",
	}, "", "")
	rendered := strings.Join(lines, "\n")

	if !strings.Contains(rendered, "Input Sources:") {
		t.Fatalf("expected input sources section, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "http://127.0.0.1:8091") {
		t.Fatalf("expected input source url, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "Output Targets:") {
		t.Fatalf("expected output targets section, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "Served Paths:") {
		t.Fatalf("expected served paths section, got:\n%s", rendered)
	}
	if strings.Contains(rendered, "input route-a-in-1: url=") {
		t.Fatalf("did not expect plain input source url in served paths, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "input route-a-in-2-live: hls live") {
		t.Fatalf("expected live served path line, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "output route-a-out-1-record: hls record") {
		t.Fatalf("expected output served path line, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "playlist=/tmp/in-2-live/stream.m3u8") {
		t.Fatalf("expected local playlist path, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "http=http://127.0.0.1:8092/in-2-live/stream.m3u8") {
		t.Fatalf("expected served http url, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "2.  Input 2") || !strings.Contains(rendered, "● LIVE") {
		t.Fatalf("expected active input marker, got:\n%s", rendered)
	}
}

func TestBuildSwitchEntries_UsesCurrentStreamerStateInputs(t *testing.T) {
	state := core.StreamerState{
		StreamInputs: []*core.State{
			{StreamID: "route-a-in-2", Url: "rtmp://localhost/live/2"},
		},
	}

	entries := buildSwitchEntries(state, map[string]string{
		"route-a-in-1": "Input 1",
		"route-a-in-2": "Input 2",
	})
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].id != "route-a-in-2" {
		t.Fatalf("unexpected entry id: %q", entries[0].id)
	}
	if entries[0].name != "Input 2" {
		t.Fatalf("unexpected entry name: %q", entries[0].name)
	}
}

func TestParseSwitchRuntimeCommand_AddInputWithSidecars(t *testing.T) {
	req, err := parseSwitchRuntimeCommand("/ai --live --record --segment-duration 5s rtmp://localhost/live/in3")
	if err != nil {
		t.Fatalf("parseSwitchRuntimeCommand returned error: %v", err)
	}
	if req.kind != "ai" || req.input == nil {
		t.Fatalf("unexpected request: %#v", req)
	}
	if req.input.url != "rtmp://localhost/live/in3" {
		t.Fatalf("unexpected input url: %q", req.input.url)
	}
	if len(req.input.sidecars) != 2 {
		t.Fatalf("expected 2 input sidecars, got %d", len(req.input.sidecars))
	}
	if req.input.sidecars[0].mode != "live" || req.input.sidecars[1].mode != "record" {
		t.Fatalf("unexpected input sidecars: %#v", req.input.sidecars)
	}
	if req.input.sidecars[0].segmentDuration != 5*time.Second {
		t.Fatalf("unexpected live segment duration: %v", req.input.sidecars[0].segmentDuration)
	}
}

func TestParseSwitchRuntimeCommand_AddOutputWithLiveMode(t *testing.T) {
	req, err := parseSwitchRuntimeCommand("/ao --live ./out/stream.m3u8")
	if err != nil {
		t.Fatalf("parseSwitchRuntimeCommand returned error: %v", err)
	}
	if req.kind != "ao" || req.output == nil {
		t.Fatalf("unexpected request: %#v", req)
	}
	if !req.output.hlsLive {
		t.Fatalf("expected output hlsLive to be true")
	}
	if req.output.url != "./out/stream.m3u8" {
		t.Fatalf("unexpected output url: %q", req.output.url)
	}
	if len(req.output.sidecars) != 1 || req.output.sidecars[0].mode != "live" {
		t.Fatalf("unexpected output sidecars: %#v", req.output.sidecars)
	}
}

func TestParseSwitchRuntimeCommand_RemoveCommands(t *testing.T) {
	req, err := parseSwitchRuntimeCommand("/di route-a-in-2")
	if err != nil {
		t.Fatalf("parseSwitchRuntimeCommand returned error: %v", err)
	}
	if req.kind != "di" || req.targetID != "route-a-in-2" {
		t.Fatalf("unexpected remove-input request: %#v", req)
	}

	req, err = parseSwitchRuntimeCommand("/do route-a-out-2")
	if err != nil {
		t.Fatalf("parseSwitchRuntimeCommand returned error: %v", err)
	}
	if req.kind != "do" || req.targetID != "route-a-out-2" {
		t.Fatalf("unexpected remove-output request: %#v", req)
	}
}
