package irajstreamer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	manifestpkg "github.com/tupicapp/restreamer/core/manifest"
)

func TestNewManifestRunner_StartStopUpdate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	start := now.Add(1 * time.Second).Unix()
	switchAt := start + 1
	manifest := manifestpkg.Manifest{
		Version: "1.0",
		Scenes: []manifestpkg.Scene{{
			ID: "scene-1",
			Slots: []manifestpkg.Slot{{
				Elements: []manifestpkg.Element{
					{
						ID:         "el-1",
						URL:        "rtmp://127.0.0.1/live/first",
						StartsAt:   start,
						FinishesAt: switchAt,
					},
					{
						ID:         "el-2",
						URL:        "rtmp://127.0.0.1/live/second",
						StartsAt:   switchAt,
						FinishesAt: -1,
					},
				},
			}},
		}},
	}

	runner, err := NewManifestRunner(manifest)
	if err != nil {
		t.Fatalf("NewManifestRunner() error = %v", err)
	}
	if err := runner.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(2500 * time.Millisecond)

	state := runner.State()
	if !state.IsStarted {
		t.Fatal("expected runner to be started")
	}
	if state.PreviewURL == "" {
		t.Fatal("expected preview url from default timeline output")
	}
	if state.Outputs[0].LocalPath == "" {
		t.Fatal("expected output local path from default timeline output")
	}
	if state.Outputs[0].URL == "" {
		t.Fatal("expected output url to be populated")
	}
	if _, err := os.Stat(filepath.Join(state.Outputs[0].LocalPath, "stream.m3u8")); err != nil {
		t.Fatalf("expected bootstrap playlist to exist, stat error = %v", err)
	}
	if got := len(state.StreamerState.StreamOutputs); got != 1 {
		t.Fatalf("expected one output in streamer state, got %d", got)
	}
	if state.ActiveSceneID != "scene-1" {
		t.Fatalf("expected active scene scene-1, got %q", state.ActiveSceneID)
	}
	if got := state.ActiveElements["scene-1-slot-1"]; got != "el-2" {
		t.Fatalf("expected active element el-2, got %q", got)
	}

	timelineRunner := runner.(*TimeLinedStreamer)
	if got := timelineRunner.base.State().CurrentInputID; got != "el-2" {
		t.Fatalf("expected base streamer current input el-2, got %q", got)
	}

	updated := manifest
	updated.Scenes[0].Slots[0].Elements[0].ID = "el-2"
	updated.Scenes[0].Slots[0].Elements = []manifestpkg.Element{{
		ID:         "el-3",
		URL:        "rtmp://127.0.0.1/live/third",
		StartsAt:   time.Now().UTC().Add(1 * time.Second).Unix(),
		FinishesAt: -1,
	}}
	if err := runner.UpdateManifest(updated); err != nil {
		t.Fatalf("UpdateManifest() error = %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	if got := timelineRunner.base.State().CurrentInputID; got != "el-3" {
		t.Fatalf("expected updated base streamer current input el-3, got %q", got)
	}

	if err := runner.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := len(timelineRunner.base.State().StreamInputs); got != 0 {
		t.Fatalf("expected runtime inputs to be torn down on stop, got %d", got)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
