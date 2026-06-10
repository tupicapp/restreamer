package api

import (
	"testing"

	manifestpkg "github.com/tupicapp/restreamer/core/manifest"
)

func TestWorkspaceService_UpdateManifestStoresAndStartsRunner(t *testing.T) {
	t.Parallel()

	runners := NewRunnerService()
	studios := NewStudioRegistry()
	workspace := NewWorkspaceService(studios, runners)

	manifest := manifestpkg.Manifest{
		Version:   "1.0",
		ChannelID: "channel-a",
		Scenes: []manifestpkg.Scene{{
			ID: "scene-1",
			Slots: []manifestpkg.Slot{{
				Elements: []manifestpkg.Element{{
					ID:         "el-1",
					URL:        "rtmp://127.0.0.1/live/channel-a-primary",
					SourceType: manifestpkg.SourceTypeLink,
					SourceID:   "camera",
					StartsAt:   1763472641,
					FinishesAt: -1,
				}},
			}},
		}},
	}

	state, err := workspace.UpdateManifest("channel-a", manifest)
	if err != nil {
		t.Fatalf("UpdateManifest() error = %v", err)
	}
	if state.Manifest == nil {
		t.Fatal("expected manifest in workspace state")
	}
	if state.Runner == nil {
		t.Fatal("expected runner state")
	}
	if !state.Runner.IsStarted {
		t.Fatal("expected runner to auto-start after manifest apply")
	}
	if !state.Runner.StreamerState.IsStarted {
		t.Fatal("expected nested streamer state to reflect started runner")
	}
	if len(state.Runner.StreamerState.StreamInputs) != 1 {
		t.Fatalf("expected one runtime input, got %d", len(state.Runner.StreamerState.StreamInputs))
	}
	if len(state.Runner.StreamerState.StreamOutputs) != 1 {
		t.Fatalf("expected one runtime output, got %d", len(state.Runner.StreamerState.StreamOutputs))
	}
	if state.Runner.Outputs[0].LocalPath == "" {
		t.Fatal("expected output local path from default channel output")
	}
}

func TestWorkspaceService_UpdateManifestRejectsOverlap(t *testing.T) {
	t.Parallel()

	runners := NewRunnerService()
	studios := NewStudioRegistry()
	workspace := NewWorkspaceService(studios, runners)

	manifest := manifestpkg.Manifest{
		Version:   "1.0",
		ChannelID: "channel-a",
		Scenes: []manifestpkg.Scene{{
			ID: "scene-1",
			Slots: []manifestpkg.Slot{{
				Elements: []manifestpkg.Element{
					{
						ID:         "el-1",
						URL:        "rtmp://127.0.0.1/live/channel-a-primary",
						StartsAt:   1763472641,
						FinishesAt: 1763472650,
					},
					{
						ID:         "el-2",
						URL:        "rtmp://127.0.0.1/live/channel-a-backup",
						StartsAt:   1763472649,
						FinishesAt: -1,
					},
				},
			}},
		}},
	}

	if _, err := workspace.UpdateManifest("channel-a", manifest); err == nil {
		t.Fatal("expected overlap validation error")
	}
}
