package api

import (
	"testing"

	manifestpkg "github.com/tupicapp/restreamer/core/manifest"
)

func TestStudioRegistry_IsolatesChannels(t *testing.T) {
	t.Parallel()

	registry := NewStudioRegistry()

	channelA := registry.ForChannel("channel-a")
	channelB := registry.ForChannel("channel-b")

	stateA, err := channelA.OpenStream("camera")
	if err != nil {
		t.Fatalf("OpenStream(channel-a) error = %v", err)
	}
	if stateA.ChannelID != "channel-a" {
		t.Fatalf("expected channel-a state, got %q", stateA.ChannelID)
	}

	stateB := channelB.State()
	if stateB.ChannelID != "channel-b" {
		t.Fatalf("expected channel-b state, got %q", stateB.ChannelID)
	}
	if stateB.StreamMode != "idle" {
		t.Fatalf("expected channel-b stream mode to remain idle, got %q", stateB.StreamMode)
	}
}

func TestRunnerService_CreateRejectsMismatchedManifestChannelID(t *testing.T) {
	t.Parallel()

	service := NewRunnerService()

	manifest := manifestpkg.Manifest{
		Version:   "1.0",
		ChannelID: "channel-b",
		Scenes: []manifestpkg.Scene{{
			ID: "scene-1",
			Slots: []manifestpkg.Slot{{
				Elements: []manifestpkg.Element{{
					ID:         "el-1",
					URL:        "https://example.com/a.m3u8",
					StartsAt:   1763472641,
					FinishesAt: -1,
				}},
			}},
		}},
	}

	if _, err := service.Create("channel-a", manifest); err == nil {
		t.Fatal("expected mismatch error")
	}
}
