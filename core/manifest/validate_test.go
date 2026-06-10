package manifest

import "testing"

func TestValidateManifest_V1AcceptsMinimalTimeline(t *testing.T) {
	t.Parallel()

	err := ValidateManifest(Manifest{
		Version:   "1.0",
		ChannelID: "channel-123",
		Scenes: []Scene{{
			ID: "scene-1",
			Slots: []Slot{{
				Elements: []Element{{
					ID:         "el-1",
					URL:        "https://example.com/a.m3u8",
					StartsAt:   1763472641,
					FinishesAt: -1,
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("ValidateManifest() error = %v", err)
	}
}

func TestValidateManifest_V1AcceptsEmptyChannelID(t *testing.T) {
	t.Parallel()

	err := ValidateManifest(Manifest{
		Version: "1.0",
		Scenes: []Scene{{
			ID: "scene-1",
			Slots: []Slot{{
				Elements: []Element{{
					ID:         "el-1",
					URL:        "https://example.com/a.m3u8",
					StartsAt:   1763472641,
					FinishesAt: -1,
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("ValidateManifest() with empty channel_id error = %v", err)
	}
}

func TestValidateManifest_V1RejectsV10Fields(t *testing.T) {
	t.Parallel()

	err := ValidateManifest(Manifest{
		Version:       "1.0",
		ActiveSceneID: "scene-1",
		Scenes: []Scene{{
			ID: "scene-1",
			Slots: []Slot{{
				Elements: []Element{{
					URL:        "https://example.com/a.m3u8",
					StartsAt:   1763472641,
					FinishesAt: -1,
				}},
			}},
		}},
	})
	if err == nil {
		t.Fatal("expected validation error for active_scene_id")
	}
}

func TestValidateManifest_V1RejectsMultipleScenesAndSlots(t *testing.T) {
	t.Parallel()

	err := ValidateManifest(Manifest{
		Version: "1.0",
		Scenes: []Scene{
			{
				ID: "scene-1",
				Slots: []Slot{{
					Elements: []Element{{
						URL:        "https://example.com/a.m3u8",
						StartsAt:   1763472641,
						FinishesAt: -1,
					}},
				}},
			},
			{
				ID: "scene-2",
				Slots: []Slot{{
					Elements: []Element{{
						URL:        "https://example.com/b.m3u8",
						StartsAt:   1763472641,
						FinishesAt: -1,
					}},
				}},
			},
		},
	})
	if err == nil {
		t.Fatal("expected validation error for multiple scenes")
	}
}

func TestValidateManifest_V1RejectsNonContiguousTimeline(t *testing.T) {
	t.Parallel()

	err := ValidateManifest(Manifest{
		Version: "1.0",
		Scenes: []Scene{{
			ID: "scene-1",
			Slots: []Slot{{
				Elements: []Element{
					{
						ID:         "el-1",
						URL:        "https://example.com/a.m3u8",
						StartsAt:   1763472641,
						FinishesAt: 1763472642,
					},
					{
						ID:         "el-2",
						URL:        "https://example.com/b.m3u8",
						StartsAt:   1763472644,
						FinishesAt: -1,
					},
				},
			}},
		}},
	})
	if err == nil {
		t.Fatal("expected validation error for non-contiguous elements")
	}
}
