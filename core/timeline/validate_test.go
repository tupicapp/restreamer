package timeline

import (
	"testing"
	"time"
)

func TestValidatePlan_AcceptsOrderedTimeline(t *testing.T) {
	t.Parallel()

	start := time.Unix(1763472641, 0).UTC()
	end := start.Add(time.Minute)
	plan := Plan{
		Kind:            PlanKindTimeline,
		ManifestVersion: "1.0",
		InitialSceneID:  "scene-1",
		Scenes: []ScenePlan{{
			ID: "scene-1",
			Slots: []SlotPlan{{
				ID: "slot-1",
				Elements: []ElementPlan{{
					ID:         "element-1",
					URL:        "https://example.com/a.m3u8",
					StartsAt:   start,
					FinishesAt: &end,
				}},
			}},
		}},
		Events: []Event{
			{At: start, Kind: EventKindActivateScene, SceneID: "scene-1"},
			{At: start, Kind: EventKindActivateElement, SceneID: "scene-1", SlotID: "slot-1", ElementID: "element-1"},
			{At: end, Kind: EventKindDeactivateElement, SceneID: "scene-1", SlotID: "slot-1", ElementID: "element-1"},
		},
	}

	if err := ValidatePlan(plan); err != nil {
		t.Fatalf("ValidatePlan() error = %v", err)
	}
}
