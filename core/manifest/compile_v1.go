package manifest

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tupicapp/restreamer/core/timeline"
)

func compileV1(m Manifest) (timeline.Plan, error) {
	plan := timeline.Plan{
		Kind:            timeline.PlanKindTimeline,
		ManifestVersion: m.Version,
		InitialSceneID:  strings.TrimSpace(m.Scenes[0].ID),
		Scenes:          make([]timeline.ScenePlan, 0, len(m.Scenes)),
	}

	for _, scene := range m.Scenes {
		scenePlan := timeline.ScenePlan{
			ID:    strings.TrimSpace(scene.ID),
			Slots: make([]timeline.SlotPlan, 0, len(scene.Slots)),
		}

		for slotIdx, slot := range scene.Slots {
			slotID := strings.TrimSpace(slot.ID)
			if slotID == "" {
				slotID = fmt.Sprintf("%s-slot-%d", scenePlan.ID, slotIdx+1)
			}
			elements := append([]Element(nil), slot.Elements...)
			sort.SliceStable(elements, func(i, j int) bool {
				return elements[i].StartsAt < elements[j].StartsAt
			})
			slotPlan := timeline.SlotPlan{
				ID:       slotID,
				Elements: make([]timeline.ElementPlan, 0, len(elements)),
			}

			for elementIdx, element := range elements {
				elementID := strings.TrimSpace(element.ID)
				if elementID == "" {
					elementID = fmt.Sprintf("%s-element-%d", slotID, elementIdx+1)
				}
				startAt := time.Unix(element.StartsAt, 0).UTC()
				var finishAt *time.Time
				if element.FinishesAt != -1 {
					finish := time.Unix(element.FinishesAt, 0).UTC()
					finishAt = &finish
				}

				slotPlan.Elements = append(slotPlan.Elements, timeline.ElementPlan{
					ID:         elementID,
					SourceID:   strings.TrimSpace(element.SourceID),
					URL:        strings.TrimSpace(element.URL),
					StartsAt:   startAt,
					FinishesAt: finishAt,
				})

				plan.Events = append(plan.Events, timeline.Event{
					At:        startAt,
					Kind:      timeline.EventKindActivateElement,
					SceneID:   scenePlan.ID,
					SlotID:    slotID,
					ElementID: elementID,
				})
				if finishAt != nil {
					plan.Events = append(plan.Events, timeline.Event{
						At:        *finishAt,
						Kind:      timeline.EventKindDeactivateElement,
						SceneID:   scenePlan.ID,
						SlotID:    slotID,
						ElementID: elementID,
					})
				}
			}

			scenePlan.Slots = append(scenePlan.Slots, slotPlan)
		}

		plan.Scenes = append(plan.Scenes, scenePlan)
	}

	return plan, nil
}
