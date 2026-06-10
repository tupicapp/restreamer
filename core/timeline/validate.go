package timeline

import (
	"fmt"
	"strings"
	"time"
)

func ValidatePlan(plan Plan) error {
	if plan.Kind != PlanKindTimeline {
		return fmt.Errorf("timeline plan kind %q is not supported", plan.Kind)
	}
	if strings.TrimSpace(plan.ManifestVersion) == "" {
		return fmt.Errorf("timeline plan manifest version is required")
	}
	if len(plan.Scenes) == 0 {
		return fmt.Errorf("timeline plan requires at least one scene")
	}

	sceneIDs := map[string]struct{}{}
	slotRefs := map[string]struct{}{}
	elementRefs := map[string]struct{}{}

	for sceneIdx, scene := range plan.Scenes {
		sceneID := strings.TrimSpace(scene.ID)
		if sceneID == "" {
			return fmt.Errorf("timeline plan scenes[%d].id is required", sceneIdx)
		}
		if _, exists := sceneIDs[sceneID]; exists {
			return fmt.Errorf("timeline plan scene %q is duplicated", sceneID)
		}
		sceneIDs[sceneID] = struct{}{}

		slotIDs := map[string]struct{}{}
		for slotIdx, slot := range scene.Slots {
			slotID := strings.TrimSpace(slot.ID)
			if slotID == "" {
				return fmt.Errorf("timeline plan scenes[%d].slots[%d].id is required", sceneIdx, slotIdx)
			}
			if _, exists := slotIDs[slotID]; exists {
				return fmt.Errorf("timeline plan scene %q slot %q is duplicated", sceneID, slotID)
			}
			slotIDs[slotID] = struct{}{}
			slotRefs[sceneID+"::"+slotID] = struct{}{}

			elementIDs := map[string]struct{}{}
			for elementIdx, element := range slot.Elements {
				elementID := strings.TrimSpace(element.ID)
				if elementID == "" {
					return fmt.Errorf("timeline plan scenes[%d].slots[%d].elements[%d].id is required", sceneIdx, slotIdx, elementIdx)
				}
				if _, exists := elementIDs[elementID]; exists {
					return fmt.Errorf("timeline plan scene %q slot %q element %q is duplicated", sceneID, slotID, elementID)
				}
				elementIDs[elementID] = struct{}{}
				elementRefs[sceneID+"::"+slotID+"::"+elementID] = struct{}{}
				if strings.TrimSpace(element.URL) == "" {
					return fmt.Errorf("timeline plan scene %q slot %q element %q url is required", sceneID, slotID, elementID)
				}
				if element.FinishesAt != nil && !element.FinishesAt.After(element.StartsAt) {
					return fmt.Errorf("timeline plan scene %q slot %q element %q finish must be after start", sceneID, slotID, elementID)
				}
			}
		}
	}

	if plan.InitialSceneID == "" {
		return fmt.Errorf("timeline plan initial scene id is required")
	}
	if _, exists := sceneIDs[plan.InitialSceneID]; !exists {
		return fmt.Errorf("timeline plan initial scene %q does not exist", plan.InitialSceneID)
	}

	events := plan.NormalizedEvents()
	var prev time.Time
	for idx, event := range events {
		if idx > 0 && event.At.Before(prev) {
			return fmt.Errorf("timeline plan events are not ordered")
		}
		prev = event.At
		switch event.Kind {
		case EventKindActivateScene:
			if _, exists := sceneIDs[event.SceneID]; !exists {
				return fmt.Errorf("timeline event references unknown scene %q", event.SceneID)
			}
		case EventKindActivateElement, EventKindDeactivateElement:
			if _, exists := slotRefs[event.SceneID+"::"+event.SlotID]; !exists {
				return fmt.Errorf("timeline event references unknown slot %q in scene %q", event.SlotID, event.SceneID)
			}
			if _, exists := elementRefs[event.SceneID+"::"+event.SlotID+"::"+event.ElementID]; !exists {
				return fmt.Errorf("timeline event references unknown element %q in slot %q", event.ElementID, event.SlotID)
			}
		default:
			return fmt.Errorf("timeline event kind %q is not supported", event.Kind)
		}
	}

	return nil
}
