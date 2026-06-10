package manifest

import (
	"fmt"
	"sort"
	"strings"
)

func validateV1(m Manifest) error {
	if len(m.Scenes) == 0 {
		return fmt.Errorf("manifest.scenes must contain at least one scene")
	}
	if len(m.Scenes) != 1 {
		return fmt.Errorf("manifest.scenes must contain exactly one scene in version 1.0")
	}
	if strings.TrimSpace(m.ActiveSceneID) != "" {
		return fmt.Errorf("manifest.active_scene_id is not supported in version 1.0")
	}
	if m.OutputSettings != nil {
		return fmt.Errorf("manifest.output_settings is not supported in version 1.0")
	}
	if len(m.Lives) > 0 {
		return fmt.Errorf("manifest.lives is not supported in version 1.0")
	}
	if len(m.Records) > 0 {
		return fmt.Errorf("manifest.records is not supported in version 1.0")
	}

	sceneIDs := make(map[string]struct{}, len(m.Scenes))
	for sceneIdx, scene := range m.Scenes {
		sceneID := strings.TrimSpace(scene.ID)
		if sceneID == "" {
			return fmt.Errorf("manifest.scenes[%d].id is required", sceneIdx)
		}
		if _, exists := sceneIDs[sceneID]; exists {
			return fmt.Errorf("manifest.scenes[%d].id %q is duplicated", sceneIdx, sceneID)
		}
		sceneIDs[sceneID] = struct{}{}
		if len(scene.AudioElements) > 0 {
			return fmt.Errorf("manifest.scenes[%d].audio_elements is not supported in version 1.0", sceneIdx)
		}
		if len(scene.Slots) == 0 {
			return fmt.Errorf("manifest.scenes[%d].slots must contain at least one slot", sceneIdx)
		}
		if len(scene.Slots) != 1 {
			return fmt.Errorf("manifest.scenes[%d].slots must contain exactly one slot in version 1.0", sceneIdx)
		}

		slotIDs := map[string]struct{}{}
		for slotIdx, slot := range scene.Slots {
			if slot.Details != (SlotDetails{}) {
				return fmt.Errorf("manifest.scenes[%d].slots[%d].details is not supported in version 1.0", sceneIdx, slotIdx)
			}
			slotID := strings.TrimSpace(slot.ID)
			if slotID != "" {
				if _, exists := slotIDs[slotID]; exists {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].slot_id %q is duplicated", sceneIdx, slotIdx, slotID)
				}
				slotIDs[slotID] = struct{}{}
			}
			if len(slot.Elements) == 0 {
				return fmt.Errorf("manifest.scenes[%d].slots[%d].elements must contain at least one element", sceneIdx, slotIdx)
			}
			elementIDs := map[string]struct{}{}
			sorted := append([]Element(nil), slot.Elements...)
			sort.SliceStable(sorted, func(i, j int) bool {
				return sorted[i].StartsAt < sorted[j].StartsAt
			})
			for elementIdx, element := range slot.Elements {
				elementID := strings.TrimSpace(element.ID)
				if elementID != "" {
					if _, exists := elementIDs[elementID]; exists {
						return fmt.Errorf("manifest.scenes[%d].slots[%d].elements[%d].id %q is duplicated", sceneIdx, slotIdx, elementIdx, elementID)
					}
					elementIDs[elementID] = struct{}{}
				}
				if strings.TrimSpace(element.URL) == "" {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements[%d].url is required", sceneIdx, slotIdx, elementIdx)
				}
				if element.SourceType != "" && element.SourceType != SourceTypeLink {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements[%d].source_type %q is not supported in version 1.0", sceneIdx, slotIdx, elementIdx, element.SourceType)
				}
				if element.AssetTrim != nil {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements[%d].asset_trim is not supported in version 1.0", sceneIdx, slotIdx, elementIdx)
				}
				if len(element.Filters) > 0 {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements[%d].filters is not supported in version 1.0", sceneIdx, slotIdx, elementIdx)
				}
				if element.StartsAt <= 0 {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements[%d].starts_at must be greater than zero", sceneIdx, slotIdx, elementIdx)
				}
				if element.FinishesAt != -1 && element.FinishesAt <= element.StartsAt {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements[%d].finishes_at must be -1 or greater than starts_at", sceneIdx, slotIdx, elementIdx)
				}
			}
			for idx := range sorted {
				if idx == 0 {
					continue
				}
				prev := sorted[idx-1]
				cur := sorted[idx]
				if prev.FinishesAt == -1 {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements must not contain elements after an open-ended element in version 1.0", sceneIdx, slotIdx)
				}
				if prev.FinishesAt != cur.StartsAt {
					return fmt.Errorf("manifest.scenes[%d].slots[%d].elements must form a contiguous non-overlapping timeline in version 1.0", sceneIdx, slotIdx)
				}
			}
		}
	}

	return nil
}
