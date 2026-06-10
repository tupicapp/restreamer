package timeline

import (
	"sort"
	"time"
)

type PlanKind string

const (
	PlanKindTimeline PlanKind = "timeline"
)

type EventKind string

const (
	EventKindActivateScene     EventKind = "activate_scene"
	EventKindActivateElement   EventKind = "activate_element"
	EventKindDeactivateElement EventKind = "deactivate_element"
)

type Plan struct {
	Kind            PlanKind
	ManifestVersion string
	InitialSceneID  string
	Scenes          []ScenePlan
	Events          []Event
}

type ScenePlan struct {
	ID    string
	Slots []SlotPlan
}

type SlotPlan struct {
	ID       string
	Elements []ElementPlan
}

type ElementPlan struct {
	ID         string
	SourceID   string
	URL        string
	StartsAt   time.Time
	FinishesAt *time.Time
}

type Event struct {
	At        time.Time
	Kind      EventKind
	SceneID   string
	SlotID    string
	ElementID string
}

type ActiveState struct {
	SceneID        string
	ActiveElements map[string]string
}

func (p Plan) NormalizedEvents() []Event {
	events := append([]Event(nil), p.Events...)
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].At.Equal(events[j].At) {
			return eventPriority(events[i].Kind) < eventPriority(events[j].Kind)
		}
		return events[i].At.Before(events[j].At)
	})
	return events
}

func eventPriority(kind EventKind) int {
	switch kind {
	case EventKindActivateScene:
		return 0
	case EventKindActivateElement:
		return 1
	case EventKindDeactivateElement:
		return 2
	default:
		return 100
	}
}

func (p Plan) ActiveStateAt(now time.Time) ActiveState {
	state := ActiveState{
		SceneID:        p.InitialSceneID,
		ActiveElements: map[string]string{},
	}
	for _, event := range p.NormalizedEvents() {
		if event.At.After(now) {
			break
		}
		switch event.Kind {
		case EventKindActivateScene:
			state.SceneID = event.SceneID
		case EventKindActivateElement:
			state.ActiveElements[event.SlotID] = event.ElementID
		case EventKindDeactivateElement:
			if state.ActiveElements[event.SlotID] == event.ElementID {
				delete(state.ActiveElements, event.SlotID)
			}
		}
	}
	return state
}

func (p Plan) NextEventAfter(now time.Time) *Event {
	for _, event := range p.NormalizedEvents() {
		if event.At.After(now) {
			next := event
			return &next
		}
	}
	return nil
}
