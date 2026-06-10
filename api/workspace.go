package api

import (
	"fmt"
	"sync"

	irajstreamer "github.com/tupicapp/restreamer/core"
	manifestpkg "github.com/tupicapp/restreamer/core/manifest"
)

type WorkspaceState struct {
	ChannelID string                `json:"channel_id"`
	Studio    StudioState           `json:"studio"`
	Manifest  *manifestpkg.Manifest `json:"manifest,omitempty"`
	Runner    *WorkspaceRunnerState `json:"runner,omitempty"`
}

type WorkspaceRunnerState struct {
	IsStarted       bool                       `json:"is_started"`
	ManifestVersion string                     `json:"manifest_version,omitempty"`
	PlanKind        string                     `json:"plan_kind,omitempty"`
	ActiveSceneID   string                     `json:"active_scene_id,omitempty"`
	StreamerState   irajstreamer.StreamerState `json:"streamer_state"`
	CurrentInputID  string                     `json:"current_input_id,omitempty"`
	CurrentInputURL string                     `json:"current_input_url,omitempty"`
	PreviewURL      string                     `json:"preview_url,omitempty"`
	Outputs         []WorkspaceOutputState     `json:"outputs,omitempty"`
}

type WorkspaceOutputState struct {
	ID        string `json:"id,omitempty"`
	URL       string `json:"url,omitempty"`
	LocalPath string `json:"local_path,omitempty"`
	ServeType string `json:"serve_type,omitempty"`
	ServeMode string `json:"serve_mode,omitempty"`
}

type WorkspaceService struct {
	mu        sync.RWMutex
	manifests map[string]manifestpkg.Manifest
	studios   *StudioRegistry
	runners   *RunnerService
}

func NewWorkspaceService(studios *StudioRegistry, runners *RunnerService) *WorkspaceService {
	return &WorkspaceService{
		manifests: map[string]manifestpkg.Manifest{},
		studios:   studios,
		runners:   runners,
	}
}

func (s *WorkspaceService) State(channelID string) WorkspaceState {
	channelID = normalizeChannelID(channelID)

	s.mu.RLock()
	manifest, ok := s.manifests[channelID]
	s.mu.RUnlock()

	state := WorkspaceState{
		ChannelID: channelID,
		Studio:    s.studios.ForChannel(channelID).State(),
	}
	if ok {
		cloned := cloneManifest(manifest)
		state.Manifest = &cloned
	}
	if runner, ok := s.runners.Get(channelID); ok {
		runnerState := runner.State()
		state.Runner = &WorkspaceRunnerState{
			IsStarted:       runnerState.IsStarted,
			ManifestVersion: runnerState.ManifestVersion,
			PlanKind:        string(runnerState.PlanKind),
			ActiveSceneID:   runnerState.ActiveSceneID,
			StreamerState:   cloneWorkspaceStreamerState(runnerState.StreamerState),
			CurrentInputID:  runnerState.CurrentInputID,
			CurrentInputURL: runnerState.CurrentInputURL,
			PreviewURL:      runnerState.PreviewURL,
			Outputs:         cloneRunnerOutputs(runnerState.Outputs),
		}
	}
	return state
}

func (s *WorkspaceService) UpdateManifest(channelID string, manifest manifestpkg.Manifest) (WorkspaceState, error) {
	channelID = normalizeChannelID(channelID)
	if manifest.ChannelID == "" {
		manifest.ChannelID = channelID
	}
	if manifest.ChannelID != channelID {
		return WorkspaceState{}, fmt.Errorf("manifest.channel_id %q does not match channel %q", manifest.ChannelID, channelID)
	}

	runner, err := s.runners.Apply(channelID, manifest)
	if err != nil {
		return WorkspaceState{}, err
	}

	s.mu.Lock()
	s.manifests[channelID] = cloneManifest(manifest)
	s.mu.Unlock()

	state := s.State(channelID)
	if runner != nil {
		runnerState := runner.State()
		state.Runner = &WorkspaceRunnerState{
			IsStarted:       runnerState.IsStarted,
			ManifestVersion: runnerState.ManifestVersion,
			PlanKind:        string(runnerState.PlanKind),
			ActiveSceneID:   runnerState.ActiveSceneID,
			StreamerState:   cloneWorkspaceStreamerState(runnerState.StreamerState),
			CurrentInputID:  runnerState.CurrentInputID,
			CurrentInputURL: runnerState.CurrentInputURL,
			PreviewURL:      runnerState.PreviewURL,
			Outputs:         cloneRunnerOutputs(runnerState.Outputs),
		}
	}
	return state, nil
}

func cloneRunnerOutputs(outputs []irajstreamer.RunnerOutput) []WorkspaceOutputState {
	if len(outputs) == 0 {
		return []WorkspaceOutputState{}
	}
	cloned := make([]WorkspaceOutputState, 0, len(outputs))
	for _, output := range outputs {
		cloned = append(cloned, WorkspaceOutputState{
			ID:        output.ID,
			URL:       output.URL,
			LocalPath: output.LocalPath,
			ServeType: output.ServeType,
			ServeMode: output.ServeMode,
		})
	}
	return cloned
}

func cloneWorkspaceStreamerState(state irajstreamer.StreamerState) irajstreamer.StreamerState {
	cloned := state
	cloned.StreamInputs = append([]*irajstreamer.State(nil), state.StreamInputs...)
	cloned.StreamOutputs = append([]*irajstreamer.State(nil), state.StreamOutputs...)
	return cloned
}

func cloneManifest(in manifestpkg.Manifest) manifestpkg.Manifest {
	out := in
	out.Lives = append([]manifestpkg.BranchRef(nil), in.Lives...)
	out.Records = append([]manifestpkg.BranchRef(nil), in.Records...)
	out.Scenes = make([]manifestpkg.Scene, 0, len(in.Scenes))
	for _, scene := range in.Scenes {
		clonedScene := scene
		clonedScene.Details = append([]any(nil), scene.Details...)
		clonedScene.Slots = make([]manifestpkg.Slot, 0, len(scene.Slots))
		for _, slot := range scene.Slots {
			clonedSlot := slot
			clonedSlot.Elements = make([]manifestpkg.Element, 0, len(slot.Elements))
			for _, element := range slot.Elements {
				clonedElement := element
				if element.Details != nil {
					clonedElement.Details = map[string]any{}
					for key, value := range element.Details {
						clonedElement.Details[key] = value
					}
				}
				clonedElement.Filters = append([]string(nil), element.Filters...)
				if element.AssetTrim != nil {
					trim := *element.AssetTrim
					clonedElement.AssetTrim = &trim
				}
				clonedSlot.Elements = append(clonedSlot.Elements, clonedElement)
			}
			clonedScene.Slots = append(clonedScene.Slots, clonedSlot)
		}
		clonedScene.AudioElements = make([][]manifestpkg.AudioElement, 0, len(scene.AudioElements))
		for _, track := range scene.AudioElements {
			clonedTrack := make([]manifestpkg.AudioElement, 0, len(track))
			for _, audio := range track {
				clonedAudio := audio
				if audio.Details != nil {
					clonedAudio.Details = map[string]any{}
					for key, value := range audio.Details {
						clonedAudio.Details[key] = value
					}
				}
				clonedTrack = append(clonedTrack, clonedAudio)
			}
			clonedScene.AudioElements = append(clonedScene.AudioElements, clonedTrack)
		}
		out.Scenes = append(out.Scenes, clonedScene)
	}
	return out
}
