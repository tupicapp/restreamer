package irajstreamer

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tupicapp/restreamer/core/inputs"
	manifestpkg "github.com/tupicapp/restreamer/core/manifest"
	"github.com/tupicapp/restreamer/core/outputs"
	shared "github.com/tupicapp/restreamer/core/shared"
	"github.com/tupicapp/restreamer/core/storage"
	"github.com/tupicapp/restreamer/core/timeline"
)

type ManifestRunner interface {
	UpdateManifest(manifestpkg.Manifest) error
	Start() error
	Stop() error
	Close() error
	State() ManifestRunnerState
	Events() <-chan Event
}

type ManifestRunnerState struct {
	IsStarted       bool              `json:"is_started"`
	ManifestVersion string            `json:"manifest_version"`
	PlanKind        timeline.PlanKind `json:"plan_kind"`
	ActiveSceneID   string            `json:"active_scene_id,omitempty"`
	ActiveElements  map[string]string `json:"active_elements,omitempty"`
	StreamerState   StreamerState     `json:"streamer_state"`
	CurrentInputID  string            `json:"current_input_id,omitempty"`
	CurrentInputURL string            `json:"current_input_url,omitempty"`
	PreviewURL      string            `json:"preview_url,omitempty"`
	Outputs         []RunnerOutput    `json:"outputs,omitempty"`
	NextEventAt     *time.Time        `json:"next_event_at,omitempty"`
	LastEventAt     *time.Time        `json:"last_event_at,omitempty"`
	UpdatedAt       time.Time         `json:"updated_at,omitempty"`
}

type RunnerOutput struct {
	ID        string `json:"id,omitempty"`
	URL       string `json:"url,omitempty"`
	LocalPath string `json:"local_path,omitempty"`
	ServeType string `json:"serve_type,omitempty"`
	ServeMode string `json:"serve_mode,omitempty"`
}

func NewManifestRunner(manifest manifestpkg.Manifest) (ManifestRunner, error) {
	plan, err := manifestpkg.CompileManifest(manifest)
	if err != nil {
		return nil, err
	}
	switch plan.Kind {
	case timeline.PlanKindTimeline:
		return newTimeLinedStreamer(manifest, plan)
	default:
		return nil, fmt.Errorf("manifest plan kind %q is not supported", plan.Kind)
	}
}

type TimeLinedStreamer struct {
	mu sync.RWMutex

	base *Streamer

	manifest       manifestpkg.Manifest
	plan           timeline.Plan
	scheduler      *timeline.Scheduler
	runtimeBuilt   bool
	runtimeInputs  []Stream
	runtimeOutputs []Stream

	events *shared.EventEmitter
	state  ManifestRunnerState

	closeOnce     sync.Once
	lifeStartOnce sync.Once
	closed        bool
}

func newTimeLinedStreamer(manifest manifestpkg.Manifest, plan timeline.Plan) (*TimeLinedStreamer, error) {
	now := time.Now().UTC()
	active := plan.ActiveStateAt(now)
	state := ManifestRunnerState{
		ManifestVersion: plan.ManifestVersion,
		PlanKind:        plan.Kind,
		ActiveSceneID:   active.SceneID,
		ActiveElements:  cloneActiveElements(active.ActiveElements),
		UpdatedAt:       now,
	}
	if next := plan.NextEventAfter(now); next != nil {
		nextAt := next.At
		state.NextEventAt = &nextAt
	}

	return &TimeLinedStreamer{
		base:     NewStreamer(WithStreamerID("timeline-runner")),
		manifest: manifest,
		plan:     plan,
		events:   shared.NewEventEmitter(128),
		state:    state,
	}, nil
}

func (t *TimeLinedStreamer) UpdateManifest(manifest manifestpkg.Manifest) error {
	plan, err := manifestpkg.CompileManifest(manifest)
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("timeline streamer is closed")
	}

	t.manifest = manifest
	t.plan = plan
	if t.state.IsStarted {
		if err := t.rebuildRuntimeLocked(time.Now().UTC()); err != nil {
			return err
		}
		if err := t.ensureBootstrapOutputsLocked(); err != nil {
			return err
		}
	}
	t.refreshStateLocked(time.Now().UTC())
	if t.scheduler != nil {
		t.scheduler.ReplacePlan(plan)
	}
	t.events.Emit(shared.Event{
		Type:       shared.EventTypeManifestUpdated,
		StreamID:   t.base.streamerIDOrDefault(),
		StreamType: "timeline_streamer",
		Message:    "manifest updated",
		Meta: map[string]any{
			"manifest_version": manifest.Version,
			"plan_kind":        plan.Kind,
		},
	})
	return nil
}

func (t *TimeLinedStreamer) Start() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("timeline streamer is closed")
	}
	if t.state.IsStarted {
		return nil
	}

	t.lifeStartOnce.Do(func() {
		t.base.StartLife()
	})
	if err := t.rebuildRuntimeLocked(time.Now().UTC()); err != nil {
		return err
	}
	t.base.Start()
	if err := t.ensureBootstrapOutputsLocked(); err != nil {
		return err
	}

	t.scheduler = timeline.NewScheduler()
	t.scheduler.Start(t.applyEvent)
	t.scheduler.ReplacePlan(t.plan)

	t.state.IsStarted = true
	t.refreshStateLocked(time.Now().UTC())
	t.events.Emit(shared.Event{
		Type:       shared.EventTypeStreamStarted,
		StreamID:   t.base.streamerIDOrDefault(),
		StreamType: "timeline_streamer",
		Message:    "timeline streamer started",
	})
	return nil
}

func (t *TimeLinedStreamer) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("timeline streamer is closed")
	}
	if !t.state.IsStarted {
		return nil
	}

	if t.scheduler != nil {
		t.scheduler.Stop()
		t.scheduler = nil
	}
	if err := t.teardownRuntimeLocked(); err != nil {
		return err
	}
	t.base.Stop()
	t.state.IsStarted = false
	t.refreshStateLocked(time.Now().UTC())
	t.events.Emit(shared.Event{
		Type:       shared.EventTypeStreamStopped,
		StreamID:   t.base.streamerIDOrDefault(),
		StreamType: "timeline_streamer",
		Message:    "timeline streamer stopped",
	})
	return nil
}

func (t *TimeLinedStreamer) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.scheduler != nil {
			t.scheduler.Stop()
			t.scheduler = nil
		}
		_ = t.teardownRuntimeLocked()
		t.base.Close()
		t.closed = true
		t.state.IsStarted = false
		t.events.Emit(shared.Event{
			Type:       shared.EventTypeStreamClosed,
			StreamID:   t.base.streamerIDOrDefault(),
			StreamType: "timeline_streamer",
			Message:    "timeline streamer closed",
		})
		t.events.Close()
	})
	return nil
}

func (t *TimeLinedStreamer) State() ManifestRunnerState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	state := t.state
	state.ActiveElements = cloneActiveElements(state.ActiveElements)
	state.StreamerState = cloneStreamerState(state.StreamerState)
	state.Outputs = append([]RunnerOutput(nil), state.Outputs...)
	if state.NextEventAt != nil {
		next := *state.NextEventAt
		state.NextEventAt = &next
	}
	if state.LastEventAt != nil {
		last := *state.LastEventAt
		state.LastEventAt = &last
	}
	return state
}

func (t *TimeLinedStreamer) Events() <-chan Event {
	if t.events == nil {
		return nil
	}
	return t.events.Chan()
}

func (t *TimeLinedStreamer) applyEvent(event timeline.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}

	switch event.Kind {
	case timeline.EventKindActivateScene:
		t.state.ActiveSceneID = event.SceneID
	case timeline.EventKindActivateElement:
		if ok := t.base.Switch(event.ElementID); !ok {
			t.events.Emit(shared.Event{
				Type:       shared.EventTypeStreamError,
				StreamID:   t.base.streamerIDOrDefault(),
				StreamType: "timeline_streamer",
				Message:    "failed to switch timeline input",
				Meta: map[string]any{
					"scene_id":   event.SceneID,
					"slot_id":    event.SlotID,
					"element_id": event.ElementID,
				},
			})
			return
		}
		if t.state.ActiveElements == nil {
			t.state.ActiveElements = map[string]string{}
		}
		t.state.ActiveElements[event.SlotID] = event.ElementID
	case timeline.EventKindDeactivateElement:
		if t.state.ActiveElements[event.SlotID] == event.ElementID {
			delete(t.state.ActiveElements, event.SlotID)
		}
	}

	t.syncRuntimeStateLocked()
	eventAt := event.At
	t.state.LastEventAt = &eventAt
	if next := t.plan.NextEventAfter(eventAt); next != nil {
		nextAt := next.At
		t.state.NextEventAt = &nextAt
	} else {
		t.state.NextEventAt = nil
	}
	t.state.UpdatedAt = time.Now().UTC()

	t.events.Emit(shared.Event{
		Type:       shared.EventTypeTimelineEventApplied,
		StreamID:   t.base.streamerIDOrDefault(),
		StreamType: "timeline_streamer",
		Message:    string(event.Kind),
		Meta: map[string]any{
			"scene_id":   event.SceneID,
			"slot_id":    event.SlotID,
			"element_id": event.ElementID,
			"at":         event.At,
		},
	})
}

func (t *TimeLinedStreamer) refreshStateLocked(now time.Time) {
	active := t.plan.ActiveStateAt(now)
	t.state.ManifestVersion = t.plan.ManifestVersion
	t.state.PlanKind = t.plan.Kind
	t.state.ActiveSceneID = active.SceneID
	t.state.ActiveElements = cloneActiveElements(active.ActiveElements)
	t.state.UpdatedAt = now
	t.syncRuntimeStateLocked()
	if next := t.plan.NextEventAfter(now); next != nil {
		nextAt := next.At
		t.state.NextEventAt = &nextAt
	} else {
		t.state.NextEventAt = nil
	}
}

func (t *TimeLinedStreamer) syncRuntimeStateLocked() {
	streamerState := t.base.State()
	t.state.StreamerState = cloneStreamerState(streamerState)
	t.state.CurrentInputID = streamerState.CurrentInputID
	t.state.CurrentInputURL = ""
	t.state.PreviewURL = ""
	t.state.Outputs = t.state.Outputs[:0]

	for _, input := range streamerState.StreamInputs {
		if input == nil || input.StreamID != streamerState.CurrentInputID {
			continue
		}
		t.state.CurrentInputURL = input.Url
		break
	}

	for _, output := range streamerState.StreamOutputs {
		if output == nil {
			continue
		}
		item := RunnerOutput{
			ID:        output.StreamID,
			URL:       output.Url,
			LocalPath: output.LocalPath,
			ServeType: output.ServeType,
			ServeMode: output.ServeMode,
		}
		t.state.Outputs = append(t.state.Outputs, item)
		if t.state.PreviewURL == "" {
			t.state.PreviewURL = firstPreviewURLFromState(output)
		}
	}

	if t.state.PreviewURL == "" {
		for _, input := range streamerState.StreamInputs {
			if input == nil || input.StreamID != streamerState.CurrentInputID {
				continue
			}
			t.state.PreviewURL = firstPreviewURLFromState(input)
			break
		}
	}
	if t.state.PreviewURL == "" {
		t.state.PreviewURL = t.state.CurrentInputURL
	}
}

func firstPreviewURLFromState(state *State) string {
	if state == nil {
		return ""
	}
	for _, served := range state.Served {
		if strings.TrimSpace(served.Url) != "" {
			return served.Url
		}
	}
	return strings.TrimSpace(state.Url)
}

func cloneActiveElements(src map[string]string) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneStreamerState(src StreamerState) StreamerState {
	dst := src
	dst.StreamInputs = append([]*State(nil), src.StreamInputs...)
	dst.StreamOutputs = append([]*State(nil), src.StreamOutputs...)
	return dst
}

func DefaultTimelineHLSRoot() string {
	return filepath.Join(os.TempDir(), "irajstreamer", "timeline-hls")
}

func (t *TimeLinedStreamer) rebuildRuntimeLocked(now time.Time) error {
	inputs, err := t.buildInputsForPlanLocked()
	if err != nil {
		return err
	}
	outputs, err := t.buildOutputsForPlanLocked(len(inputs) > 0)
	if err != nil {
		for _, input := range inputs {
			if input != nil {
				input.Close()
			}
		}
		return err
	}
	if err := t.base.UpdateStreams(inputs, outputs); err != nil {
		for _, input := range inputs {
			if input != nil {
				input.Close()
			}
		}
		for _, output := range outputs {
			if output != nil {
				output.Close()
			}
		}
		return err
	}
	t.runtimeInputs = inputs
	t.runtimeOutputs = outputs
	t.runtimeBuilt = true

	active := t.plan.ActiveStateAt(now)
	if activeID := active.ActiveElements[firstSlotID(t.plan)]; activeID != "" {
		if ok := t.base.Switch(activeID); !ok {
			return fmt.Errorf("failed to activate current element %q", activeID)
		}
	}
	return nil
}

func (t *TimeLinedStreamer) buildInputsForPlanLocked() ([]Stream, error) {
	if len(t.plan.Scenes) != 1 || len(t.plan.Scenes[0].Slots) != 1 {
		return nil, fmt.Errorf("timeline runtime currently requires exactly one scene and one slot")
	}
	slot := t.plan.Scenes[0].Slots[0]
	inputs := make([]Stream, 0, len(slot.Elements))
	for _, element := range slot.Elements {
		input, err := newTimelineInput(element.ID, element.URL)
		if err != nil {
			for _, created := range inputs {
				if created != nil {
					created.Close()
				}
			}
			return nil, fmt.Errorf("build input %q: %w", element.ID, err)
		}
		inputs = append(inputs, input)
	}
	return inputs, nil
}

func (t *TimeLinedStreamer) buildOutputsForPlanLocked(hasInputs bool) ([]Stream, error) {
	if !hasInputs {
		return nil, nil
	}

	channelID := normalizeTimelineChannelID(t.manifest.ChannelID)
	outputDir := filepath.Join(DefaultTimelineHLSRoot(), "channels", sanitizeTimelinePathComponent(channelID))
	if err := os.RemoveAll(outputDir); err != nil {
		return nil, fmt.Errorf("reset timeline output dir: %w", err)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("bootstrap channel output dir: %w", err)
	}
	output, err := outputs.NewHLSLiveDestination(
		channelID+"-channel-out",
		storage.NewFolder(outputDir, storage.WithPublicBaseURL(timelineOutputPublicBaseURL(channelID))),
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(2*time.Second),
		outputs.WithHLSPlaylistSize(6),
		outputs.WithHLSCleanInterval(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("build channel output: %w", err)
	}
	return []Stream{output}, nil
}

func (t *TimeLinedStreamer) teardownRuntimeLocked() error {
	if !t.runtimeBuilt {
		return nil
	}
	if err := t.base.UpdateStreams(nil, nil); err != nil {
		return err
	}
	t.runtimeInputs = nil
	t.runtimeOutputs = nil
	t.runtimeBuilt = false
	return nil
}

func (t *TimeLinedStreamer) ensureBootstrapOutputsLocked() error {
	for _, output := range t.runtimeOutputs {
		if output == nil || output.State() == nil {
			continue
		}
		state := output.State()
		if strings.TrimSpace(state.ServeType) != "hls" || strings.TrimSpace(state.LocalPath) == "" {
			continue
		}
		if err := bootstrapTimelineHLSOutputDir(state.LocalPath); err != nil {
			return err
		}
	}
	return nil
}

func firstSlotID(plan timeline.Plan) string {
	if len(plan.Scenes) == 0 || len(plan.Scenes[0].Slots) == 0 {
		return ""
	}
	return plan.Scenes[0].Slots[0].ID
}

func newTimelineInput(id, streamURL string) (Stream, error) {
	switch detectTimelineInputKind(streamURL) {
	case timelineInputKindRTMP:
		return inputs.NewCompatibleInput(inputs.NewRTMP(id, streamURL), inputs.WithCompatRuntimeDetection(false)), nil
	case timelineInputKindHLS:
		return inputs.NewHLSAuto(id, streamURL, nil, inputs.OptionWithRealTime(true))
	default:
		return inputs.NewCompatibleInput(inputs.NewRTMP(id, streamURL), inputs.WithCompatRuntimeDetection(false)), nil
	}
}

func normalizeTimelineChannelID(channelID string) string {
	if strings.TrimSpace(channelID) == "" {
		return "default"
	}
	return strings.TrimSpace(channelID)
}

func sanitizeTimelinePathComponent(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", "..", "_", ":", "_")
	return replacer.Replace(strings.TrimSpace(value))
}

func timelineOutputPublicBaseURL(channelID string) string {
	return "/hls/channels/" + url.PathEscape(channelID)
}

func bootstrapTimelineHLSOutputDir(outputDir string) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	const bootstrapPlaylist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n"
	return os.WriteFile(filepath.Join(outputDir, "stream.m3u8"), []byte(bootstrapPlaylist), 0o644)
}

type timelineInputKind string

const (
	timelineInputKindRTMP timelineInputKind = "rtmp"
	timelineInputKindHLS  timelineInputKind = "hls"
)

func detectTimelineInputKind(streamURL string) timelineInputKind {
	lowerURL := strings.ToLower(strings.TrimSpace(streamURL))
	switch {
	case strings.HasPrefix(lowerURL, "rtmp://"):
		return timelineInputKindRTMP
	case strings.HasPrefix(lowerURL, "http://"), strings.HasPrefix(lowerURL, "https://"), strings.HasSuffix(lowerURL, ".m3u8"):
		return timelineInputKindHLS
	default:
		return timelineInputKindRTMP
	}
}

var _ ManifestRunner = (*TimeLinedStreamer)(nil)
