package api

import (
	"fmt"
	"strings"
	"sync"
)

type StudioStream struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	URL    string `json:"url"`
	Key    string `json:"key"`
	Action string `json:"action"`
	Kind   string `json:"kind"`
}

type StudioTimelineItem struct {
	ID           string  `json:"id"`
	StreamID     string  `json:"stream_id"`
	Label        string  `json:"label"`
	Kind         string  `json:"kind"`
	StartPercent float64 `json:"start_percent"`
	WidthPercent float64 `json:"width_percent"`
}

type StudioState struct {
	ChannelID        string               `json:"channel_id,omitempty"`
	Streams          []StudioStream       `json:"streams"`
	SelectedStreamID string               `json:"selected_stream_id"`
	StreamMode       string               `json:"stream_mode"`
	TimelineItems    []StudioTimelineItem `json:"timeline_items"`
}

type AddTimelineItemInput struct {
	StreamID     string  `json:"stream_id"`
	StartPercent float64 `json:"start_percent"`
}

type OpenStreamInput struct {
	StreamID string `json:"stream_id"`
}

type SelectStreamInput struct {
	StreamID string `json:"stream_id"`
}

type StudioService struct {
	channelID string
	mu        sync.RWMutex
	streams   []StudioStream
	state     StudioState
	nextID    int
}

func NewStudioService(channelID string) *StudioService {
	channelID = normalizeChannelID(channelID)
	streams := []StudioStream{
		{ID: "screen", Title: "Stream screen", URL: fmt.Sprintf("rtmp://localhost:1938/%s/screen", channelID), Key: fmt.Sprintf("%s-scrn", channelID), Action: "screen", Kind: "screen"},
		{ID: "camera", Title: "Stream camera", URL: fmt.Sprintf("rtmp://localhost:1938/%s/camera", channelID), Key: fmt.Sprintf("%s-camr", channelID), Action: "camera", Kind: "camera"},
		{ID: "guest", Title: "Guest feed", URL: fmt.Sprintf("rtmp://localhost:1938/%s/guest", channelID), Key: fmt.Sprintf("%s-gues", channelID), Action: "camera", Kind: "camera"},
		{ID: "slides", Title: "Slides input", URL: fmt.Sprintf("rtmp://localhost:1938/%s/slides", channelID), Key: fmt.Sprintf("%s-slds", channelID), Action: "screen", Kind: "screen"},
		{ID: "backup", Title: "Backup scene", URL: fmt.Sprintf("rtmp://localhost:1938/%s/backup", channelID), Key: fmt.Sprintf("%s-bkup", channelID), Action: "screen", Kind: "screen"},
	}

	return &StudioService{
		channelID: channelID,
		streams:   streams,
		state: StudioState{
			ChannelID:        channelID,
			Streams:          append([]StudioStream(nil), streams...),
			SelectedStreamID: "camera",
			StreamMode:       "idle",
			TimelineItems:    []StudioTimelineItem{},
		},
		nextID: 1,
	}
}

type StudioRegistry struct {
	mu      sync.Mutex
	studios map[string]*StudioService
}

func NewStudioRegistry() *StudioRegistry {
	return &StudioRegistry{
		studios: map[string]*StudioService{},
	}
}

func (r *StudioRegistry) ForChannel(channelID string) *StudioService {
	normalized := normalizeChannelID(channelID)

	r.mu.Lock()
	defer r.mu.Unlock()

	if studio, ok := r.studios[normalized]; ok {
		return studio
	}

	studio := NewStudioService(normalized)
	r.studios[normalized] = studio
	return studio
}

func (s *StudioService) State() StudioState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneStudioState(s.state)
}

func (s *StudioService) SelectStream(streamID string) (StudioState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.findStreamLocked(streamID); !ok {
		return StudioState{}, fmt.Errorf("stream %q not found", streamID)
	}
	s.state.SelectedStreamID = streamID
	return cloneStudioState(s.state), nil
}

func (s *StudioService) OpenStream(streamID string) (StudioState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.findStreamLocked(streamID)
	if !ok {
		return StudioState{}, fmt.Errorf("stream %q not found", streamID)
	}
	s.state.SelectedStreamID = streamID
	s.state.StreamMode = stream.Action
	return cloneStudioState(s.state), nil
}

func (s *StudioService) StopBroadcast() StudioState {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.StreamMode = "idle"
	return cloneStudioState(s.state)
}

func (s *StudioService) AddTimelineItem(input AddTimelineItemInput) (StudioState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.findStreamLocked(input.StreamID)
	if !ok {
		return StudioState{}, fmt.Errorf("stream %q not found", input.StreamID)
	}

	startPercent := clampPercent(input.StartPercent)
	widthPercent := 10.0
	if startPercent > 100-widthPercent {
		startPercent = 100 - widthPercent
	}

	item := StudioTimelineItem{
		ID:           fmt.Sprintf("timeline-%d", s.nextID),
		StreamID:     stream.ID,
		Label:        stream.Title,
		Kind:         stream.Kind,
		StartPercent: startPercent,
		WidthPercent: widthPercent,
	}
	s.nextID++
	s.state.SelectedStreamID = stream.ID
	s.state.TimelineItems = append(s.state.TimelineItems, item)
	return cloneStudioState(s.state), nil
}

func (s *StudioService) findStreamLocked(streamID string) (StudioStream, bool) {
	for _, stream := range s.streams {
		if stream.ID == streamID {
			return stream, true
		}
	}
	return StudioStream{}, false
}

func cloneStudioState(state StudioState) StudioState {
	cloned := state
	cloned.Streams = append([]StudioStream{}, state.Streams...)
	cloned.TimelineItems = append([]StudioTimelineItem{}, state.TimelineItems...)
	return cloned
}

func clampPercent(value float64) float64 {
	switch {
	case value < 0:
		return 0
	case value > 100:
		return 100
	default:
		return value
	}
}

func normalizeChannelID(channelID string) string {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return "default"
	}
	return channelID
}
