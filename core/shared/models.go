package shared

import (
	"context"
	"sync"
	"time"
)

var sidecarPushTimeout = 10 * time.Millisecond

type StreamType string

type Stream interface {
	GetVideoChan() chan *Frame
	GetAudioChan() chan *Frame
	GetID() string
	Start()
	Stop()
	Close()
	State() *State
	Clone() (Stream, error)
	WaitForStart(ctx context.Context) error
	Type() string
	IsRestartable() bool
	RestartInterval() time.Duration
	EventChan() chan Event
}

type EventSource interface {
	EventChan() chan Event
}

type Event struct {
	Type       EventType `json:"type"`
	Time       time.Time `json:"time"`
	StreamID   string    `json:"stream_id,omitempty"`
	StreamType string    `json:"stream_type,omitempty"`
	Message    string    `json:"message,omitempty"`
	Error      error     `json:"-"`
	Meta       any       `json:"meta,omitempty"`
}

type EventType string

const (
	EventTypeStreamStarted        EventType = "stream_started"
	EventTypeStreamStopped        EventType = "stream_stopped"
	EventTypeStreamClosed         EventType = "stream_closed"
	EventTypeStreamError          EventType = "stream_error"
	EventTypeManifestUpdated      EventType = "manifest_updated"
	EventTypeTimelineEventApplied EventType = "timeline_event_applied"
	EventTypeStreamAdded          EventType = "stream_added"
	EventTypeInputAdded           EventType = "input_added"
	EventTypeInputRemoved         EventType = "input_removed"
	EventTypeDestinationAdded     EventType = "destination_added"
	EventTypeDestinationRemoved   EventType = "destination_removed"
	EventTypeInputSwitched        EventType = "input_switched"
	EventTypeSceneAdded           EventType = "scene_added"
	EventTypeSceneActivated       EventType = "scene_activated"
	EventTypeSegmentGenerated     EventType = "segment_generated"
	EventTypeRecordStarted        EventType = "record_started"
)

type StreamLifecycleMeta struct {
	URL         string `json:"url,omitempty"`
	Restartable bool   `json:"restartable,omitempty"`
}

type ChildStreamMeta struct {
	Role       string `json:"role,omitempty"`
	ChildID    string `json:"child_id,omitempty"`
	ChildType  string `json:"child_type,omitempty"`
	ChildURL   string `json:"child_url,omitempty"`
	Managed    bool   `json:"managed,omitempty"`
	Replaced   bool   `json:"replaced,omitempty"`
	ChannelID  string `json:"channel_id,omitempty"`
	ProgramID  string `json:"program_id,omitempty"`
	SceneID    string `json:"scene_id,omitempty"`
	SceneCount int    `json:"scene_count,omitempty"`
}

type InputSwitchedMeta struct {
	PreviousInputID string `json:"previous_input_id,omitempty"`
	CurrentInputID  string `json:"current_input_id,omitempty"`
}

type RestartMeta struct {
	PreviousLastIO time.Time `json:"previous_last_io,omitempty"`
	Interval       string    `json:"interval,omitempty"`
}

type SegmentGeneratedMeta struct {
	Sequence        int     `json:"sequence"`
	FileName        string  `json:"file_name"`
	SegmentURL      string  `json:"segment_url,omitempty"`
	PlaylistName    string  `json:"playlist_name,omitempty"`
	PlaylistURL     string  `json:"playlist_url,omitempty"`
	DurationSeconds float64 `json:"duration_seconds"`
}

type RecordStartedMeta struct {
	SessionID     string   `json:"session_id,omitempty"`
	PlaylistName  string   `json:"playlist_name,omitempty"`
	PlaylistURL   string   `json:"playlist_url,omitempty"`
	SegmentCount  int      `json:"segment_count"`
	SegmentURLs   []string `json:"segment_urls,omitempty"`
	StartedAtUnix int64    `json:"started_at_unix,omitempty"`
}

type EventEmitter struct {
	ch        chan Event
	closeOnce sync.Once
	stopped   bool
}

func NewEventEmitter(bufferSize int) *EventEmitter {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	return &EventEmitter{
		ch: make(chan Event, bufferSize),
	}
}

func (e *EventEmitter) Chan() chan Event {
	if e == nil {
		return nil
	}
	return e.ch
}

func (e *EventEmitter) Emit(event Event) {
	if e == nil || e.ch == nil || e.stopped {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	select {
	case e.ch <- event:
	default:
	}
}

// after closing emiting will panic
func (e *EventEmitter) Close() {
	if e == nil || e.ch == nil {
		return
	}

	e.stopped = true

	e.closeOnce.Do(func() {
		close(e.ch)
	})
}

type Frame struct {
	PTS           time.Duration
	DTS           time.Duration
	Duration      time.Duration
	Payload       [][]byte
	Codec         string
	PacketType    string
	Timestamp     time.Time
	InputID       string
	IsKeyFrame    bool
	Discontinuity bool
	SequenceID    int64
	GOPID         int64
	IsFile        bool
	SampleRate    int // audio sample rate in Hz, 0 if unknown
	VideoSPS      []byte
	VideoPPS      []byte
}

type State struct {
	IsStarted          bool          `json:"is_started"`
	IsRemovable        bool          `json:"is_removable,omitempty"`
	IsResumable        bool          `json:"is_resumable"`
	RunnerDetails      string        `json:"runner_details"`
	LastIO             time.Time     `json:"last_io"`
	StreamID           string        `json:"id"`
	Type               string        `json:"type"`
	Url                string        `json:"url"`
	AudioFps           float64       `json:"audio_fps"`
	VideoFps           float64       `json:"video_fps"`
	LocalPath          string        `json:"local_path,omitempty"`
	ServeType          string        `json:"serve_type,omitempty"`
	ServeMode          string        `json:"serve_mode,omitempty"`
	Served             []ServedState `json:"served,omitempty"`
	DroppedAudioFrames float64       `json:"dropped_audio_frames"`
	DroppedVideoFrames float64       `json:"dropped_video_frames"`
	TotalVideoFrames   int64         `json:"total_video_frames"`
	TotalAudioFrames   int64         `json:"total_audio_frames"`
}

type ServedState struct {
	StreamID  string `json:"id,omitempty"`
	Url       string `json:"url,omitempty"`
	LocalPath string `json:"local_path,omitempty"`
	ServeType string `json:"serve_type,omitempty"`
	ServeMode string `json:"serve_mode,omitempty"`
}

func CloneFrame(frame *Frame) *Frame {
	if frame == nil {
		return nil
	}

	cloned := *frame
	if len(frame.Payload) > 0 {
		cloned.Payload = make([][]byte, len(frame.Payload))
		for i, payload := range frame.Payload {
			cloned.Payload[i] = append([]byte(nil), payload...)
		}
	}
	cloned.VideoSPS = append([]byte(nil), frame.VideoSPS...)
	cloned.VideoPPS = append([]byte(nil), frame.VideoPPS...)
	return &cloned
}

func CloneStreams(streams []Stream) ([]Stream, error) {
	if len(streams) == 0 {
		return nil, nil
	}

	cloned := make([]Stream, 0, len(streams))
	for _, stream := range streams {
		if stream == nil {
			continue
		}
		clone, err := stream.Clone()
		if err != nil {
			return nil, err
		}
		cloned = append(cloned, clone)
	}
	return cloned, nil
}

func StartSidecars(streams []Stream) {
	for _, stream := range streams {
		if stream != nil {
			stream.Start()
		}
	}
}

func StopSidecars(streams []Stream) {
	for _, stream := range streams {
		if stream != nil {
			stream.Stop()
		}
	}
}

func CloseSidecars(streams []Stream) {
	for _, stream := range streams {
		if stream != nil {
			stream.Close()
		}
	}
}

func PushToSidecars(streams []Stream, frame *Frame, video bool) {
	if frame == nil {
		return
	}
	for _, stream := range streams {
		if stream == nil {
			continue
		}
		var ch chan *Frame
		if video {
			ch = stream.GetVideoChan()
		} else {
			ch = stream.GetAudioChan()
		}
		if ch == nil {
			continue
		}
		cloned := CloneFrame(frame)
		select {
		case ch <- cloned:
		case <-time.After(sidecarPushTimeout):
		}
	}
}

func MergeServedState(parent *State, sidecars []Stream) *State {
	if parent == nil {
		parent = &State{}
	}

	merged := *parent
	parentServed := collectServedFromState(parent)
	sidecarServed := collectServedFromStreams(sidecars)
	if len(parentServed) == 0 && len(sidecarServed) == 0 {
		return &merged
	}

	merged.Served = append(parentServed, sidecarServed...)
	return &merged
}

func collectServedFromStreams(streams []Stream) []ServedState {
	served := make([]ServedState, 0, len(streams))
	for _, stream := range streams {
		if stream == nil {
			continue
		}
		state := stream.State()
		if state == nil {
			continue
		}
		if len(state.Served) > 0 {
			served = append(served, state.Served...)
			continue
		}
		if item, ok := servedStateFromState(state); ok {
			served = append(served, item)
		}
	}
	return served
}

func collectServedFromState(state *State) []ServedState {
	if state == nil {
		return nil
	}
	if len(state.Served) > 0 {
		return append([]ServedState(nil), state.Served...)
	}
	if item, ok := servedStateFromState(state); ok {
		return []ServedState{item}
	}
	return nil
}

func servedStateFromState(state *State) (ServedState, bool) {
	if state == nil {
		return ServedState{}, false
	}
	if state.LocalPath == "" && state.ServeType == "" && state.ServeMode == "" {
		return ServedState{}, false
	}
	return ServedState{
		StreamID:  state.StreamID,
		Url:       state.Url,
		LocalPath: state.LocalPath,
		ServeType: state.ServeType,
		ServeMode: state.ServeMode,
	}, true
}

const (
	InputTypeSRT   StreamType = "srt"
	InputTypeRTMP  StreamType = "rtmp"
	InputTypeRTSP  StreamType = "rtsp"
	InputTypeFILE  StreamType = "file"
	InputTypePRINT StreamType = "printer"
)
