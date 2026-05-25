package shared

import (
	"context"
	"sync"
	"time"
)

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
	EventTypeStreamStarted      EventType = "stream_started"
	EventTypeStreamStopped      EventType = "stream_stopped"
	EventTypeStreamClosed       EventType = "stream_closed"
	EventTypeStreamError        EventType = "stream_error"
	EventTypeStreamAdded        EventType = "stream_added"
	EventTypeInputAdded         EventType = "input_added"
	EventTypeInputRemoved       EventType = "input_removed"
	EventTypeDestinationAdded   EventType = "destination_added"
	EventTypeDestinationRemoved EventType = "destination_removed"
	EventTypeInputSwitched      EventType = "input_switched"
	EventTypeSceneAdded         EventType = "scene_added"
	EventTypeSceneActivated     EventType = "scene_activated"
	EventTypeSegmentGenerated   EventType = "segment_generated"
	EventTypeRecordStarted      EventType = "record_started"
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
	PTS        time.Duration
	DTS        time.Duration
	Duration   time.Duration
	Payload    [][]byte
	Codec      string
	PacketType string
	Timestamp  time.Time
	InputID    string
	IsKeyFrame bool
	SequenceID int64
	GOPID      int64
	IsFile     bool
	SampleRate int // audio sample rate in Hz, 0 if unknown
}

type State struct {
	IsStarted          bool      `json:"is_started"`
	IsResumable        bool      `json:"is_resumable"`
	RunnerDetails      string    `json:"runner_details"`
	LastIO             time.Time `json:"last_io"`
	StreamID           string    `json:"id"`
	Type               string    `json:"type"`
	Url                string    `json:"url"`
	AudioFps           float64   `json:"audio_fps"`
	VideoFps           float64   `json:"video_fps"`
	DroppedAudioFrames float64   `json:"dropped_audio_frames"`
	DroppedVideoFrames float64   `json:"dropped_video_frames"`
	TotalVideoFrames   int64     `json:"total_video_frames"`
	TotalAudioFrames   int64     `json:"total_audio_frames"`
}

const (
	InputTypeSRT   StreamType = "srt"
	InputTypeRTMP  StreamType = "rtmp"
	InputTypeRTSP  StreamType = "rtsp"
	InputTypeFILE  StreamType = "file"
	InputTypePRINT StreamType = "printer"
)
