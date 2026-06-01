package irajstreamer

import (
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
)

type StreamType = shared.StreamType
type Stream = shared.Stream
type Frame = shared.Frame
type State = shared.State
type Event = shared.Event
type EventType = shared.EventType
type EventSource = shared.EventSource

type MultiCaster interface {
	SetStreamer(*Streamer)
	GetAudioChan() chan *Frame
	GetVideoChan() chan *Frame
	Start()
	Close()
}

type StreamerState struct {
	IsStarted      bool   `json:"is_started"`
	IsResumable    bool   `json:"is_resumable"`
	CurrentInputID string `json:"current_input_id"`

	StreamInputs  []*State `json:"inputs"`
	StreamOutputs []*State `json:"outputs"`

	AvailableInputHLSURLs  []string `json:"available_input_hls_urls"`
	AvailableOutputHLSURLs []string `json:"available_output_hls_urls"`
	InputRecordHLSURLs     []string `json:"input_record_hls_urls"`
	OutputRecordHLSURLs    []string `json:"output_record_hls_urls"`
}

type HLSConfig struct {
	PlaylistName        string
	SegmentDuration     time.Duration
	PlaylistSize        int
	TargetDuration      int
	ChannelPlaylistSize int
	PathPrefix          string
}

type RecorderConfig struct {
	SegmentDuration time.Duration
	TargetDuration  int
	PathPrefix      string
}
