package irajstreamer

import shared "github.com/tupicapp/restreamer/core/shared"

type StreamType = shared.StreamType
type Stream = shared.Stream
type Frame = shared.Frame
type State = shared.State
type ServedState = shared.ServedState
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
}
