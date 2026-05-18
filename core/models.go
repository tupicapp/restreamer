package irajstreamer

import shared "restreamer/core/shared"

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
