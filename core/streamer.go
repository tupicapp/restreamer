package irajstreamer

import (
	"strings"
	"sync"

	shared "github.com/tupicapp/restreamer/core/shared"
)

type Streamer struct {
	IsStarted bool

	inputs        map[string]Stream
	outputs       map[string]Stream
	inputsMu      *sync.RWMutex
	outputsMu     *sync.RWMutex
	activeInputID string
	SwitchChan    chan string

	MultiCaster   MultiCaster
	stagedInputID string
	id            string

	hlsFolders     *shared.HLSFolders
	hlsConfig      HLSConfig
	recorderConfig RecorderConfig

	events   *shared.EventEmitter
	listener EventListener

	closeOnce sync.Once
	done      chan struct{}
}

type pauseWhenInactiveCapable interface {
	ShouldPauseWhenInactive() bool
}

func NewStreamer(opts ...StreamerOption) *Streamer {
	multicaster := NewMultiCaster()
	streamer := &Streamer{
		inputs:      make(map[string]Stream),
		outputs:     make(map[string]Stream),
		inputsMu:    &sync.RWMutex{},
		outputsMu:   &sync.RWMutex{},
		done:        make(chan struct{}),
		SwitchChan:  make(chan string, 10),
		MultiCaster: multicaster,
		hlsFolders:  shared.NewHLSFolders(),
		events:      shared.NewEventEmitter(256),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(streamer)
		}
	}
	multicaster.SetStreamer(streamer)
	return streamer
}

func (s *Streamer) hlsPlaylistName() string {
	name := strings.TrimSpace(s.hlsConfig.PlaylistName)
	if name == "" {
		return "stream.m3u8"
	}
	return name
}

func (s *Streamer) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.IsStarted = false

		if s.MultiCaster != nil {
			s.MultiCaster.Close()
		}

		for _, v := range s.inputs {
			v.Close()
		}

		for _, v := range s.outputs {
			v.Close()
		}

		s.emitEvent(shared.Event{
			Type:       shared.EventTypeStreamClosed,
			StreamID:   s.streamerIDOrDefault(),
			StreamType: "streamer",
			Message:    "streamer closed",
		})
		s.events.Close()
	})
}

func (s *Streamer) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *Streamer) AttachEventListener(listener EventListener) {
	if listener == nil {
		return
	}
	s.listener = listener
	s.listener.Watch(s)
}

func (s *Streamer) emitEvent(event shared.Event) {
	if s.events == nil {
		return
	}
	if event.StreamID == "" {
		event.StreamID = s.streamerIDOrDefault()
	}
	if event.StreamType == "" {
		event.StreamType = "streamer"
	}
	s.events.Emit(event)
}

func (s *Streamer) EmitEvent(event shared.Event) {
	s.emitEvent(event)
}

func (s *Streamer) watchStream(stream Stream) {
	if s.listener == nil || stream == nil {
		return
	}
	s.listener.Watch(stream)
}

func (s *Streamer) streamerIDOrDefault() string {
	if strings.TrimSpace(s.id) != "" {
		return strings.TrimSpace(s.id)
	}
	return "streamer"
}

func (s *Streamer) shouldStartInputLocked(stream Stream, inputID string) bool {
	if !shouldPauseWhenInactive(stream) {
		return true
	}
	return inputID == s.activeInputID || inputID == s.stagedInputID
}
