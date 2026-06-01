package irajstreamer

import (
	"context"
	"fmt"
	"sync"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
)

const (
	defaultPipeBufferSize = 128
	defaultPipeWriteWait  = 100 * time.Millisecond
)

type Pipe struct {
	core *pipeCore
}

type pipeCore struct {
	id string

	video chan *Frame
	audio chan *Frame

	mu     sync.Mutex
	input  *pipeInputStream
	output *pipeOutputStream

	closeOnce sync.Once
}

type pipeSideState struct {
	mu sync.RWMutex

	started bool
	closed  bool
	lastIO  time.Time

	totalVideoFrames   int64
	totalAudioFrames   int64
	droppedVideoFrames int64
	droppedAudioFrames int64
}

type pipeOutputStream struct {
	id   string
	pipe *pipeCore

	videoIn chan *Frame
	audioIn chan *Frame

	events *shared.EventEmitter

	started chan struct{}
	done    chan struct{}

	startOnce  sync.Once
	signalOnce sync.Once
	closeOnce  sync.Once
	runWG      sync.WaitGroup

	state pipeSideState
}

type pipeInputStream struct {
	id   string
	pipe *pipeCore

	videoOut chan *Frame
	audioOut chan *Frame

	events *shared.EventEmitter

	started chan struct{}
	done    chan struct{}

	startOnce  sync.Once
	signalOnce sync.Once
	closeOnce  sync.Once
	chanOnce   sync.Once
	runWG      sync.WaitGroup

	state pipeSideState
}

func NewPipe(id string) *Pipe {
	return &Pipe{
		core: &pipeCore{
			id:    id,
			video: make(chan *Frame, defaultPipeBufferSize),
			audio: make(chan *Frame, defaultPipeBufferSize),
		},
	}
}

func (p *Pipe) AsInput() Stream {
	if p == nil || p.core == nil {
		return nil
	}

	p.core.mu.Lock()
	defer p.core.mu.Unlock()

	if p.core.input != nil {
		return p.core.input
	}

	p.core.input = &pipeInputStream{
		id:       p.core.id + ":input",
		pipe:     p.core,
		videoOut: make(chan *Frame, defaultPipeBufferSize),
		audioOut: make(chan *Frame, defaultPipeBufferSize),
		events:   shared.NewEventEmitter(64),
		started:  make(chan struct{}),
		done:     make(chan struct{}),
	}

	return p.core.input
}

func (p *Pipe) AsOutput() Stream {
	if p == nil || p.core == nil {
		return nil
	}

	p.core.mu.Lock()
	defer p.core.mu.Unlock()

	if p.core.output != nil {
		return p.core.output
	}

	p.core.output = &pipeOutputStream{
		id:      p.core.id + ":output",
		pipe:    p.core,
		videoIn: make(chan *Frame, defaultPipeBufferSize),
		audioIn: make(chan *Frame, defaultPipeBufferSize),
		events:  shared.NewEventEmitter(64),
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}

	return p.core.output
}

func (p *pipeOutputStream) GetVideoChan() chan *Frame { return p.videoIn }

func (p *pipeOutputStream) GetAudioChan() chan *Frame { return p.audioIn }

func (p *pipeOutputStream) GetID() string { return p.id }

func (p *pipeOutputStream) Start() {
	p.startOnce.Do(func() {
		p.runWG.Add(2)
		go p.forwardVideo()
		go p.forwardAudio()
	})

	alreadyStarted := p.state.setStarted(true)
	if !alreadyStarted {
		p.signalOnce.Do(func() {
			close(p.started)
		})
		p.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: p.id, StreamType: p.Type(), Message: "pipe output started"})
		return
	}

	p.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: p.id, StreamType: p.Type(), Message: "pipe output resumed"})
}

func (p *pipeOutputStream) Stop() {
	if !p.state.setStarted(false) {
		return
	}
	p.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: p.id, StreamType: p.Type(), Message: "pipe output stopped"})
}

func (p *pipeOutputStream) Close() {
	p.closeOnce.Do(func() {
		p.state.markClosed()
		close(p.done)
		p.runWG.Wait()
		p.pipe.closeBridge()
		p.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: p.id, StreamType: p.Type(), Message: "pipe output closed"})
		p.events.Close()
	})
}

func (p *pipeOutputStream) State() *shared.State {
	return p.state.snapshot(p.id, p.Type())
}

func (p *pipeOutputStream) Clone() (shared.Stream, error) {
	return nil, fmt.Errorf("pipe output cannot be cloned")
}

func (p *pipeOutputStream) WaitForStart(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.started:
		return nil
	case <-p.done:
		return fmt.Errorf("pipe output is closed")
	}
}

func (p *pipeOutputStream) Type() string                   { return "pipe_output" }
func (p *pipeOutputStream) IsRestartable() bool            { return false }
func (p *pipeOutputStream) RestartInterval() time.Duration { return time.Second }

func (p *pipeOutputStream) EventChan() chan shared.Event {
	if p.events == nil {
		return nil
	}
	return p.events.Chan()
}

func (p *pipeOutputStream) forwardVideo() {
	defer p.runWG.Done()

	for {
		select {
		case <-p.done:
			return
		case frame := <-p.videoIn:
			if frame == nil {
				continue
			}
			if !p.state.isStarted() {
				p.state.recordDrop(true)
				continue
			}
			select {
			case p.pipe.video <- frame:
				p.state.recordIO(true)
			case <-time.After(defaultPipeWriteWait):
				p.state.recordDrop(true)
			case <-p.done:
				return
			}
		}
	}
}

func (p *pipeOutputStream) forwardAudio() {
	defer p.runWG.Done()

	for {
		select {
		case <-p.done:
			return
		case frame := <-p.audioIn:
			if frame == nil {
				continue
			}
			if !p.state.isStarted() {
				p.state.recordDrop(false)
				continue
			}
			select {
			case p.pipe.audio <- frame:
				p.state.recordIO(false)
			case <-time.After(defaultPipeWriteWait):
				p.state.recordDrop(false)
			case <-p.done:
				return
			}
		}
	}
}

func (p *pipeInputStream) GetVideoChan() chan *Frame { return p.videoOut }

func (p *pipeInputStream) GetAudioChan() chan *Frame { return p.audioOut }

func (p *pipeInputStream) GetID() string { return p.id }

func (p *pipeInputStream) Start() {
	p.startOnce.Do(func() {
		p.runWG.Add(2)
		go p.forwardVideo()
		go p.forwardAudio()
	})

	alreadyStarted := p.state.setStarted(true)
	if !alreadyStarted {
		p.signalOnce.Do(func() {
			close(p.started)
		})
		p.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: p.id, StreamType: p.Type(), Message: "pipe input started"})
		return
	}

	p.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: p.id, StreamType: p.Type(), Message: "pipe input resumed"})
}

func (p *pipeInputStream) Stop() {
	if !p.state.setStarted(false) {
		return
	}
	p.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: p.id, StreamType: p.Type(), Message: "pipe input stopped"})
}

func (p *pipeInputStream) Close() {
	p.closeOnce.Do(func() {
		p.state.markClosed()
		close(p.done)
		p.runWG.Wait()
		p.closeSideChannels()
		p.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: p.id, StreamType: p.Type(), Message: "pipe input closed"})
		p.events.Close()
	})
}

func (p *pipeInputStream) State() *shared.State {
	return p.state.snapshot(p.id, p.Type())
}

func (p *pipeInputStream) Clone() (shared.Stream, error) {
	return nil, fmt.Errorf("pipe input cannot be cloned")
}

func (p *pipeInputStream) WaitForStart(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.started:
		return nil
	case <-p.done:
		return fmt.Errorf("pipe input is closed")
	}
}

func (p *pipeInputStream) Type() string                   { return "pipe_input" }
func (p *pipeInputStream) IsRestartable() bool            { return false }
func (p *pipeInputStream) RestartInterval() time.Duration { return time.Second }

func (p *pipeInputStream) EventChan() chan shared.Event {
	if p.events == nil {
		return nil
	}
	return p.events.Chan()
}

func (p *pipeInputStream) forwardVideo() {
	defer func() {
		p.closeVideoChannel()
		p.runWG.Done()
	}()

	for {
		select {
		case <-p.done:
			return
		case frame, ok := <-p.pipe.video:
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			if !p.state.isStarted() {
				p.state.recordDrop(true)
				continue
			}
			select {
			case p.videoOut <- frame:
				p.state.recordIO(true)
			case <-time.After(defaultPipeWriteWait):
				p.state.recordDrop(true)
			case <-p.done:
				return
			}
		}
	}
}

func (p *pipeInputStream) forwardAudio() {
	defer func() {
		p.closeAudioChannel()
		p.runWG.Done()
	}()

	for {
		select {
		case <-p.done:
			return
		case frame, ok := <-p.pipe.audio:
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			if !p.state.isStarted() {
				p.state.recordDrop(false)
				continue
			}
			select {
			case p.audioOut <- frame:
				p.state.recordIO(false)
			case <-time.After(defaultPipeWriteWait):
				p.state.recordDrop(false)
			case <-p.done:
				return
			}
		}
	}
}

func (p *pipeInputStream) closeSideChannels() {
	p.closeVideoChannel()
	p.closeAudioChannel()
}

func (p *pipeInputStream) closeVideoChannel() {
	p.chanOnce.Do(func() {
		close(p.videoOut)
		close(p.audioOut)
	})
}

func (p *pipeInputStream) closeAudioChannel() {
	p.chanOnce.Do(func() {
		close(p.videoOut)
		close(p.audioOut)
	})
}

func (p *pipeCore) closeBridge() {
	p.closeOnce.Do(func() {
		close(p.video)
		close(p.audio)
	})
}

func (s *pipeSideState) isStarted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started && !s.closed
}

func (s *pipeSideState) setStarted(started bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	wasStarted := s.started
	if s.closed {
		return wasStarted
	}

	s.started = started
	return wasStarted
}

func (s *pipeSideState) markClosed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.started = false
}

func (s *pipeSideState) recordIO(video bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastIO = time.Now()
	if video {
		s.totalVideoFrames++
		return
	}
	s.totalAudioFrames++
}

func (s *pipeSideState) recordDrop(video bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastIO = time.Now()
	if video {
		s.droppedVideoFrames++
		return
	}
	s.droppedAudioFrames++
}

func (s *pipeSideState) snapshot(id string, streamType string) *shared.State {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &shared.State{
		IsStarted:          s.started && !s.closed,
		LastIO:             s.lastIO,
		StreamID:           id,
		Type:               streamType,
		DroppedAudioFrames: float64(s.droppedAudioFrames),
		DroppedVideoFrames: float64(s.droppedVideoFrames),
		TotalVideoFrames:   s.totalVideoFrames,
		TotalAudioFrames:   s.totalAudioFrames,
	}
}
