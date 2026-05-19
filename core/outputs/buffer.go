package outputs

import (
	"context"
	"fmt"
	"github.com/tupicapp/restreamer/core/shared"
	"sync"
	"time"
)

// buffering is a Stream implementation that buffers all frames it receives
type buffering struct {
	id        string
	videoChan chan *shared.Frame
	audioChan chan *shared.Frame

	// Buffers to store all received frames
	videoFrames []*shared.Frame
	audioFrames []*shared.Frame
	bufferMu    sync.Mutex

	audioMu sync.RWMutex
	videoMu sync.RWMutex

	IsStarted   bool
	IsInitiated bool
	done        chan struct{}
	started     chan struct{}
	events      *shared.EventEmitter

	TotalVideoFrames   int64
	TotalAudioFrames   int64
	DroppedVideoFrames int64
	DroppedAudioFrames int64
	LastIO             time.Time
}

func NewBuffering(id string) *buffering {
	return &buffering{
		id:        id,
		videoChan: make(chan *shared.Frame, 100),
		audioChan: make(chan *shared.Frame, 100),
		done:      make(chan struct{}),
		started:   make(chan struct{}),
		events:    shared.NewEventEmitter(128),
	}
}

func (b *buffering) GetVideoChan() chan *shared.Frame { return b.videoChan }
func (b *buffering) GetAudioChan() chan *shared.Frame { return b.audioChan }
func (b *buffering) GetID() string                    { return b.id }
func (b *buffering) Type() string                     { return "buffering_destination" }
func (b *buffering) AudioLock() *sync.RWMutex         { return &b.audioMu }
func (b *buffering) VideoLock() *sync.RWMutex         { return &b.videoMu }
func (b *buffering) IsRestartable() bool              { return false }
func (b *buffering) RestartInterval() time.Duration   { return 100 * time.Second }

func (b *buffering) State() *shared.State {
	b.videoMu.RLock()
	b.audioMu.RLock()
	defer b.videoMu.RUnlock()
	defer b.audioMu.RUnlock()

	return &shared.State{
		LastIO:             b.LastIO,
		IsStarted:          b.IsStarted,
		StreamID:           b.id,
		Type:               b.Type(),
		DroppedAudioFrames: float64(b.DroppedAudioFrames),
		DroppedVideoFrames: float64(b.DroppedVideoFrames),
		TotalVideoFrames:   b.TotalVideoFrames,
		TotalAudioFrames:   b.TotalAudioFrames,
	}
}

func (b *buffering) Clone() (shared.Stream, error) {
	return NewBuffering(b.id), nil
}

func (b *buffering) WaitForStart(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.started:
		return nil
	case <-b.done:
		return fmt.Errorf("buffering destination is closed")
	}
}

func (b *buffering) EventChan() chan shared.Event {
	if b.events == nil {
		return nil
	}
	return b.events.Chan()
}

func (b *buffering) Start() {
	if b.IsInitiated {
		b.IsStarted = true
		b.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: b.id, StreamType: b.Type(), Message: "buffering destination resumed"})
		return
	}

	b.IsInitiated = true
	b.IsStarted = true
	close(b.started)
	b.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: b.id, StreamType: b.Type(), Message: "buffering destination started"})

	go b.runVideo()
	go b.runAudio()
}

func (b *buffering) runVideo() {
	for {
		select {
		case <-b.done:
			return
		case frame := <-b.videoChan:
			if frame != nil {
				b.bufferMu.Lock()
				b.videoFrames = append(b.videoFrames, frame)
				b.TotalVideoFrames++
				b.LastIO = time.Now()
				b.bufferMu.Unlock()
			}
		}
	}
}
func (b *buffering) runAudio() {
	for {
		select {
		case <-b.done:
			return
		case frame := <-b.audioChan:
			if frame != nil {
				b.bufferMu.Lock()
				b.audioFrames = append(b.audioFrames, frame)
				b.TotalAudioFrames++
				b.LastIO = time.Now()
				b.bufferMu.Unlock()
			}
		}
	}
}

func (b *buffering) Stop() {
	b.IsStarted = false
	b.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: b.id, StreamType: b.Type(), Message: "buffering destination stopped"})
}

func (b *buffering) Close() {
	b.Stop()
	close(b.done)
	b.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: b.id, StreamType: b.Type(), Message: "buffering destination closed"})
	b.events.Close()
}

func (b *buffering) IsKeyFrame(*shared.Frame) bool { return true }
func (b *buffering) OnSwitch()                     {}

func (b *buffering) GetVideoFrames() []*shared.Frame {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	// Return a copy to avoid race conditions
	result := make([]*shared.Frame, len(b.videoFrames))
	copy(result, b.videoFrames)
	return result
}

func (b *buffering) GetAudioFrames() []*shared.Frame {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	// Return a copy to avoid race conditions
	result := make([]*shared.Frame, len(b.audioFrames))
	copy(result, b.audioFrames)
	return result
}
