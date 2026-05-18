package test

import (
	"context"
	"fmt"
	core "restreamer/core"
	"restreamer/core/shared"
	"sync"
	"time"
)

type buffering struct {
	id        string
	videoChan chan *shared.Frame
	audioChan chan *shared.Frame

	videoFrames []*shared.Frame
	audioFrames []*shared.Frame
	bufferMu    sync.Mutex

	IsStarted   bool
	IsInitiated bool
	done        chan struct{}
	started     chan struct{}
	events      chan shared.Event

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
		events:    make(chan shared.Event, 8),
	}
}

func (b *buffering) GetVideoChan() chan *shared.Frame { return b.videoChan }
func (b *buffering) GetAudioChan() chan *shared.Frame { return b.audioChan }
func (b *buffering) GetID() string                    { return b.id }
func (b *buffering) EventChan() chan shared.Event     { return b.events }
func (b *buffering) Type() string                     { return "buffering_destination" }
func (b *buffering) IsRestartable() bool              { return false }
func (b *buffering) RestartInterval() time.Duration   { return 100 * time.Second }

func (b *buffering) State() *core.State {
	return &core.State{
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

func (b *buffering) Clone() (core.Stream, error) {
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

func (b *buffering) Start() {
	if b.IsInitiated {
		b.IsStarted = true
		return
	}

	b.IsInitiated = true
	b.IsStarted = true
	close(b.started)

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
}

func (b *buffering) Close() {
	b.Stop()
	close(b.done)
}

func (b *buffering) GetVideoFrames() []*shared.Frame {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	result := make([]*shared.Frame, len(b.videoFrames))
	copy(result, b.videoFrames)
	return result
}

func (b *buffering) GetAudioFrames() []*shared.Frame {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	result := make([]*shared.Frame, len(b.audioFrames))
	copy(result, b.audioFrames)
	return result
}
