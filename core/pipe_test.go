package irajstreamer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
)

type pipeTestInput struct {
	id string

	video chan *Frame
	audio chan *Frame
	done  chan struct{}

	started bool
}

func newPipeTestInput(id string) *pipeTestInput {
	return &pipeTestInput{
		id:    id,
		video: make(chan *Frame, defaultPipeBufferSize),
		audio: make(chan *Frame, defaultPipeBufferSize),
		done:  make(chan struct{}),
	}
}

func (p *pipeTestInput) GetVideoChan() chan *Frame              { return p.video }
func (p *pipeTestInput) GetAudioChan() chan *Frame              { return p.audio }
func (p *pipeTestInput) GetID() string                          { return p.id }
func (p *pipeTestInput) Type() string                           { return "pipe_test_input" }
func (p *pipeTestInput) IsRestartable() bool                    { return false }
func (p *pipeTestInput) RestartInterval() time.Duration         { return time.Second }
func (p *pipeTestInput) EventChan() chan shared.Event           { return make(chan shared.Event) }
func (p *pipeTestInput) Start()                                 { p.started = true }
func (p *pipeTestInput) Stop()                                  { p.started = false }
func (p *pipeTestInput) Close()                                 { p.started = false; close(p.done) }
func (p *pipeTestInput) Clone() (shared.Stream, error)          { return nil, fmt.Errorf("clone not supported") }
func (p *pipeTestInput) WaitForStart(ctx context.Context) error { return nil }

func (p *pipeTestInput) State() *shared.State {
	return &shared.State{
		IsStarted: p.started,
		StreamID:  p.id,
		Type:      p.Type(),
	}
}

type pipeTestSink struct {
	id string

	video chan *Frame
	audio chan *Frame
	done  chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
	runWG     sync.WaitGroup

	mu          sync.Mutex
	started     bool
	videoFrames []*Frame
	audioFrames []*Frame
}

func newPipeTestSink(id string) *pipeTestSink {
	return &pipeTestSink{
		id:    id,
		video: make(chan *Frame, defaultPipeBufferSize),
		audio: make(chan *Frame, defaultPipeBufferSize),
		done:  make(chan struct{}),
	}
}

func (p *pipeTestSink) GetVideoChan() chan *Frame      { return p.video }
func (p *pipeTestSink) GetAudioChan() chan *Frame      { return p.audio }
func (p *pipeTestSink) GetID() string                  { return p.id }
func (p *pipeTestSink) Type() string                   { return "pipe_test_sink" }
func (p *pipeTestSink) IsRestartable() bool            { return false }
func (p *pipeTestSink) RestartInterval() time.Duration { return time.Second }
func (p *pipeTestSink) EventChan() chan shared.Event   { return make(chan shared.Event) }

func (p *pipeTestSink) Start() {
	p.startOnce.Do(func() {
		p.runWG.Add(2)
		go p.collectVideo()
		go p.collectAudio()
	})

	p.mu.Lock()
	p.started = true
	p.mu.Unlock()
}

func (p *pipeTestSink) Stop() {
	p.mu.Lock()
	p.started = false
	p.mu.Unlock()
}

func (p *pipeTestSink) Close() {
	p.closeOnce.Do(func() {
		p.Stop()
		close(p.done)
		p.runWG.Wait()
	})
}

func (p *pipeTestSink) Clone() (shared.Stream, error) { return nil, fmt.Errorf("clone not supported") }

func (p *pipeTestSink) WaitForStart(ctx context.Context) error { return nil }

func (p *pipeTestSink) State() *shared.State {
	p.mu.Lock()
	defer p.mu.Unlock()

	return &shared.State{
		IsStarted:        p.started,
		StreamID:         p.id,
		Type:             p.Type(),
		TotalVideoFrames: int64(len(p.videoFrames)),
		TotalAudioFrames: int64(len(p.audioFrames)),
	}
}

func (p *pipeTestSink) collectVideo() {
	defer p.runWG.Done()

	for {
		select {
		case <-p.done:
			return
		case frame := <-p.video:
			if frame == nil {
				continue
			}
			p.mu.Lock()
			p.videoFrames = append(p.videoFrames, frame)
			p.mu.Unlock()
		}
	}
}

func (p *pipeTestSink) collectAudio() {
	defer p.runWG.Done()

	for {
		select {
		case <-p.done:
			return
		case frame := <-p.audio:
			if frame == nil {
				continue
			}
			p.mu.Lock()
			p.audioFrames = append(p.audioFrames, frame)
			p.mu.Unlock()
		}
	}
}

func (p *pipeTestSink) videoCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.videoFrames)
}

func (p *pipeTestSink) audioCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.audioFrames)
}

func TestPipeAsInputAsOutputReuseSameWrappers(t *testing.T) {
	pipe := NewPipe("cascade")

	if got, want := pipe.AsInput(), pipe.AsInput(); got != want {
		t.Fatalf("expected AsInput to reuse the same wrapper")
	}
	if got, want := pipe.AsOutput(), pipe.AsOutput(); got != want {
		t.Fatalf("expected AsOutput to reuse the same wrapper")
	}
}

func TestPipeDirectForwardingStopAndClose(t *testing.T) {
	pipe := NewPipe("cascade")
	input := pipe.AsInput()
	output := pipe.AsOutput()

	input.Start()
	output.Start()

	videoFrame := &Frame{SequenceID: 1, InputID: "src-video"}
	audioFrame := &Frame{SequenceID: 2, InputID: "src-audio"}

	output.GetVideoChan() <- videoFrame
	output.GetAudioChan() <- audioFrame

	if got := readFrame(t, input.GetVideoChan()); got != videoFrame {
		t.Fatalf("expected forwarded video frame")
	}
	if got := readFrame(t, input.GetAudioChan()); got != audioFrame {
		t.Fatalf("expected forwarded audio frame")
	}

	input.Stop()
	output.GetVideoChan() <- &Frame{SequenceID: 3}
	expectNoFrame(t, input.GetVideoChan(), 200*time.Millisecond)

	input.Start()
	resumed := &Frame{SequenceID: 4}
	output.GetVideoChan() <- resumed
	if got := readFrame(t, input.GetVideoChan()); got != resumed {
		t.Fatalf("expected resumed video frame")
	}

	output.Close()

	expectClosed(t, input.GetVideoChan(), time.Second)
	expectClosed(t, input.GetAudioChan(), time.Second)
}

func TestPipeStreamerCascade(t *testing.T) {
	upstream := NewStreamer()
	defer upstream.Close()

	downstream := NewStreamer()
	defer downstream.Close()

	pipe := NewPipe("cascade")
	source := newPipeTestInput("source")
	sink := newPipeTestSink("sink")
	defer sink.Close()

	if err := upstream.UpdateStreams([]Stream{source}, []Stream{pipe.AsOutput()}); err != nil {
		t.Fatalf("upstream UpdateStreams failed: %v", err)
	}
	if err := downstream.UpdateStreams([]Stream{pipe.AsInput()}, []Stream{sink}); err != nil {
		t.Fatalf("downstream UpdateStreams failed: %v", err)
	}

	if ok := upstream.Switch(source.GetID()); !ok {
		t.Fatalf("expected upstream switch to succeed")
	}
	if ok := downstream.Switch(pipe.AsInput().GetID()); !ok {
		t.Fatalf("expected downstream switch to succeed")
	}

	upstream.StartLife()
	downstream.StartLife()
	upstream.Start()
	downstream.Start()

	sourceVideo := &Frame{SequenceID: 10, InputID: source.GetID()}
	sourceAudio := &Frame{SequenceID: 11, InputID: source.GetID()}

	source.GetVideoChan() <- sourceVideo
	source.GetAudioChan() <- sourceAudio

	waitForCount(t, time.Second, sink.videoCount, 1, "video")
	waitForCount(t, time.Second, sink.audioCount, 1, "audio")
}

func readFrame(t *testing.T, ch <-chan *Frame) *Frame {
	t.Helper()

	select {
	case frame := <-ch:
		return frame
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for frame")
		return nil
	}
}

func expectNoFrame(t *testing.T, ch <-chan *Frame, d time.Duration) {
	t.Helper()

	select {
	case frame := <-ch:
		t.Fatalf("expected no frame, got %#v", frame)
	case <-time.After(d):
	}
}

func expectClosed(t *testing.T, ch <-chan *Frame, d time.Duration) {
	t.Helper()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("expected channel to be closed")
		}
	case <-time.After(d):
		t.Fatalf("timed out waiting for channel close")
	}
}

func waitForCount(t *testing.T, timeout time.Duration, fn func() int, want int, label string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := fn(); got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s frames: want at least %d, got %d", label, want, fn())
}
