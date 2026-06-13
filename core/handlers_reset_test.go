package irajstreamer

import (
	"context"
	"sync"
	"testing"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
)

type resetMockStream struct {
	id     string
	url    string
	typ    string
	closed int
	stoped int

	videoCh chan *Frame
	audioCh chan *Frame
	events  chan shared.Event

	mu sync.Mutex
}

func newResetMockStream(id, url, typ string) *resetMockStream {
	return &resetMockStream{
		id:      id,
		url:     url,
		typ:     typ,
		videoCh: make(chan *Frame, 1),
		audioCh: make(chan *Frame, 1),
		events:  make(chan shared.Event, 8),
	}
}

func (m *resetMockStream) GetVideoChan() chan *Frame { return m.videoCh }
func (m *resetMockStream) GetAudioChan() chan *Frame { return m.audioCh }
func (m *resetMockStream) GetID() string             { return m.id }
func (m *resetMockStream) Start()                    {}
func (m *resetMockStream) Stop()                     { m.mu.Lock(); m.stoped++; m.mu.Unlock() }
func (m *resetMockStream) Close()                    { m.mu.Lock(); m.closed++; m.mu.Unlock() }
func (m *resetMockStream) State() *State {
	return &State{StreamID: m.id, Url: m.url, Type: m.typ, IsStarted: true, LastIO: time.Now()}
}
func (m *resetMockStream) Clone() (Stream, error)                 { return newResetMockStream(m.id, m.url, m.typ), nil }
func (m *resetMockStream) WaitForStart(ctx context.Context) error { return ctx.Err() }
func (m *resetMockStream) Type() string                           { return m.typ }
func (m *resetMockStream) IsRestartable() bool                    { return false }
func (m *resetMockStream) RestartInterval() time.Duration         { return time.Second }
func (m *resetMockStream) EventChan() chan shared.Event           { return m.events }
func (m *resetMockStream) closeCount() int                        { m.mu.Lock(); defer m.mu.Unlock(); return m.closed }
func (m *resetMockStream) stopCount() int                         { m.mu.Lock(); defer m.mu.Unlock(); return m.stoped }

func TestStreamerResetPipelineIfInputlessAfterUpdateStreams(t *testing.T) {
	streamer := NewStreamer()

	input := newResetMockStream("input-1", "in", "reader")
	output := newResetMockStream("output-1", "out", "writer")

	if err := streamer.UpdateStreams([]Stream{input}, []Stream{output}); err != nil {
		t.Fatalf("initial UpdateStreams() error = %v", err)
	}
	if ok := streamer.Switch(input.GetID()); !ok {
		t.Fatalf("Switch(%q) failed", input.GetID())
	}

	if err := streamer.UpdateStreams(nil, []Stream{newResetMockStream("output-2", "out2", "writer")}); err != nil {
		t.Fatalf("inputless UpdateStreams() error = %v", err)
	}

	state := streamer.State()
	if len(state.StreamInputs) != 0 {
		t.Fatalf("expected no inputs after reset, got %d", len(state.StreamInputs))
	}
	if len(state.StreamOutputs) != 0 {
		t.Fatalf("expected no outputs after reset, got %d", len(state.StreamOutputs))
	}
	if state.CurrentInputID != "" {
		t.Fatalf("expected current input to reset, got %q", state.CurrentInputID)
	}
	if output.closeCount() != 1 {
		t.Fatalf("expected original output to be closed once, got %d", output.closeCount())
	}
}

func TestStreamerResetPipelineIfInputlessAfterRemoveInput(t *testing.T) {
	streamer := NewStreamer()

	input := newResetMockStream("input-1", "in", "reader")
	output := newResetMockStream("output-1", "out", "writer")

	if err := streamer.UpdateStreams([]Stream{input}, []Stream{output}); err != nil {
		t.Fatalf("UpdateStreams() error = %v", err)
	}
	if ok := streamer.Switch(input.GetID()); !ok {
		t.Fatalf("Switch(%q) failed", input.GetID())
	}

	streamer.RemoveInput(input.GetID())

	state := streamer.State()
	if len(state.StreamInputs) != 0 {
		t.Fatalf("expected no inputs after removing last input, got %d", len(state.StreamInputs))
	}
	if len(state.StreamOutputs) != 0 {
		t.Fatalf("expected outputs to be cleared after removing last input, got %d", len(state.StreamOutputs))
	}
	if state.CurrentInputID != "" {
		t.Fatalf("expected current input to reset, got %q", state.CurrentInputID)
	}
	if output.closeCount() != 1 {
		t.Fatalf("expected output to close once, got %d", output.closeCount())
	}
	if output.stopCount() == 0 {
		t.Fatalf("expected output to be stopped before reset")
	}
}

func TestStreamerResetPipelineIfInputlessAfterAddOutput(t *testing.T) {
	streamer := NewStreamer()

	output := newResetMockStream("output-1", "out", "writer")
	if err := streamer.AddOutput(output); err != nil {
		t.Fatalf("AddOutput() error = %v", err)
	}

	state := streamer.State()
	if len(state.StreamInputs) != 0 {
		t.Fatalf("expected no inputs, got %d", len(state.StreamInputs))
	}
	if len(state.StreamOutputs) != 0 {
		t.Fatalf("expected output to be removed immediately while inputless, got %d", len(state.StreamOutputs))
	}
	if output.closeCount() != 1 {
		t.Fatalf("expected output to close once, got %d", output.closeCount())
	}
}
