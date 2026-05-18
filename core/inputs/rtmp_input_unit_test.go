package inputs

import (
	"context"
	"testing"
	"time"

	shared "restreamer/irajstreamer/core/shared"
)

type rtmpInputMockWatcher struct {
	id      string
	stopped int
	closed  int
	videoCh chan *Frame
	audioCh chan *Frame
	events  chan shared.Event
}

func newRTMPInputMockWatcher(id string) *rtmpInputMockWatcher {
	return &rtmpInputMockWatcher{
		id:      id,
		videoCh: make(chan *Frame, 1),
		audioCh: make(chan *Frame, 1),
		events:  make(chan shared.Event, 1),
	}
}

func (m *rtmpInputMockWatcher) GetVideoChan() chan *Frame { return m.videoCh }
func (m *rtmpInputMockWatcher) GetAudioChan() chan *Frame { return m.audioCh }
func (m *rtmpInputMockWatcher) GetID() string             { return m.id }
func (m *rtmpInputMockWatcher) Start()                    {}
func (m *rtmpInputMockWatcher) Stop()                     { m.stopped++ }
func (m *rtmpInputMockWatcher) Close()                    { m.closed++ }
func (m *rtmpInputMockWatcher) State() *shared.State {
	return &shared.State{StreamID: m.id, Type: "mock", Url: m.id}
}
func (m *rtmpInputMockWatcher) Clone() (Stream, error) { return newRTMPInputMockWatcher(m.id), nil }
func (m *rtmpInputMockWatcher) WaitForStart(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
func (m *rtmpInputMockWatcher) Type() string                   { return "mock" }
func (m *rtmpInputMockWatcher) IsRestartable() bool            { return false }
func (m *rtmpInputMockWatcher) RestartInterval() time.Duration { return 0 }
func (m *rtmpInputMockWatcher) EventChan() chan shared.Event   { return m.events }

func TestRTMPInput_CloseRunsOnCloseAfterWatcherShutdown(t *testing.T) {
	watcher := newRTMPInputMockWatcher("watcher-1")
	input := NewRTMPInput("input-1", "rtmp://example/live", nil, []Stream{watcher})

	callbackCalled := false
	input.SetOnClose(func() {
		callbackCalled = true
		if watcher.closed != 1 {
			t.Fatalf("expected watcher to be closed before onClose callback, got %d", watcher.closed)
		}
		input.Close()
	})

	input.Close()

	if !callbackCalled {
		t.Fatalf("expected onClose callback to run")
	}
	if watcher.closed != 1 {
		t.Fatalf("expected watcher closed exactly once, got %d", watcher.closed)
	}
}
