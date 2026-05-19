package scenes

import (
	"context"
	"testing"
	"time"

	shared "restreamer/core/shared"
)

func TestDeriveCanvas(t *testing.T) {
	canvas, err := DeriveCanvas([]shared.VideoLayout{
		{X: 0, Y: 0, Width: 2, Height: 2},
		{X: 2, Y: 2, Width: 4, Height: 4},
	})
	if err != nil {
		t.Fatalf("DeriveCanvas() error = %v", err)
	}
	if canvas.Width != 6 || canvas.Height != 6 {
		t.Fatalf("unexpected canvas size: got %dx%d want 6x6", canvas.Width, canvas.Height)
	}
}

func TestNewSceneUsesSceneStreamType(t *testing.T) {
	scene, err := NewScene(
		"scene-1",
		shared.NewBlackCanvasSpec(2, 2),
		[]Input{{Stream: newMockSceneStream("input-1"), Layout: shared.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2}}},
	)
	if err != nil {
		t.Fatalf("NewScene() error = %v", err)
	}
	if scene.Type() != "scene" {
		t.Fatalf("unexpected scene type: got %q want %q", scene.Type(), "scene")
	}
}

type mockSceneStream struct {
	id     string
	video  chan *shared.Frame
	audio  chan *shared.Frame
	events chan shared.Event
}

func newMockSceneStream(id string) *mockSceneStream {
	return &mockSceneStream{
		id:     id,
		video:  make(chan *shared.Frame),
		audio:  make(chan *shared.Frame),
		events: make(chan shared.Event, 1),
	}
}

func (m *mockSceneStream) GetVideoChan() chan *shared.Frame { return m.video }
func (m *mockSceneStream) GetAudioChan() chan *shared.Frame { return m.audio }
func (m *mockSceneStream) GetID() string                    { return m.id }
func (m *mockSceneStream) Start()                           {}
func (m *mockSceneStream) Stop()                            {}
func (m *mockSceneStream) Close()                           {}
func (m *mockSceneStream) Type() string                     { return "mock" }
func (m *mockSceneStream) IsRestartable() bool              { return false }
func (m *mockSceneStream) RestartInterval() time.Duration   { return 0 }
func (m *mockSceneStream) EventChan() chan shared.Event     { return m.events }

func (m *mockSceneStream) State() *shared.State {
	return &shared.State{StreamID: m.id, Type: m.Type()}
}

func (m *mockSceneStream) Clone() (shared.Stream, error) {
	return newMockSceneStream(m.id), nil
}

func (m *mockSceneStream) WaitForStart(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
