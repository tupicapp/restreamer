package shared

import (
	"context"
	"testing"
	"time"
)

type sharedTestStream struct {
	id      string
	videoCh chan *Frame
	audioCh chan *Frame
}

func newSharedTestStream(id string, buffer int) *sharedTestStream {
	return &sharedTestStream{
		id:      id,
		videoCh: make(chan *Frame, buffer),
		audioCh: make(chan *Frame, buffer),
	}
}

func (s *sharedTestStream) GetVideoChan() chan *Frame { return s.videoCh }
func (s *sharedTestStream) GetAudioChan() chan *Frame { return s.audioCh }
func (s *sharedTestStream) GetID() string             { return s.id }
func (s *sharedTestStream) Start()                    {}
func (s *sharedTestStream) Stop()                     {}
func (s *sharedTestStream) Close()                    {}
func (s *sharedTestStream) State() *State             { return &State{StreamID: s.id} }
func (s *sharedTestStream) Clone() (Stream, error) {
	return newSharedTestStream(s.id, cap(s.videoCh)), nil
}
func (s *sharedTestStream) WaitForStart(context.Context) error { return nil }
func (s *sharedTestStream) Type() string                       { return "test" }
func (s *sharedTestStream) IsRestartable() bool                { return false }
func (s *sharedTestStream) RestartInterval() time.Duration     { return 0 }
func (s *sharedTestStream) EventChan() chan Event              { return nil }

func TestPushToSidecarsWaitsForConsumer(t *testing.T) {
	oldTimeout := sidecarPushTimeout
	sidecarPushTimeout = 50 * time.Millisecond
	defer func() { sidecarPushTimeout = oldTimeout }()

	sidecar := newSharedTestStream("sidecar", 0)
	frame := &Frame{SequenceID: 1}
	received := make(chan *Frame, 1)

	go func() {
		time.Sleep(20 * time.Millisecond)
		received <- <-sidecar.GetVideoChan()
	}()

	start := time.Now()
	PushToSidecars([]Stream{sidecar}, frame, true)
	elapsed := time.Since(start)

	if elapsed < 15*time.Millisecond {
		t.Fatalf("PushToSidecars returned too early, elapsed=%v", elapsed)
	}

	select {
	case got := <-received:
		if got == frame {
			t.Fatal("sidecar should receive a cloned frame, not the original pointer")
		}
		if got.SequenceID != frame.SequenceID {
			t.Fatalf("unexpected cloned frame content: got seq=%d want %d", got.SequenceID, frame.SequenceID)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected sidecar to receive frame")
	}
}

func TestPushToSidecarsTimesOutWhenBlocked(t *testing.T) {
	oldTimeout := sidecarPushTimeout
	sidecarPushTimeout = 20 * time.Millisecond
	defer func() { sidecarPushTimeout = oldTimeout }()

	sidecar := newSharedTestStream("blocked", 0)
	frame := &Frame{SequenceID: 2}

	start := time.Now()
	PushToSidecars([]Stream{sidecar}, frame, true)
	elapsed := time.Since(start)

	if elapsed < 15*time.Millisecond {
		t.Fatalf("PushToSidecars returned before timeout, elapsed=%v", elapsed)
	}
	if elapsed > 150*time.Millisecond {
		t.Fatalf("PushToSidecars exceeded reasonable timeout window, elapsed=%v", elapsed)
	}

	select {
	case got := <-sidecar.GetVideoChan():
		t.Fatalf("unexpected frame delivered after timeout: %+v", got)
	default:
	}
}

func TestPushToSidecarsClonesFramesPerSidecar(t *testing.T) {
	oldTimeout := sidecarPushTimeout
	sidecarPushTimeout = 100 * time.Millisecond
	defer func() { sidecarPushTimeout = oldTimeout }()

	sidecarA := newSharedTestStream("a", 1)
	sidecarB := newSharedTestStream("b", 1)
	frame := &Frame{
		SequenceID: 3,
		Payload:    [][]byte{{0x01, 0x02, 0x03}},
		VideoSPS:   []byte{0x67, 0x42},
		VideoPPS:   []byte{0x68, 0xce},
	}

	PushToSidecars([]Stream{sidecarA, sidecarB}, frame, true)

	gotA := <-sidecarA.GetVideoChan()
	gotB := <-sidecarB.GetVideoChan()

	if gotA == frame || gotB == frame {
		t.Fatal("sidecars should receive cloned frames, not the original pointer")
	}
	if gotA == gotB {
		t.Fatal("each sidecar should receive its own frame clone")
	}

	gotA.Payload[0][0] = 0x99
	gotA.VideoSPS[0] = 0x55
	gotA.VideoPPS[0] = 0x44

	if frame.Payload[0][0] != 0x01 {
		t.Fatalf("original payload mutated: got %x want 01", frame.Payload[0][0])
	}
	if frame.VideoSPS[0] != 0x67 {
		t.Fatalf("original VideoSPS mutated: got %x want 67", frame.VideoSPS[0])
	}
	if frame.VideoPPS[0] != 0x68 {
		t.Fatalf("original VideoPPS mutated: got %x want 68", frame.VideoPPS[0])
	}
	if gotB.Payload[0][0] != 0x01 {
		t.Fatalf("sidecar clone leaked into other sidecar: got %x want 01", gotB.Payload[0][0])
	}
}
