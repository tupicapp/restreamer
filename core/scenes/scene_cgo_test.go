//go:build cgo && media

package scenes

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tupicapp/restreamer/core/decoder"
	"github.com/tupicapp/restreamer/core/raw"
	shared "github.com/tupicapp/restreamer/core/shared"
)

func TestScene_ComposesRawInputsAndEncodesVideo(t *testing.T) {
	base := newMockRawStream("base", raw.VideoFrame{
		Frame: &shared.Frame{
			Payload:    [][]byte{append([]byte{}, 10, 20, 30, 40, 90, 140)},
			Codec:      raw.VideoCodec,
			PacketType: raw.YUV420PPixFmt,
			Timestamp:  time.Now(),
		},
		Width:  2,
		Height: 2,
		PixFmt: raw.YUV420PPixFmt,
	})
	overlay := newMockRawStream("overlay", raw.VideoFrame{
		Frame: &shared.Frame{
			Payload:    [][]byte{append([]byte{}, 50, 60, 70, 80, 100, 150)},
			Codec:      raw.VideoCodec,
			PacketType: raw.YUV420PPixFmt,
			Timestamp:  time.Now(),
		},
		Width:  2,
		Height: 2,
		PixFmt: raw.YUV420PPixFmt,
	})

	scene, err := NewScene("scene-test", raw.NewBlackCanvasSpec(2, 2), []Input{
		{Stream: base, Layout: raw.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2, ZIndex: 0}},
		{Stream: overlay, Layout: raw.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2, ZIndex: 1}},
	}, WithOutputFPS(5))
	if err != nil {
		t.Fatalf("NewScene() error = %v", err)
	}
	defer scene.Close()

	scene.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := scene.WaitForStart(ctx); err != nil {
		t.Fatalf("WaitForStart() error = %v", err)
	}

	var encoded *shared.Frame
	select {
	case encoded = <-scene.GetVideoChan():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for encoded scene output")
	}

	if encoded == nil {
		t.Fatal("scene output frame is nil")
	}
	if encoded.Codec != "h264" {
		t.Fatalf("unexpected output codec: got %q want %q", encoded.Codec, "h264")
	}
	if len(encoded.Payload) == 0 {
		t.Fatal("encoded frame payload is empty")
	}

	decodeIn := make(chan *shared.Frame, 1)
	videoDecoder, err := decoder.NewH264Decoder("scene-verify", decodeIn)
	if err != nil {
		t.Fatalf("NewH264Decoder() error = %v", err)
	}
	defer videoDecoder.Close()

	if err := videoDecoder.Start(); err != nil {
		t.Fatalf("decoder.Start() error = %v", err)
	}

	decodeIn <- encoded
	close(decodeIn)

	select {
	case got := <-videoDecoder.Output():
		if got == nil {
			t.Fatal("decoded scene frame is nil")
		}
		if got.Width != 2 || got.Height != 2 {
			t.Fatalf("unexpected decoded scene size: got %dx%d want 2x2", got.Width, got.Height)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for decoded scene frame")
	}
}

type mockRawStream struct {
	id      string
	video   raw.VideoFrame
	videoCh chan *shared.Frame
	audioCh chan *shared.Frame
	started chan struct{}
	events  chan shared.Event
	once    sync.Once
}

func newMockRawStream(id string, frame raw.VideoFrame) *mockRawStream {
	return &mockRawStream{
		id:      id,
		video:   frame,
		videoCh: make(chan *shared.Frame, 2),
		audioCh: make(chan *shared.Frame),
		started: make(chan struct{}),
		events:  make(chan shared.Event, 8),
	}
}

func (m *mockRawStream) GetVideoChan() chan *shared.Frame { return m.videoCh }
func (m *mockRawStream) GetAudioChan() chan *shared.Frame { return m.audioCh }
func (m *mockRawStream) GetID() string                    { return m.id }
func (m *mockRawStream) EventChan() chan shared.Event     { return m.events }
func (m *mockRawStream) Type() string                     { return "raw" }
func (m *mockRawStream) IsRestartable() bool              { return false }
func (m *mockRawStream) RestartInterval() time.Duration   { return 0 }
func (m *mockRawStream) Stop()                            {}
func (m *mockRawStream) Close()                           {}

func (m *mockRawStream) Start() {
	m.once.Do(func() {
		m.videoCh <- m.video.Frame
		close(m.started)
	})
}

func (m *mockRawStream) State() *shared.State {
	return &shared.State{
		IsStarted: true,
		StreamID:  m.id,
		Type:      raw.VideoCodec,
	}
}

func (m *mockRawStream) Clone() (shared.Stream, error) {
	return newMockRawStream(m.id, m.video), nil
}

func (m *mockRawStream) WaitForStart(ctx context.Context) error {
	select {
	case <-m.started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
