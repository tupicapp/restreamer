//go:build cgo && media

package test

import (
	"context"
	"testing"
	"time"

	core "restreamer/core"
	"restreamer/core/decoder"
	"restreamer/core/outputs"
	"restreamer/core/raw"
	"restreamer/core/rawstreamer"
	shared "restreamer/core/shared"
)

func TestStreamer_RawSceneWithFourInputsOneOutput(t *testing.T) {
	inputs := []rawstreamer.Input{
		{
			Stream: newMockRawInput("in-1", quadrantFrame(2, 2, 30, 90, 140)),
			Layout: shared.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2, ZIndex: 0},
		},
		{
			Stream: newMockRawInput("in-2", quadrantFrame(2, 2, 80, 100, 150)),
			Layout: shared.VideoLayout{X: 2, Y: 0, Width: 2, Height: 2, ZIndex: 0},
		},
		{
			Stream: newMockRawInput("in-3", quadrantFrame(2, 2, 140, 110, 160)),
			Layout: shared.VideoLayout{X: 0, Y: 2, Width: 2, Height: 2, ZIndex: 0},
		},
		{
			Stream: newMockRawInput("in-4", quadrantFrame(2, 2, 220, 120, 170)),
			Layout: shared.VideoLayout{X: 2, Y: 2, Width: 2, Height: 2, ZIndex: 0},
		},
	}

	scene, err := rawstreamer.New(
		"scene-4up",
		raw.NewBlackCanvasSpec(4, 4),
		inputs,
		raw.NewComposer,
		rawstreamer.WithStreamType("scene"),
		rawstreamer.WithOutputFPS(5),
	)
	if err != nil {
		t.Fatalf("rawstreamer.New() error = %v", err)
	}
	defer scene.Close()

	streamer := core.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	dest := outputs.NewBuffering("scene-dest")
	if err := streamer.UpdateStreams([]core.Stream{scene}, []core.Stream{dest}); err != nil {
		t.Fatalf("streamer.UpdateStreams() error = %v", err)
	}
	if ok := streamer.Switch(scene.GetID()); !ok {
		t.Fatalf("streamer.Switch(%q) = false", scene.GetID())
	}
	streamer.Start()

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := scene.WaitForStart(waitCtx); err != nil {
		t.Fatalf("scene.WaitForStart() error = %v", err)
	}
	if err := dest.WaitForStart(waitCtx); err != nil {
		t.Fatalf("dest.WaitForStart() error = %v", err)
	}

	var encoded *shared.Frame
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		frames := dest.GetVideoFrames()
		for _, frame := range frames {
			if frame != nil && len(frame.Payload) > 0 {
				encoded = frame
				break
			}
		}
		if encoded != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if encoded == nil {
		t.Fatal("timed out waiting for composed output frame")
	}
	if encoded.InputID != scene.GetID() {
		t.Fatalf("unexpected output input id: got %q want %q", encoded.InputID, scene.GetID())
	}
	if encoded.Codec != "h264" {
		t.Fatalf("unexpected output codec: got %q want %q", encoded.Codec, "h264")
	}

	decoded := decodeVideoFrame(t, encoded)
	if decoded.Width != 4 || decoded.Height != 4 {
		t.Fatalf("unexpected decoded size: got %dx%d want 4x4", decoded.Width, decoded.Height)
	}

	yPlane, _, _ := raw.SplitYUV420P(decoded.Frame.Payload[0], decoded.Width, decoded.Height)
	topLeft := averageBlock(yPlane, decoded.Width, 0, 0, 2, 2)
	topRight := averageBlock(yPlane, decoded.Width, 2, 0, 2, 2)
	bottomLeft := averageBlock(yPlane, decoded.Width, 0, 2, 2, 2)
	bottomRight := averageBlock(yPlane, decoded.Width, 2, 2, 2, 2)

	if !(topLeft < topRight && topRight < bottomLeft && bottomLeft < bottomRight) {
		t.Fatalf(
			"unexpected quadrant luminance ordering: tl=%d tr=%d bl=%d br=%d",
			topLeft,
			topRight,
			bottomLeft,
			bottomRight,
		)
	}
	if bottomRight-topLeft < 80 {
		t.Fatalf("quadrant separation too small after encode/decode: tl=%d br=%d", topLeft, bottomRight)
	}
}

type mockRawInput struct {
	id      string
	frame   *shared.Frame
	videoCh chan *shared.Frame
	audioCh chan *shared.Frame
	started chan struct{}
	events  chan shared.Event
}

func newMockRawInput(id string, frame *shared.Frame) *mockRawInput {
	return &mockRawInput{
		id:      id,
		frame:   frame,
		videoCh: make(chan *shared.Frame, 1),
		audioCh: make(chan *shared.Frame),
		started: make(chan struct{}),
		events:  make(chan shared.Event, 1),
	}
}

func (m *mockRawInput) GetVideoChan() chan *shared.Frame { return m.videoCh }
func (m *mockRawInput) GetAudioChan() chan *shared.Frame { return m.audioCh }
func (m *mockRawInput) GetID() string                    { return m.id }
func (m *mockRawInput) Type() string                     { return "mock_raw" }
func (m *mockRawInput) IsRestartable() bool              { return false }
func (m *mockRawInput) RestartInterval() time.Duration   { return 0 }
func (m *mockRawInput) EventChan() chan shared.Event     { return m.events }
func (m *mockRawInput) Stop()                            {}
func (m *mockRawInput) Close()                           {}

func (m *mockRawInput) Start() {
	select {
	case <-m.started:
		return
	default:
		m.videoCh <- cloneFrame(m.frame)
		close(m.started)
	}
}

func (m *mockRawInput) State() *shared.State {
	return &shared.State{
		IsStarted: true,
		StreamID:  m.id,
		Type:      m.Type(),
	}
}

func (m *mockRawInput) Clone() (shared.Stream, error) {
	return newMockRawInput(m.id, cloneFrame(m.frame)), nil
}

func (m *mockRawInput) WaitForStart(ctx context.Context) error {
	select {
	case <-m.started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func quadrantFrame(width, height int, yValue, uValue, vValue byte) *shared.Frame {
	ySize := width * height
	uvSize := (width / 2) * (height / 2)
	payload := make([]byte, 0, ySize+2*uvSize)
	for range ySize {
		payload = append(payload, yValue)
	}
	for range uvSize {
		payload = append(payload, uValue)
	}
	for range uvSize {
		payload = append(payload, vValue)
	}

	return &shared.Frame{
		Payload:    [][]byte{payload},
		Codec:      raw.VideoCodec,
		PacketType: raw.YUV420PPixFmt,
		Timestamp:  time.Now(),
	}
}

func decodeVideoFrame(t *testing.T, frame *shared.Frame) *raw.VideoFrame {
	t.Helper()

	input := make(chan *shared.Frame, 1)
	videoDecoder, err := decoder.NewH264Decoder("scene-verify", input)
	if err != nil {
		t.Fatalf("decoder.NewH264Decoder() error = %v", err)
	}
	defer videoDecoder.Close()

	if err := videoDecoder.Start(); err != nil {
		t.Fatalf("videoDecoder.Start() error = %v", err)
	}

	input <- frame
	close(input)

	select {
	case decoded := <-videoDecoder.Output():
		if decoded == nil {
			t.Fatal("decoded frame is nil")
		}
		return decoded
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for decoded output frame")
		return nil
	}
}

func averageBlock(yPlane []byte, stride, startX, startY, width, height int) int {
	total := 0
	samples := 0
	for y := startY; y < startY+height; y++ {
		for x := startX; x < startX+width; x++ {
			total += int(yPlane[y*stride+x])
			samples++
		}
	}
	if samples == 0 {
		return 0
	}
	return total / samples
}

func cloneFrame(frame *shared.Frame) *shared.Frame {
	if frame == nil {
		return nil
	}
	out := *frame
	if len(frame.Payload) > 0 {
		out.Payload = make([][]byte, 0, len(frame.Payload))
		for _, payload := range frame.Payload {
			out.Payload = append(out.Payload, append([]byte(nil), payload...))
		}
	}
	return &out
}
