package raw

import (
	"bytes"
	"testing"

	shared "restreamer/irajstreamer/core/shared"
)

func TestComposeYUV420PBackground(t *testing.T) {
	spec := NewBlackCanvasSpec(4, 4)

	out, err := ComposeYUV420P(spec, nil)
	if err != nil {
		t.Fatalf("ComposeYUV420P() error = %v", err)
	}

	got := out.Frame.Payload[0]
	want := []byte{
		16, 16, 16, 16,
		16, 16, 16, 16,
		16, 16, 16, 16,
		16, 16, 16, 16,
		128, 128, 128, 128,
		128, 128, 128, 128,
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected canvas bytes:\n got %v\nwant %v", got, want)
	}
}

func TestComposeYUV420PPlacement(t *testing.T) {
	src := newRawVideoFrameForTest(
		2,
		2,
		[]byte{
			10, 20,
			30, 40,
		},
		[]byte{90},
		[]byte{140},
	)

	out, err := ComposeYUV420P(
		NewBlackCanvasSpec(4, 4),
		[]VideoPlacement{
			{
				Input: src,
				Layout: VideoLayout{
					X:      2,
					Y:      0,
					Width:  2,
					Height: 2,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("ComposeYUV420P() error = %v", err)
	}

	gotY, gotU, gotV := SplitYUV420P(out.Frame.Payload[0], 4, 4)

	wantY := []byte{
		16, 16, 10, 20,
		16, 16, 30, 40,
		16, 16, 16, 16,
		16, 16, 16, 16,
	}
	wantU := []byte{
		128, 90,
		128, 128,
	}
	wantV := []byte{
		128, 140,
		128, 128,
	}

	if !bytes.Equal(gotY, wantY) {
		t.Fatalf("unexpected Y plane:\n got %v\nwant %v", gotY, wantY)
	}
	if !bytes.Equal(gotU, wantU) {
		t.Fatalf("unexpected U plane:\n got %v\nwant %v", gotU, wantU)
	}
	if !bytes.Equal(gotV, wantV) {
		t.Fatalf("unexpected V plane:\n got %v\nwant %v", gotV, wantV)
	}
}

func TestComposeYUV420PScaling(t *testing.T) {
	src := newRawVideoFrameForTest(
		2,
		2,
		[]byte{
			10, 20,
			30, 40,
		},
		[]byte{100},
		[]byte{150},
	)

	out, err := ComposeYUV420P(
		NewBlackCanvasSpec(4, 4),
		[]VideoPlacement{
			{
				Input: src,
				Layout: VideoLayout{
					X:      0,
					Y:      0,
					Width:  4,
					Height: 4,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("ComposeYUV420P() error = %v", err)
	}

	gotY, gotU, gotV := SplitYUV420P(out.Frame.Payload[0], 4, 4)

	wantY := []byte{
		10, 10, 20, 20,
		10, 10, 20, 20,
		30, 30, 40, 40,
		30, 30, 40, 40,
	}
	wantU := []byte{
		100, 100,
		100, 100,
	}
	wantV := []byte{
		150, 150,
		150, 150,
	}

	if !bytes.Equal(gotY, wantY) {
		t.Fatalf("unexpected Y plane:\n got %v\nwant %v", gotY, wantY)
	}
	if !bytes.Equal(gotU, wantU) {
		t.Fatalf("unexpected U plane:\n got %v\nwant %v", gotU, wantU)
	}
	if !bytes.Equal(gotV, wantV) {
		t.Fatalf("unexpected V plane:\n got %v\nwant %v", gotV, wantV)
	}
}

func TestComposeYUV420PZIndex(t *testing.T) {
	bottom := newRawVideoFrameForTest(
		2,
		2,
		[]byte{
			10, 20,
			30, 40,
		},
		[]byte{90},
		[]byte{140},
	)
	top := newRawVideoFrameForTest(
		2,
		2,
		[]byte{
			50, 60,
			70, 80,
		},
		[]byte{100},
		[]byte{150},
	)

	out, err := ComposeYUV420P(
		NewBlackCanvasSpec(2, 2),
		[]VideoPlacement{
			{
				Input: bottom,
				Layout: VideoLayout{
					X:      0,
					Y:      0,
					Width:  2,
					Height: 2,
					ZIndex: 0,
				},
			},
			{
				Input: top,
				Layout: VideoLayout{
					X:      0,
					Y:      0,
					Width:  2,
					Height: 2,
					ZIndex: 5,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("ComposeYUV420P() error = %v", err)
	}

	gotY, gotU, gotV := SplitYUV420P(out.Frame.Payload[0], 2, 2)

	if want := []byte{50, 60, 70, 80}; !bytes.Equal(gotY, want) {
		t.Fatalf("unexpected Y plane:\n got %v\nwant %v", gotY, want)
	}
	if want := []byte{100}; !bytes.Equal(gotU, want) {
		t.Fatalf("unexpected U plane:\n got %v\nwant %v", gotU, want)
	}
	if want := []byte{150}; !bytes.Equal(gotV, want) {
		t.Fatalf("unexpected V plane:\n got %v\nwant %v", gotV, want)
	}
}

func TestComposeYUV420PTransparency(t *testing.T) {
	src := newRawVideoFrameForTest(
		2,
		2,
		[]byte{
			80, 80,
			80, 80,
		},
		[]byte{64},
		[]byte{192},
	)

	out, err := ComposeYUV420P(
		NewBlackCanvasSpec(2, 2),
		[]VideoPlacement{
			{
				Input: src,
				Layout: VideoLayout{
					X:            0,
					Y:            0,
					Width:        2,
					Height:       2,
					Transparency: 0.5,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("ComposeYUV420P() error = %v", err)
	}

	gotY, gotU, gotV := SplitYUV420P(out.Frame.Payload[0], 2, 2)

	if want := []byte{48, 48, 48, 48}; !bytes.Equal(gotY, want) {
		t.Fatalf("unexpected Y plane:\n got %v\nwant %v", gotY, want)
	}
	if want := []byte{96}; !bytes.Equal(gotU, want) {
		t.Fatalf("unexpected U plane:\n got %v\nwant %v", gotU, want)
	}
	if want := []byte{160}; !bytes.Equal(gotV, want) {
		t.Fatalf("unexpected V plane:\n got %v\nwant %v", gotV, want)
	}
}

func newRawVideoFrameForTest(width, height int, y, u, v []byte) VideoFrame {
	payload := make([]byte, 0, len(y)+len(u)+len(v))
	payload = append(payload, y...)
	payload = append(payload, u...)
	payload = append(payload, v...)

	return VideoFrame{
		Frame: &shared.Frame{
			Payload: [][]byte{payload},
		},
		Width:  width,
		Height: height,
		PixFmt: YUV420PPixFmt,
	}
}
