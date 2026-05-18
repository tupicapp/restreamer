//go:build cgo && media

package decoder

import (
	"context"
	"testing"
	"time"

	"restreamer/core/inputs"
	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

func TestH264Decoder_DecodesRTMPReaderPackets(t *testing.T) {
	videoFrames := collectVideoFramesForDecoderTest(t, 8)
	if len(videoFrames) == 0 {
		t.Fatalf("expected test source to provide H264 frames")
	}

	width, height, ok := firstInferableResolution(videoFrames)
	if !ok {
		t.Fatalf("failed to infer source resolution from H264 SPS")
	}

	inputCh := make(chan *shared.Frame, len(videoFrames))
	videoDecoder, err := NewH264Decoder("decoder-test", inputCh)
	if err != nil {
		t.Fatalf("NewH264Decoder() error = %v", err)
	}
	defer videoDecoder.Close()

	if err := videoDecoder.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	for _, frame := range videoFrames {
		inputCh <- frame
	}
	close(inputCh)

	select {
	case err := <-videoDecoder.Errors():
		if err != nil {
			t.Fatalf("decoder reported error: %v", err)
		}
	default:
	}

	var got *raw.VideoFrame
	select {
	case got = <-videoDecoder.Output():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for decoded frame")
	}

	if got == nil {
		t.Fatal("decoder output closed without producing a frame")
	}

	if got.Width != width || got.Height != height {
		t.Fatalf("unexpected decoded size: got %dx%d want %dx%d", got.Width, got.Height, width, height)
	}
	if got.PixFmt != raw.YUV420PPixFmt {
		t.Fatalf("unexpected pixel format: got %q want %q", got.PixFmt, raw.YUV420PPixFmt)
	}

	expectedSize, err := raw.ExpectedYUV420PSize(width, height)
	if err != nil {
		t.Fatalf("ExpectedYUV420PSize() error = %v", err)
	}
	if len(got.Frame.Payload) != 1 {
		t.Fatalf("expected one raw payload plane, got %d", len(got.Frame.Payload))
	}
	if len(got.Frame.Payload[0]) != expectedSize {
		t.Fatalf("unexpected raw frame size: got %d want %d", len(got.Frame.Payload[0]), expectedSize)
	}
}

func collectVideoFramesForDecoderTest(t *testing.T, want int) []*shared.Frame {
	t.Helper()

	const rtmpURL = "rtmp://localhost:1938/live/1"

	reader := inputs.NewRTMP("decoder-test-rtmp", rtmpURL)
	reader.Start()
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := reader.WaitForStart(ctx); err != nil {
		t.Skipf("RTMP source not available for decoder test: %v", err)
	}

	frames := make([]*shared.Frame, 0, want)
	timeout := time.After(10 * time.Second)
	hasInferableResolution := false
	seenKeyFrame := false

	for len(frames) < want || !hasInferableResolution {
		select {
		case <-timeout:
			t.Fatalf("timed out while reading RTMP source, collected %d frames", len(frames))
		case frame, ok := <-reader.GetVideoChan():
			if !ok {
				if len(frames) == 0 {
					t.Fatalf("RTMP reader closed before emitting video frames")
				}
				if !hasInferableResolution {
					t.Fatalf("RTMP reader emitted %d frames but none included SPS for resolution inference", len(frames))
				}
				return frames
			}
			if frame == nil {
				continue
			}
			if !seenKeyFrame {
				if !frame.IsKeyFrame {
					continue
				}
				seenKeyFrame = true
			}

			cloned := cloneFrameForDecoderTest(frame)
			frames = append(frames, cloned)
			if !hasInferableResolution {
				_, _, hasInferableResolution = InferResolutionFromH264Frame(cloned)
			}
		}
	}

	return frames
}

func firstInferableResolution(frames []*shared.Frame) (int, int, bool) {
	for _, frame := range frames {
		if width, height, ok := InferResolutionFromH264Frame(frame); ok {
			return width, height, true
		}
	}
	return 0, 0, false
}

func cloneFrameForDecoderTest(src *shared.Frame) *shared.Frame {
	if src == nil {
		return nil
	}

	payload := make([][]byte, len(src.Payload))
	for i, nalu := range src.Payload {
		payload[i] = cloneBytesForDecoderTest(nalu)
	}

	return &shared.Frame{
		PTS:        src.PTS,
		DTS:        src.DTS,
		Duration:   src.Duration,
		Payload:    payload,
		Codec:      src.Codec,
		PacketType: src.PacketType,
		Timestamp:  src.Timestamp,
		InputID:    src.InputID,
		IsKeyFrame: src.IsKeyFrame,
		SequenceID: src.SequenceID,
		GOPID:      src.GOPID,
		IsFile:     src.IsFile,
	}
}

func cloneBytesForDecoderTest(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
