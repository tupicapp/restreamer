//go:build cgo && media

package raw_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"restreamer/core/decoder"
	"restreamer/core/inputs"
	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

func TestMergeTwoRTMPReadersToRawVideo(t *testing.T) {
	url1 := getenvOrDefault("MERGE_RTMP_URL_1", "rtmp://localhost:1938/live/1")
	url2 := getenvOrDefault("MERGE_RTMP_URL_2", "rtmp://localhost:1938/live/1")

	const (
		outputWidth   = 1280
		outputHeight  = 720
		outputFPS     = 25
		outputFrames  = 250
		baseWidth     = 1280
		baseHeight    = 720
		overlayWidth  = 416
		overlayHeight = 234
	)

	reader1 := inputs.NewRTMP("merge-rtmp-1", url1)
	reader2 := inputs.NewRTMP("merge-rtmp-2", url2)
	reader1.Start()
	reader2.Start()
	defer reader1.Close()
	defer reader2.Close()

	if err := waitForReaderStart(reader1, 10*time.Second); err != nil {
		t.Skipf("RTMP source 1 not available: %v", err)
	}
	if err := waitForReaderStart(reader2, 10*time.Second); err != nil {
		t.Skipf("RTMP source 2 not available: %v", err)
	}

	input1 := make(chan *shared.Frame, 128)
	input2 := make(chan *shared.Frame, 128)

	decoder1, err := decoder.NewH264Decoder(
		"merge-decoder-1",
		input1,
		decoder.WithH264DecoderOutputResolution(baseWidth, baseHeight),
	)
	if err != nil {
		t.Fatalf("NewH264Decoder(decoder1) error = %v", err)
	}
	decoder2, err := decoder.NewH264Decoder(
		"merge-decoder-2",
		input2,
		decoder.WithH264DecoderOutputResolution(overlayWidth, overlayHeight),
	)
	if err != nil {
		t.Fatalf("NewH264Decoder(decoder2) error = %v", err)
	}
	defer decoder1.Close()
	defer decoder2.Close()

	if err := decoder1.Start(); err != nil {
		t.Fatalf("decoder1 Start() error = %v", err)
	}
	if err := decoder2.Start(); err != nil {
		t.Fatalf("decoder2 Start() error = %v", err)
	}

	forwardDone := make(chan struct{}, 2)
	go forwardReaderVideoToDecoderInput(reader1.GetVideoChan(), input1, forwardDone)
	go forwardReaderVideoToDecoderInput(reader2.GetVideoChan(), input2, forwardDone)

	slot1 := &rawFrameSlot{}
	slot2 := &rawFrameSlot{}
	errCh := make(chan error, 8)

	go collectDecodedFrames(decoder1, slot1, errCh, "decoder1")
	go collectDecodedFrames(decoder2, slot2, errCh, "decoder2")

	if err := waitForDecodedFrames(slot1, slot2, 50*time.Second); err != nil {
		t.Fatalf("waiting for decoded frames failed: %v", err)
	}

	layout1 := raw.VideoLayout{
		X:      0,
		Y:      0,
		Width:  baseWidth,
		Height: baseHeight,
		ZIndex: 0,
	}
	layout2 := raw.VideoLayout{
		X:            824,
		Y:            40,
		Width:        overlayWidth,
		Height:       overlayHeight,
		ZIndex:       10,
		Transparency: 0.5,
	}
	canvas := raw.NewBlackCanvasSpec(outputWidth, outputHeight)

	outputDir := filepath.Join("..", "..", "..", "testdata", "output", "raw", sanitizeMergeTestName(t.Name()))
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}

	rawPath := filepath.Join(outputDir, "merged.yuv")
	rawFile, err := os.Create(rawPath)
	if err != nil {
		t.Fatalf("failed to create raw output file: %v", err)
	}
	defer rawFile.Close()

	y4mPath := filepath.Join(outputDir, "merged.y4m")
	y4mFile, err := os.Create(y4mPath)
	if err != nil {
		t.Fatalf("failed to create y4m output file: %v", err)
	}
	defer y4mFile.Close()

	if err := writeY4MHeader(y4mFile, outputWidth, outputHeight, outputFPS); err != nil {
		t.Fatalf("failed to write y4m header: %v", err)
	}

	ticker := time.NewTicker(time.Second / outputFPS)
	defer ticker.Stop()

	written := 0
	writeDeadline := time.After(60 * time.Second)

	for written < outputFrames {
		select {
		case <-writeDeadline:
			t.Fatalf("timed out while writing merged output, wrote %d frames", written)
		case err := <-errCh:
			if err != nil {
				t.Fatalf("decoder pipeline error: %v", err)
			}
		case <-ticker.C:
			frame1, _, ok1 := slot1.Snapshot()
			frame2, _, ok2 := slot2.Snapshot()
			if !ok1 || !ok2 {
				continue
			}

			merged, err := raw.ComposeYUV420P(
				canvas,
				[]raw.VideoPlacement{
					{Input: *frame1, Layout: layout1},
					{Input: *frame2, Layout: layout2},
				},
			)
			if err != nil {
				t.Fatalf("ComposeYUV420P() error = %v", err)
			}

			if _, err := rawFile.Write(merged.Frame.Payload[0]); err != nil {
				t.Fatalf("failed to write merged frame: %v", err)
			}
			if err := writeY4MFrame(y4mFile, merged.Frame.Payload[0]); err != nil {
				t.Fatalf("failed to write merged y4m frame: %v", err)
			}

			written++
		}
	}

	readmePath := filepath.Join(outputDir, "README.txt")
	readme := fmt.Sprintf(
		"Video written by %s\n\nPlayable video: %s\nRaw dump: %s\nPixel format: %s\nSize: %dx%d\nFPS: %d\nFrames: %d\n\nLayout:\n- stream 1: base layer, z-index 0, opaque, %dx%d at (0,0)\n- stream 2: overlay, z-index 10, transparency 0.20, %dx%d at (%d,%d)\n\nOpen the easier file with:\nffplay %s\n\nOr, if you want the raw dump:\nffplay -f rawvideo -pixel_format %s -video_size %dx%d -framerate %d %s\n",
		t.Name(),
		y4mPath,
		rawPath,
		raw.YUV420PPixFmt,
		outputWidth,
		outputHeight,
		outputFPS,
		outputFrames,
		baseWidth,
		baseHeight,
		overlayWidth,
		overlayHeight,
		layout2.X,
		layout2.Y,
		y4mPath,
		raw.YUV420PPixFmt,
		outputWidth,
		outputHeight,
		outputFPS,
		rawPath,
	)
	if err := os.WriteFile(readmePath, []byte(readme), 0o644); err != nil {
		t.Fatalf("failed to write README: %v", err)
	}

	t.Logf("merged y4m video written to %s", y4mPath)
	t.Logf("merged raw video written to %s", rawPath)
	t.Logf("open instructions written to %s", readmePath)
}

type rawFrameSlot struct {
	mu    sync.RWMutex
	frame *raw.VideoFrame
	count int
}

func (s *rawFrameSlot) Set(frame *raw.VideoFrame) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.frame = frame
	s.count++
}

func (s *rawFrameSlot) Snapshot() (*raw.VideoFrame, int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.frame == nil {
		return nil, 0, false
	}

	frameCopy := *s.frame
	return &frameCopy, s.count, true
}

func waitForDecodedFrames(slot1, slot2 *rawFrameSlot, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for both decoders to emit frames")
		case <-ticker.C:
			if _, _, ok := slot1.Snapshot(); !ok {
				continue
			}
			if _, _, ok := slot2.Snapshot(); !ok {
				continue
			}
			return nil
		}
	}
}

func collectDecodedFrames(videoDecoder decoder.VideoDecoder, slot *rawFrameSlot, errCh chan<- error, label string) {
	for {
		select {
		case err, ok := <-videoDecoder.Errors():
			if !ok {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("%s: %w", label, err)
				return
			}
		case frame, ok := <-videoDecoder.Output():
			if !ok {
				return
			}
			if frame != nil {
				slot.Set(frame)
			}
		}
	}
}

func forwardReaderVideoToDecoderInput(src <-chan *shared.Frame, dst chan<- *shared.Frame, done chan<- struct{}) {
	defer close(dst)
	defer func() {
		done <- struct{}{}
	}()

	seenKeyFrame := false

	for frame := range src {
		if frame == nil {
			continue
		}
		if frame.Codec != "" && frame.Codec != "h264" {
			continue
		}
		if !seenKeyFrame {
			if !frame.IsKeyFrame {
				continue
			}
			seenKeyFrame = true
		}

		dst <- cloneSharedFrameForMergeTest(frame)
	}
}

func cloneSharedFrameForMergeTest(src *shared.Frame) *shared.Frame {
	if src == nil {
		return nil
	}

	payload := make([][]byte, len(src.Payload))
	for i, nalu := range src.Payload {
		payload[i] = append([]byte(nil), nalu...)
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

func waitForReaderStart(reader shared.Stream, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return reader.WaitForStart(ctx)
}

func getenvOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func sanitizeMergeTestName(name string) string {
	replacer := strings.NewReplacer("/", "_", ":", "_", "\\", "_", " ", "_")
	return replacer.Replace(name)
}

func writeY4MHeader(file *os.File, width, height, fps int) error {
	header := fmt.Sprintf("YUV4MPEG2 W%d H%d F%d:1 Ip A1:1 C420jpeg\n", width, height, fps)
	_, err := file.WriteString(header)
	return err
}

func writeY4MFrame(file *os.File, frame []byte) error {
	if _, err := file.WriteString("FRAME\n"); err != nil {
		return err
	}
	_, err := file.Write(frame)
	return err
}
