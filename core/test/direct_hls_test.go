package test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	streaminputs "restreamer/core/inputs"
	"restreamer/core/outputs"
	"restreamer/core/storage"
)

// TestDirectHLSPassthrough streams directly from HLS input to HLS output without Streamer,
// and verifies frame count matches using ffprobe.
func TestDirectHLSPassthrough(t *testing.T) {
	testCases := []struct {
		name string
		url  string
	}{
		// {"miladNob", miladNobURL},
		{"aljazeera", aljaziraURL},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testDirectHLSPassthroughWithURL(t, tc.url)
		})
	}
}

func testDirectHLSPassthroughWithURL(t *testing.T, sourceURL string) {
	requireHTTPReachable(t, sourceURL, 5*time.Second)

	outDir := "./testdata_direct/"
	os.RemoveAll(outDir)
	os.MkdirAll(outDir, 0755)
	defer os.RemoveAll(outDir)

	outFolder := storage.NewFolder(outDir)

	// Create HLS output destination
	hlsDest, err := outputs.NewHLSLiveDestination("hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(2*time.Second),
		outputs.WithHLSPlaylistSize(30),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination: %v", err)
	}

	// Create HLS input
	hlsInput := streaminputs.NewHLSLive("hls-input", sourceURL)

	// Start both input and output
	hlsInput.Start()
	hlsDest.Start()

	// Wait for both to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := hlsInput.(interface{ WaitForStart(context.Context) error }).WaitForStart(ctx); err != nil {
		t.Fatalf("hls input failed to start: %v", err)
	}
	if err := hlsDest.WaitForStart(ctx); err != nil {
		t.Fatalf("hls dest failed to start: %v", err)
	}

	// Get channels
	videoIn := hlsInput.GetVideoChan()
	audioIn := hlsInput.GetAudioChan()
	videoOut := hlsDest.GetVideoChan()
	audioOut := hlsDest.GetAudioChan()

	stopChan := make(chan struct{})
	var videoCount, audioCount int64
	lastFrameTime := time.Now()
	noFrameTimeout := 2 * time.Second

	// Goroutine 1: read video frames and pass to output
	go func() {
		for {
			select {
			case <-stopChan:
				return
			case frame := <-videoIn:
				if frame == nil {
					return
				}
				videoCount++
				lastFrameTime = time.Now()
				select {
				case videoOut <- frame:
				case <-stopChan:
					return
				}
			}
		}
	}()

	// Goroutine 2: read audio frames and pass to output
	go func() {
		for {
			select {
			case <-stopChan:
				return
			case frame := <-audioIn:
				if frame == nil {
					return
				}
				audioCount++
				select {
				case audioOut <- frame:
				case <-stopChan:
					return
				}
			}
		}
	}()

	// Monitor for no frames timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if time.Since(lastFrameTime) > noFrameTimeout {
			t.Logf("No frames for %v, stopping", noFrameTimeout)
			break
		}
	}

	close(stopChan)
	time.Sleep(500 * time.Millisecond)

	// Close everything
	hlsInput.Close()
	hlsDest.Close()

	// Wait for writes to complete
	time.Sleep(1 * time.Second)

	t.Logf("Video frames passed: %d", videoCount)
	t.Logf("Audio frames passed: %d", audioCount)

	// Get frame count from input using ffprobe
	inputPlaylist := sourceURL
	inputFrameCount, err := getFFprobeFrameCount(inputPlaylist)
	if err != nil {
		t.Logf("Warning: could not get input frame count: %v", err)
		return
	}

	// Get frame count from output using ffprobe
	outputPlaylist := outDir + "stream.m3u8"
	outputFrameCount, err := getFFprobeFrameCount(outputPlaylist)
	if err != nil {
		t.Logf("Warning: could not get output frame count: %v", err)
		return
	}

	t.Logf("Input ffprobe:  %d frames", inputFrameCount)
	t.Logf("Output ffprobe: %d frames", outputFrameCount)

	// Allow small difference (up to 10% for live streaming variance)
	maxDiffPercent := 10.0
	diff := float64(inputFrameCount - outputFrameCount)
	if diff < 0 {
		diff = -diff
	}
	tolerance := float64(inputFrameCount) * maxDiffPercent / 100.0

	if diff > tolerance {
		t.Errorf("Frame count mismatch: input=%d, output=%d, diff=%.0f, tolerance=%.0f (%.1f%%)",
			inputFrameCount, outputFrameCount, diff, tolerance, (diff/float64(inputFrameCount))*100)
	} else {
		t.Logf("✓ Frame counts match (within %.1f%%)", maxDiffPercent)
	}
}

// getFFprobeFrameCount returns the total number of video frames in an HLS playlist using ffprobe
func getFFprobeFrameCount(playlistURL string) (int, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-count_frames",
		"-show_entries", "stream=nb_read_frames",
		"-of", "csv=p=0",
		playlistURL,
	)

	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("ffprobe failed: %w", err)
	}

	// ffprobe may output multiple lines, take the first non-empty one
	lines := strings.Split(string(output), "\n")
	var countStr string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			countStr = line
			break
		}
	}

	if countStr == "" {
		return 0, fmt.Errorf("ffprobe returned empty output")
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		return 0, fmt.Errorf("could not parse frame count '%s': %w", countStr, err)
	}

	return count, nil
}
