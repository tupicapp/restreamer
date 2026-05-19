package test

import (
	"context"
	"os"
	"testing"
	"time"

	core "restreamer/core"
	streaminputs "restreamer/core/inputs"
	"restreamer/core/outputs"
	"restreamer/core/storage"
)

// TestHLSStreamerPassthrough tests HLS input → Streamer → HLS output passthrough,
// comparing frame counts from ffprobe.
func TestHLSStreamerPassthrough(t *testing.T) {
	requireHTTPReachable(t, miladNobURL, 5*time.Second)

	outDir := "./testdata_streamer/"
	os.RemoveAll(outDir)
	os.MkdirAll(outDir, 0755)
	defer os.RemoveAll(outDir)

	outFolder := storage.NewFolder(outDir)

	// Create HLS destination
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
	hlsInput := streaminputs.NewHLSLive("hls-input", miladNobURL)

	// Create streamer
	streamer := core.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	// Update streamer with input and output
	if err := streamer.UpdateStreams([]core.Stream{hlsInput}, []core.Stream{hlsDest}); err != nil {
		t.Fatalf("UpdateStreams: %v", err)
	}

	// Start streamer
	streamer.Start()
	streamer.Switch("hls-input")

	// Wait for both to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := hlsInput.(interface{ WaitForStart(context.Context) error }).WaitForStart(ctx); err != nil {
		t.Fatalf("hls input failed to start: %v", err)
	}
	if err := hlsDest.WaitForStart(ctx); err != nil {
		t.Fatalf("hls dest failed to start: %v", err)
	}

	t.Log("HLS input and output started, collecting frames for 2s of inactivity...")

	// Monitor for no frames timeout
	lastFrameTime := time.Now()
	noFrameTimeout := 2 * time.Second
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		// Check input state for activity (this is a simple check)
		state := hlsInput.(interface{ State() *streaminputs.State }).State()
		if state.LastIO.After(lastFrameTime) {
			lastFrameTime = state.LastIO
		}

		if time.Since(lastFrameTime) > noFrameTimeout {
			t.Logf("No frames for %v, stopping", noFrameTimeout)
			break
		}
	}

	t.Log("Closing streams...")
	streamer.Close()

	// Wait for writes to complete
	time.Sleep(1 * time.Second)

	// Get frame count from input using simple ffprobe
	inputPlaylist := miladNobURL
	inputCount, err := getFFprobeFrameCount(inputPlaylist)
	if err != nil {
		t.Logf("Warning: could not get input frame count: %v", err)
		return
	}

	// Get frame count from output using simple ffprobe
	outputPlaylist := outDir + "stream.m3u8"
	outputCount, err := getFFprobeFrameCount(outputPlaylist)
	if err != nil {
		t.Logf("Warning: could not get output frame count: %v", err)
		return
	}

	t.Logf("Input ffprobe:  %d frames", inputCount)
	t.Logf("Output ffprobe: %d frames", outputCount)

	// Compare frame counts
	maxDiffPercent := 10.0
	diff := inputCount - outputCount
	if diff < 0 {
		diff = -diff
	}
	tolerance := inputCount * int(maxDiffPercent) / 100

	if diff > tolerance {
		t.Errorf("Frame count mismatch: input=%d, output=%d, diff=%d (tolerance=%d, %.1f%%)",
			inputCount, outputCount, diff, tolerance, maxDiffPercent)
	} else {
		t.Logf("✓ Frame counts match (within %.1f%%)", maxDiffPercent)
	}

	// Calculate percentage match
	percentMatch := float64(outputCount) / float64(inputCount) * 100.0
	t.Logf("Output is %.1f%% of input", percentMatch)
}
