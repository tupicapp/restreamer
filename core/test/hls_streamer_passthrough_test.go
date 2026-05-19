package test

import (
	"context"
	"os"
	"testing"
	"time"

	core "github.com/tupicapp/restreamer/core"
	streaminputs "github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
)

// TestHLSStreamerPassthrough tests HLS input → Streamer → HLS output passthrough,
// and verifies the output playlist matches the input playlist using ffprobe.
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

	// Monitor for no frames timeout
	lastFrameTime := time.Now()
	noFrameTimeout := 2 * time.Second
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		inputState := hlsInput.State()
		if inputState != nil && inputState.LastIO.After(lastFrameTime) {
			lastFrameTime = inputState.LastIO
		}

		destState := hlsDest.State()
		if destState != nil && destState.LastIO.After(lastFrameTime) {
			lastFrameTime = destState.LastIO
		}

		if time.Since(lastFrameTime) > noFrameTimeout {
			t.Logf("No frames for %v, stopping", noFrameTimeout)
			break
		}
	}

	streamer.Close()

	// Wait for writes to complete
	time.Sleep(1500 * time.Millisecond)

	destState := hlsDest.State()
	if destState != nil {
		t.Logf("Video frames passed: %d", destState.TotalVideoFrames)
		t.Logf("Audio frames passed: %d", destState.TotalAudioFrames)
	}

	inputPlaylist := miladNobURL
	outputPlaylist := outDir + "stream.m3u8"

	hlsErr := EqualHLS(inputPlaylist, outputPlaylist)

	if hlsErr.ProbeError1 != nil {
		t.Errorf("ffprobe failed on input playlist: %v", hlsErr.ProbeError1)
	}
	if hlsErr.ProbeError2 != nil {
		t.Errorf("ffprobe failed on output playlist: %v", hlsErr.ProbeError2)
	}

	if hlsErr.StreamCountMismatch {
		t.Errorf("Stream count mismatch: input=%d, output=%d",
			hlsErr.StreamCount1, hlsErr.StreamCount2)
	}
	for _, sd := range hlsErr.StreamDiffs {
		t.Errorf("Stream[%d] %s mismatch: input=%q output=%q",
			sd.Index, sd.Field, sd.Value1, sd.Value2)
	}

	if hlsErr.PacketCountMismatch {
		t.Errorf("Packet count mismatch: video input=%d output=%d, audio input=%d output=%d",
			hlsErr.VideoPacketCount1, hlsErr.VideoPacketCount2,
			hlsErr.AudioPacketCount1, hlsErr.AudioPacketCount2)
	}

	countErrors := 0
	errorPrints := 100
	for _, pd := range hlsErr.PacketDiffs {
		countErrors++
		if pd.Field == "data" {
			t.Errorf("Packet[%s #%d] %s mismatch",
				pd.CodecType, pd.Index, pd.Field)
			continue
		}

		t.Errorf("Packet[%s #%d] %s mismatch: input=%q output=%q",
			pd.CodecType, pd.Index, pd.Field, pd.Value1, pd.Value2)
		if countErrors >= errorPrints {
			break
		}
	}
}
