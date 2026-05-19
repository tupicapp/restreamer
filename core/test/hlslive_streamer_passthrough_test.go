package test

import (
	"context"
	core "github.com/tupicapp/restreamer/core"
	streaminputs "github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
	"path/filepath"
	"testing"
	"time"
)

// TestHLSLiveStreamerPassthrough snapshots a fixed Al Jazeera live window,
// feeds it through the Streamer via NewHLSLive, and verifies the output matches
// the same frozen reference fixture.
func TestHLSLiveStreamerPassthrough(t *testing.T) {
	inputURL, referencePlaylist, cleanup := setupDeterministicLiveFixtureServer(t, 15*time.Second)
	defer cleanup()

	outDir := filepath.Join(t.TempDir(), "output")
	testHLSLiveStreamerPassthroughWithSnapshot(t, inputURL, referencePlaylist, outDir)
}

func testHLSLiveStreamerPassthroughWithSnapshot(t *testing.T, inputURL, referencePlaylist, outDir string) {
	t.Helper()

	outFolder := storage.NewFolder(outDir)
	hlsDest, err := outputs.NewHLSLiveDestination("hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(2*time.Second),
		outputs.WithHLSPlaylistSize(30),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination: %v", err)
	}

	hlsInput := streaminputs.NewHLSLive("hls-input", inputURL)
	hlsInput.Start()

	preCtx, preCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer preCancel()
	if err := hlsInput.WaitForStart(preCtx); err != nil {
		t.Fatalf("pre-start hls live input failed: %v", err)
	}

	streamer := core.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	if err := streamer.UpdateStreams([]core.Stream{hlsInput}, []core.Stream{hlsDest}); err != nil {
		t.Fatalf("UpdateStreams: %v", err)
	}

	streamer.Start()
	if ok := streamer.Switch("hls-input"); !ok {
		t.Fatalf("switch to hls-input failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := hlsDest.WaitForStart(ctx); err != nil {
		t.Fatalf("hls dest failed to start: %v", err)
	}

	lastFrameTime := time.Now()
	noFrameTimeout := 5 * time.Second
	maxPassDuration := 20 * time.Second
	passDeadline := time.Now().Add(maxPassDuration)
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
		if time.Now().After(passDeadline) {
			t.Logf("Reached max pass duration %v, stopping", maxPassDuration)
			break
		}
	}

	streamer.Close()
	time.Sleep(1500 * time.Millisecond)

	destState := hlsDest.State()
	if destState != nil {
		t.Logf("Video frames passed: %d", destState.TotalVideoFrames)
		t.Logf("Audio frames passed: %d", destState.TotalAudioFrames)
	}

	outputPlaylist := filepath.Join(outDir, "stream.m3u8")
	waitForHLSArtifacts(t, outDir, 20*time.Second, 2)
	hlsErr := EqualHLS(referencePlaylist, outputPlaylist)

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

	refVideo, refAudio := collectHLSFrames(t, referencePlaylist, "hlslive-streamer-ref")
	outVideo, outAudio := collectHLSFrames(t, outputPlaylist, "hlslive-streamer-out")

	assertWindowMatches(t, "hlslive-streamer-video", outVideo, refVideo)
	assertWindowMatches(t, "hlslive-streamer-audio", outAudio, refAudio)
}
