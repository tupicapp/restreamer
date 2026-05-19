package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	core "restreamer/core"
	streaminputs "restreamer/core/inputs"
	"restreamer/core/outputs"
	"restreamer/core/storage"
	"testing"
	"time"
)

// TestHLSLiveStreamerPassthrough snapshots a fixed Al Jazeera live window,
// feeds it through the Streamer via NewHLSLive, and verifies the output matches
// the same frozen reference fixture.
func TestHLSLiveStreamerPassthrough(t *testing.T) {
	liveSourceURL := getMixedTestLiveURL()
	requireOrSkipHTTPReachable(t, liveSourceURL, 20*time.Second)

	workDir := "./tests_streamer_live"
	snapshotDir := filepath.Join(workDir, "snapshot")
	normalizedDir := filepath.Join(workDir, "normalized")
	outDir := filepath.Join(workDir, "output")
	_ = os.RemoveAll(workDir)
	for _, dir := range []string{snapshotDir, normalizedDir, outDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	defer os.RemoveAll(workDir)

	snapshotPath := snapshotLiveHLSFixture(t, liveSourceURL, 8*time.Second, snapshotDir)
	makeHLSFixture(t, snapshotPath, 0, 15*time.Second, normalizedDir)
	referencePlaylist := filepath.Join(normalizedDir, "stream.m3u8")

	fileServer := httptest.NewServer(http.FileServer(http.Dir(workDir)))
	defer fileServer.Close()

	inputURL := fileServer.URL + "/normalized/stream.m3u8"
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
	time.Sleep(1500 * time.Millisecond)

	destState := hlsDest.State()
	if destState != nil {
		t.Logf("Video frames passed: %d", destState.TotalVideoFrames)
		t.Logf("Audio frames passed: %d", destState.TotalAudioFrames)
	}

	outputPlaylist := filepath.Join(outDir, "stream.m3u8")
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
