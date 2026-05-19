package test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	corehelpers "restreamer/core"
	streaminputs "restreamer/core/inputs"
	"restreamer/core/outputs"
	"restreamer/core/storage"
	testtools "restreamer/core/test_tools"
)

func mustListen(t *testing.T, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("net.Listen(%q): %v", addr, err)
	}
	return ln
}

const miladNobURL = "http://localhost:8091/testdata/stream.m3u8"
const aljaziraURL = "https://live-hls-web-aja-fa.getaj.net/AJA/03.m3u8"

// TestHLSLiveInputToHLSOutput_FramesMatch streams from a live HLS source through the
// full input→streamer→HLS-output→re-read pipeline and verifies that the decoded
// output frames match what was received directly from the source.
func TestHLSLiveInputToHLSOutput_FramesMatch(t *testing.T) {
	requireHTTPReachable(t, miladNobURL, 5*time.Second)

	const collectionDuration = 30 * time.Second

	// ── Step 1: collect reference frames directly from source ────────────────
	t.Log("Step 1: collecting reference frames directly from source…")

	refStreamer := corehelpers.NewStreamer()
	defer refStreamer.Close()
	refStreamer.StartLife()

	refInput := streaminputs.NewHLSLive("ref-input", miladNobURL)
	refBuf := outputs.NewBuffering("ref-buf")

	if err := refStreamer.UpdateStreams([]Stream{refInput}, []Stream{refBuf}); err != nil {
		t.Fatalf("ref UpdateStreams: %v", err)
	}
	refStreamer.Start()
	refStreamer.Switch("ref-input")

	{
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := refInput.WaitForStart(ctx); err != nil {
			t.Fatalf("ref input failed to start: %v", err)
		}
		if err := refBuf.WaitForStart(ctx); err != nil {
			t.Fatalf("ref buf failed to start: %v", err)
		}
	}

	// ── Step 2: stream into HLS output destination ───────────────────────────
	t.Log("Step 2: streaming into HLS live output…")

	outDir := "./testdata/"
	outFolder := storage.NewFolder(outDir)

	hlsDest, err := outputs.NewHLSLiveDestination("hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(2*time.Second),
		outputs.WithHLSPlaylistSize(10),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination: %v", err)
	}

	pipeStreamer := corehelpers.NewStreamer()
	defer pipeStreamer.Close()
	pipeStreamer.StartLife()

	pipeInput := streaminputs.NewHLS("pipe-input", miladNobURL)

	if err := pipeStreamer.UpdateStreams([]Stream{pipeInput}, []Stream{hlsDest}); err != nil {
		t.Fatalf("pipe UpdateStreams: %v", err)
	}
	pipeStreamer.Start()
	pipeStreamer.Switch("pipe-input")

	{
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := pipeInput.WaitForStart(ctx); err != nil {
			t.Fatalf("pipe input failed to start: %v", err)
		}
		if err := hlsDest.WaitForStart(ctx); err != nil {
			t.Fatalf("hls dest failed to start: %v", err)
		}
	}

	// Give the HLS destination time to write at least 2 segments before reading
	t.Log("Waiting for HLS output to produce segments…")
	time.Sleep(6 * time.Second)

	// ── Step 3: serve the HLS output over HTTP and read it back ─────────────
	t.Log("Step 3: serving HLS output over HTTP and reading it back…")

	srv := http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.FileServer(http.Dir(outDir)),
	}
	// Use a random listener
	ln := mustListen(t, "127.0.0.1:0")
	outPlaylistURL := "http://" + ln.Addr().String() + "/stream.m3u8"
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	outInput := streaminputs.NewHLS("out-input", outPlaylistURL)
	outBuf := outputs.NewBuffering("out-buf")

	outStreamer := corehelpers.NewStreamer()
	defer outStreamer.Close()
	outStreamer.StartLife()

	if err := outStreamer.UpdateStreams([]Stream{outInput}, []Stream{outBuf}); err != nil {
		t.Fatalf("out UpdateStreams: %v", err)
	}
	outStreamer.Start()
	outStreamer.Switch("out-input")

	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := outInput.WaitForStart(ctx); err != nil {
			t.Fatalf("out input failed to start: %v", err)
		}
		if err := outBuf.WaitForStart(ctx); err != nil {
			t.Fatalf("out buf failed to start: %v", err)
		}
	}

	// ── Step 4: collect for collectionDuration ───────────────────────────────
	t.Logf("Collecting frames for %v…", collectionDuration)
	time.Sleep(collectionDuration)

	refVideo := refBuf.GetVideoFrames()
	refAudio := refBuf.GetAudioFrames()
	outVideo := outBuf.GetVideoFrames()
	outAudio := outBuf.GetAudioFrames()

	t.Logf("Reference:  video=%d  audio=%d", len(refVideo), len(refAudio))
	t.Logf("Output:     video=%d  audio=%d", len(outVideo), len(outAudio))

	if len(refVideo) == 0 && len(refAudio) == 0 {
		t.Fatal("reference reader collected no frames — is the source reachable?")
	}
	if len(outVideo) == 0 && len(outAudio) == 0 {
		t.Fatal("output reader collected no frames — HLS output may not be producing valid segments")
	}

	// ── Step 5: stream-health checks ─────────────────────────────────────────
	t.Log("=== Reference stream health ===")
	refVHealth := testtools.CheckStreamHealth(refVideo, "ref-video")
	refAHealth := testtools.CheckStreamHealth(refAudio, "ref-audio")
	testtools.PrintStreamHealth(t, refVHealth, "ref-video")
	testtools.PrintStreamHealth(t, refAHealth, "ref-audio")

	t.Log("=== Output stream health ===")
	outVHealth := testtools.CheckStreamHealth(outVideo, "out-video")
	outAHealth := testtools.CheckStreamHealth(outAudio, "out-audio")
	testtools.PrintStreamHealth(t, outVHealth, "out-video")
	testtools.PrintStreamHealth(t, outAHealth, "out-audio")

	if !outVHealth.IsHealthy {
		t.Errorf("output video stream unhealthy: monotonic-PTS=%.2f%% monotonic-DTS=%.2f%% valid-gaps=%.2f%% DTS-valid=%.2f%%",
			outVHealth.MonotonicPTSPercent, outVHealth.MonotonicDTSPercent,
			outVHealth.ValidGapPercent, outVHealth.DTSValidPercent)
	}
	if !outAHealth.IsHealthy {
		t.Errorf("output audio stream unhealthy: monotonic-PTS=%.2f%% monotonic-DTS=%.2f%% valid-gaps=%.2f%% DTS-valid=%.2f%%",
			outAHealth.MonotonicPTSPercent, outAHealth.MonotonicDTSPercent,
			outAHealth.ValidGapPercent, outAHealth.DTSValidPercent)
	}

	// DTS must be strictly monotonic — any failure here is a bug in the input
	if len(outVHealth.MonotonicDTSIssues) > 0 {
		t.Errorf("output video has %d non-monotonic DTS frames (input bug)", len(outVHealth.MonotonicDTSIssues))
	}
	if len(outAHealth.MonotonicDTSIssues) > 0 {
		t.Errorf("output audio has %d non-monotonic DTS frames (input bug)", len(outAHealth.MonotonicDTSIssues))
	}
	if len(outVHealth.DTSIssues) > 0 {
		t.Errorf("output video has %d frames where DTS > PTS (input bug)", len(outVHealth.DTSIssues))
	}
	if len(outAHealth.DTSIssues) > 0 {
		t.Errorf("output audio has %d frames where DTS > PTS (input bug)", len(outAHealth.DTSIssues))
	}

	// ── Step 6: window-match comparison ──────────────────────────────────────
	const threshold = 0.10
	t.Log("=== Window-match comparison: reference vs output ===")

	videoRes := testtools.WindowMatchBenchmark(outVideo, refVideo, "video")
	audioRes := testtools.WindowMatchBenchmark(outAudio, refAudio, "audio")
	testtools.PrintWindowMatchBenchmark(t, videoRes, "video")
	testtools.PrintWindowMatchBenchmark(t, audioRes, "audio")

	if videoRes.MatchPercent < (1.0-threshold)*100 {
		t.Errorf("video match %.2f%% < required %.2f%% — output does not match input",
			videoRes.MatchPercent, (1.0-threshold)*100)
	}
	if audioRes.MatchPercent < (1.0-threshold)*100 {
		t.Errorf("audio match %.2f%% < required %.2f%% — output does not match input",
			audioRes.MatchPercent, (1.0-threshold)*100)
	}
}
