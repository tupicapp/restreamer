package test

import (
	"context"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type namedTest struct {
	name         string
	isSequential bool
	fn           func(*testing.T)
}

const (
	testHLSFixtureRelativePath = "testdata/stream.m3u8"
	testMinionFixturePath      = "testdata/minion.mp4"
	testHLSFixtureURL          = "http://127.0.0.1:8091/testdata/stream.m3u8"
	testRTMPBaseURL            = "rtmp://localhost:1938/live/"
	testRTMPAVURL              = "rtmp://localhost:1938/live/1"
	testRTMPVideoLessURL       = "rtmp://localhost:1938/live/video-less"
	testRTMPAudioLessURL       = "rtmp://localhost:1938/live/audio-less"
	miladNobURL                = "http://127.0.0.1:8091/testdata/stream.m3u8"
	testAllHLSFixtureURL       = testHLSFixtureURL
	testAllRTMPAVURL           = testRTMPAVURL
	testAllRTMPAudioLessURL    = testRTMPAudioLessURL
	testAllRTMPVideoLessURL    = testRTMPVideoLessURL
)

func runNamedTests(t *testing.T, tests []namedTest) {
	t.Helper()

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if !tc.isSequential {
				t.Parallel()
			}
			tc.fn(t)
		})
	}
}

// TestAll runs the curated end-to-end suite for the streaming pipeline.
// Cases inside each group run in parallel.
func TestAll(t *testing.T) {
	t.Log("=== Running All irajstreamer Tests ===")
	cleanupInfra := requireAllTestInfraReady(t)
	if cleanupInfra != nil {
		defer cleanupInfra()
	}

	t.Run("Unit", func(t *testing.T) {
		runNamedTests(t, []namedTest{
			{name: "RewriteHLSPlaylist_RewritesRelativeSegmentURI", fn: TestRewriteHLSPlaylist_RewritesRelativeSegmentURI},
			{name: "RewriteHLSPlaylist_DoesNotRewriteAbsoluteSegmentURI", fn: TestRewriteHLSPlaylist_DoesNotRewriteAbsoluteSegmentURI},
			{name: "RewriteHLSPlaylist_RewritesLegacyRootRelativeProgramURIToConfiguredPrefix", fn: TestRewriteHLSPlaylist_RewritesLegacyRootRelativeProgramURIToConfiguredPrefix},
			{name: "JoinHLSPrefix_URLBase", fn: TestJoinHLSPrefix_URLBase},
			{name: "StreamerUpdateStreams_ReplacesAndRemoves", fn: TestStreamer_UpdateStreams_ReplacesAndRemoves},
			{name: "StreamerAddInputOutputAndSwitch", fn: TestStreamer_AddInputOutputAndSwitch},
			{name: "StreamerRemoveInputIfSame_OnlyRemovesMatchingInstance", fn: TestStreamer_RemoveInputIfSame_OnlyRemovesMatchingInstance},
			{name: "StreamerStopOutput_StopsWithoutRemoving", fn: TestStreamer_StopOutput_StopsWithoutRemoving},
			{name: "StreamManagerRestart", fn: TestStreamManager_RestartsOnStaleIO},
		})
	})

	t.Run("Integration", func(t *testing.T) {
		runNamedTests(t, []namedTest{
			{name: "StreamerHLSReaderToBufferingDestination", fn: TestStreamer_HLSReaderToBufferingDestination},
			{name: "StreamerHLSReaderLiveToBufferingDestination", fn: TestStreamer_HLSReaderLiveToBufferingDestination},
			{name: "StreamerRTMPReaderToBufferingDestination", fn: TestStreamer_RTMPReaderToBufferingDestination},
			{name: "StreamerSwitchBetweenInputs", fn: TestStreamer_SwitchBetweenInputs, isSequential: true},
			{name: "StreamerHLSReaderTiming", fn: TestStreamer_HLSReaderTiming},
			{name: "StreamerRTMPReaderTiming", fn: TestStreamer_RTMPReaderTiming},

			// {name: "HLSLiveInputToHLSOutput_FramesMatch", fn: TestHLSLiveInputToHLSOutput_FramesMatch},
		})
	})

	t.Run("Passthrough", func(t *testing.T) {
		runNamedTests(t, []namedTest{
			{name: "DirectHLSPassthrough", fn: TestDirectHLSPassthrough},
			{name: "HLSStreamerPassthrough", fn: TestHLSStreamerPassthrough},
			{name: "DirectHLSLivePassthrough", fn: TestDirectHLSLivePassthrough},
			{name: "HLSLiveStreamerPassthrough", fn: TestHLSLiveStreamerPassthrough},
			{name: "MultiHLSToHLSWindowSwitchesMatchReference", fn: TestMultiHLSToHLS_WindowSwitchesMatchReference},

			// {name: "MultiHLSToHLSMixedFileAndLiveWindowSwitchesMatchReference", fn: TestMultiHLSToHLS_MixedFileAndLiveWindowSwitchesMatchReference},
		})
	})

	t.Run("Compatibility", func(t *testing.T) {
		runNamedTests(t, []namedTest{
			{name: "AudioLessToHLS", fn: TestAudioLessToHLS, isSequential: true},
			{name: "VideoLessToHLS", fn: TestVideoLessToHLS, isSequential: true},
			{name: "SwitchRTMPCompatibleInputsKeepAVFlow", fn: TestSwitchRTMPCompatibleInputsKeepAVFlow, isSequential: true},
			{name: "SwitchRTMPCompatibleInputsRemainDecodableAtHLSDestination", fn: TestSwitchRTMPCompatibleInputsRemainDecodableAtHLSDestination, isSequential: true},
			{name: "SwitchRTMPCompatibleInputsRemainDecodableAtRTMPOutput", fn: TestSwitchRTMPCompatibleInputsRemainDecodableAtRTMPOutput, isSequential: true},
			{name: "SwitchMixedHLSAndRTMPRemainDecodableAtHLSDestination", fn: TestSwitchMixedHLSAndRTMPRemainDecodableAtHLSDestination, isSequential: true},
			{name: "SwitchMixedHLSAndRTMPRemainDecodableAtHLSLiveDestination", fn: TestSwitchMixedHLSAndRTMPLiveEdgeAttachRemainProbeableAtHLSDestination, isSequential: true},

			// {name: "SwitchRTMPVideoLessInputsRemainDecodableAtHLSDestination", fn: TestSwitchRTMPVideoLessInputsRemainDecodableAtHLSDestination, isSequential: true},
			// {name: "SwitchRTMPAudioLessInputsRemainDecodableAtHLSDestination", fn: TestSwitchRTMPAudioLessInputsRemainDecodableAtHLSDestination, isSequential: true},
		})
	})

	t.Log("=== All Tests Completed ===")
}

func requireAllTestInfraReady(t *testing.T) func() {
	t.Helper()

	requireBinary(t, "ffmpeg")
	requireBinary(t, "ffprobe")
	requireTestFixturePathsExist(t, testHLSFixtureRelativePath, testMinionFixturePath)

	hlsCleanup := ensureAggregateHLSFixtureServer(t)
	ensureAggregateRTMPFixturePublishers(t)
	waitForHTTPFixtureReady(t, testAllHLSFixtureURL, 20*time.Second)
	waitForRTMPFixtureReady(t, testAllRTMPAVURL, 60*time.Second)
	waitForRTMPFixtureReady(t, testAllRTMPAudioLessURL, 60*time.Second)
	waitForRTMPFixtureReady(t, testAllRTMPVideoLessURL, 60*time.Second)

	return hlsCleanup
}

func waitForHTTPFixtureReady(t *testing.T, targetURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err == nil {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				cancel()
				if resp.StatusCode >= 200 && resp.StatusCode < 400 {
					return
				}
				lastErr = &httpStatusError{statusCode: resp.StatusCode}
			} else {
				lastErr = err
			}
		} else {
			lastErr = err
		}
		cancel()
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("test HTTP fixture not ready: %s (%v)", targetURL, lastErr)
}

func waitForRTMPFixtureReady(t *testing.T, rtmpURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-i", rtmpURL, "-show_streams")
		if err := cmd.Run(); err == nil {
			cancel()
			return
		} else {
			lastErr = err
		}
		cancel()
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("test RTMP fixture not ready: %s (%v)", rtmpURL, lastErr)
}

type httpStatusError struct {
	statusCode int
}

func (e *httpStatusError) Error() string {
	return http.StatusText(e.statusCode)
}

func ensureAggregateHLSFixtureServer(t *testing.T) func() {
	t.Helper()

	fixtureURL := testAllHLSFixtureURL
	if isHTTPFixtureReady(fixtureURL, 2*time.Second) {
		return nil
	}

	listener, err := net.Listen("tcp", ":8091")
	if err != nil {
		t.Fatalf("HLS fixture %s is not ready and local fixture server could not start on :8091: %v", fixtureURL, err)
	}

	testdataDir := testFixtureRootDir()
	if testdataDir == "" {
		_ = listener.Close()
		t.Fatal("testdata directory not found for aggregate HLS fixture server")
	}

	rootDir := filepath.Dir(testdataDir)
	server := &http.Server{
		Handler: http.FileServer(http.Dir(rootDir)),
	}

	go func() {
		_ = server.Serve(listener)
	}()

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
}

func isHTTPFixtureReady(targetURL string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()

	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

func ensureAggregateRTMPFixturePublishers(t *testing.T) {
	t.Helper()

	minionFixture := resolveTestFixturePath(testMinionFixturePath)
	if minionFixture == "" {
		t.Fatalf("unable to resolve fixture path %q", testMinionFixturePath)
	}

	ensureRTMPFixturePublisher(t, "audio-less", testAllRTMPAudioLessURL, []string{
		"-re",
		"-stream_loop", "-1",
		"-fflags", "+genpts",
		"-i", minionFixture,
		"-an",
		"-vf", "fps=30",
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-tune", "zerolatency",
		"-g", "30",
		"-bf", "0",
		"-sc_threshold", "0",
		"-flvflags", "+no_duration_filesize",
		"-f", "flv",
		testAllRTMPAudioLessURL,
	})

	ensureRTMPFixturePublisher(t, "video-less", testAllRTMPVideoLessURL, []string{
		"-re",
		"-stream_loop", "-1",
		"-fflags", "+genpts",
		"-i", minionFixture,
		"-vn",
		"-c:a", "aac",
		"-ar", "44100",
		"-b:a", "128k",
		"-flvflags", "+no_duration_filesize",
		"-f", "flv",
		testAllRTMPVideoLessURL,
	})
}

func ensureRTMPFixturePublisher(t *testing.T, streamName, streamURL string, ffmpegArgs []string) {
	t.Helper()

	if isRTMPFixtureReady(streamURL, 2*time.Second) {
		return
	}

	cmd := exec.Command("ffmpeg", ffmpegArgs...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start ffmpeg publisher for %s failed: %v", streamName, err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	})
}

func isRTMPFixtureReady(rtmpURL string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-i", rtmpURL, "-show_streams")
	return cmd.Run() == nil
}

// In RTMP reading, encoder buffering can produce a short initial audio-only
// interval. Tests therefore compare windows after both streams have begun and
// allow minor A/V timing skew.
