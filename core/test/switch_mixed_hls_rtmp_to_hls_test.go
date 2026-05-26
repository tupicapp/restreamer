package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corehelpers "github.com/tupicapp/restreamer/core"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
	"github.com/tupicapp/restreamer/core/streamfactory"
)

type commandResult struct {
	err    error
	stderr string
}

func TestSwitchMixedHLSAndRTMPRemainDecodableAtHLSDestination(t *testing.T) {
	requireBinary(t, "ffprobe")
	requireBinary(t, "ffmpeg")

	hlsURL := getConfiguredHLSFixtureURL(testHLSFixtureRelativePath)
	if hlsURL == testHLSFixtureURL && !isHTTPFixtureReady(hlsURL, 2*time.Second) {
		fixturePlaylistPath := resolveTestFixturePath(testHLSFixtureRelativePath)
		if fixturePlaylistPath == "" {
			t.Fatalf("unable to resolve fixture path %q", testHLSFixtureRelativePath)
		}
		fixtureDir := filepath.Dir(fixturePlaylistPath)
		fixtureServer := httptest.NewServer(http.FileServer(http.Dir(fixtureDir)))
		t.Cleanup(fixtureServer.Close)
		hlsURL = fixtureServer.URL + "/stream.m3u8"
	}
	requireHTTPReachable(t, hlsURL, 10*time.Second)

	rtmpURL := getConfiguredRTMPURL(t)
	requireRTMPPublishingOrSkip(t, rtmpURL, 10*time.Second)
	if base := getRTMPBaseURL(t, rtmpURL); base != "" {
		t.Setenv("HLS_READER_LIVE_FFMPEG_RTMP_URL", base)
	}

	streamer := corehelpers.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	inputIDs := []string{"mixed-hls-in", "mixed-rtmp-in"}
	inputURLs := []string{hlsURL, rtmpURL}
	inputs := make([]Stream, 0, len(inputURLs))
	for i, inputURL := range inputURLs {
		stream, err := streamfactory.NewInput(inputIDs[i], inputURL)
		if err != nil {
			t.Fatalf("NewInput(%q) error = %v", inputURL, err)
		}
		inputs = append(inputs, stream)
	}

	outDir := filepath.Join("test", "mixed-switch-hls-out")
	outFolder := storage.NewFolder(outDir)
	dest, err := outputs.NewHLSLiveDestination(
		"mixed-switch-hls-dest",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(1*time.Second),
		outputs.WithHLSPlaylistSize(24),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination() error = %v", err)
	}

	if err := streamer.UpdateStreams(inputs, []Stream{dest}); err != nil {
		t.Fatalf("UpdateStreams() error = %v", err)
	}
	streamer.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := dest.WaitForStart(startCtx); err != nil {
		t.Fatalf("dest WaitForStart() error = %v", err)
	}

	switchWindows := []time.Duration{
		350 * time.Millisecond,
		700 * time.Millisecond,
		1200 * time.Millisecond,
		450 * time.Millisecond,
		900 * time.Millisecond,
		600 * time.Millisecond,
	}
	switchOrder := make([]string, 0, 18)
	for i := 0; i < 18; i++ {
		switchOrder = append(switchOrder, inputIDs[i%2])
	}
	streamsByID := inputsByID(inputs, inputIDs)
	lastSegmentCount := 0
	lastSegmentGrowth := time.Now()
	stallBudget := 3 * time.Second

	for i, inputID := range switchOrder {
		if ok := streamer.Switch(inputID); !ok {
			t.Fatalf("Switch(%q) returned false", inputID)
		}
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := streamsByID[inputID].WaitForStart(waitCtx); err != nil {
			waitCancel()
			t.Fatalf("input %q WaitForStart() error after switch: %v", inputID, err)
		}
		waitCancel()
		time.Sleep(switchWindows[i%len(switchWindows)])

		segmentCount := countTransportSegments(t, outDir)
		if segmentCount > lastSegmentCount {
			lastSegmentCount = segmentCount
			lastSegmentGrowth = time.Now()
		} else if time.Since(lastSegmentGrowth) > stallBudget {
			t.Fatalf("HLS destination appears stalled during switching: segments=%d no growth for %s", segmentCount, time.Since(lastSegmentGrowth).Truncate(100*time.Millisecond))
		}
	}

	time.Sleep(2 * time.Second)
	streamer.Close()

	playlistPath := filepath.Join(outDir, "stream.m3u8")
	minSegments := len(switchOrder) / 3
	if minSegments < 6 {
		minSegments = 6
	}
	waitForHLSArtifacts(t, outDir, 8*time.Second, minSegments)
	assertHLSPlaylistLooksValid(t, playlistPath)
	segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	sort.Strings(segmentFiles)
	if len(segmentFiles) < minSegments {
		t.Fatalf("expected at least %d segments, got %d", minSegments, len(segmentFiles))
	}
	for _, segmentPath := range segmentFiles {
		if err := checkTransportStreamPacketsStrict(segmentPath); err != nil {
			t.Fatal(err)
		}
	}

	assertPlaylistTimestampsWithinJumpBudget(t, playlistPath, 100*time.Millisecond)
	assertHLSPlaylistRemuxesWithoutTimestampWarnings(t, playlistPath)
}

func TestSwitchMixedHLSAndRTMPLiveEdgeAttachRemainProbeableAtHLSDestination(t *testing.T) {
	requireBinary(t, "ffmpeg")
	requireBinary(t, "ffprobe")

	hlsURL := getConfiguredHLSFixtureURL(testHLSFixtureRelativePath)
	if hlsURL == testHLSFixtureURL && !isHTTPFixtureReady(hlsURL, 2*time.Second) {
		fixturePlaylistPath := resolveTestFixturePath(testHLSFixtureRelativePath)
		if fixturePlaylistPath == "" {
			t.Fatalf("unable to resolve fixture path %q", testHLSFixtureRelativePath)
		}
		fixtureDir := filepath.Dir(fixturePlaylistPath)
		fixtureServer := httptest.NewServer(http.FileServer(http.Dir(fixtureDir)))
		t.Cleanup(fixtureServer.Close)
		hlsURL = fixtureServer.URL + "/stream.m3u8"
	}
	requireHTTPReachable(t, hlsURL, 10*time.Second)

	rtmpURL := getConfiguredRTMPURL(t)
	requireRTMPPublishingOrSkip(t, rtmpURL, 10*time.Second)
	if base := getRTMPBaseURL(t, rtmpURL); base != "" {
		t.Setenv("HLS_READER_LIVE_FFMPEG_RTMP_URL", base)
	}

	streamer := corehelpers.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	inputIDs := []string{"mixed-hls-live-edge-in", "mixed-rtmp-live-edge-in"}
	inputURLs := []string{hlsURL, rtmpURL}
	inputs := make([]Stream, 0, len(inputURLs))
	for i, inputURL := range inputURLs {
		stream, err := streamfactory.NewInput(inputIDs[i], inputURL)
		if err != nil {
			t.Fatalf("NewInput(%q) error = %v", inputURL, err)
		}
		inputs = append(inputs, stream)
	}

	outDir := filepath.Join("test", "mixed-switch-hls-live-edge-out")
	outFolder := storage.NewFolder(outDir)
	dest, err := outputs.NewHLSLiveDestination(
		"mixed-switch-hls-live-edge-dest",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(1*time.Second),
		outputs.WithHLSPlaylistSize(12),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination() error = %v", err)
	}

	if err := streamer.UpdateStreams(inputs, []Stream{dest}); err != nil {
		t.Fatalf("UpdateStreams() error = %v", err)
	}
	streamer.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := dest.WaitForStart(startCtx); err != nil {
		t.Fatalf("dest WaitForStart() error = %v", err)
	}

	switchWindows := []time.Duration{
		350 * time.Millisecond,
		700 * time.Millisecond,
		1200 * time.Millisecond,
		450 * time.Millisecond,
		900 * time.Millisecond,
		600 * time.Millisecond,
		800 * time.Millisecond,
		500 * time.Millisecond,
	}
	switchOrder := make([]string, 0, 28)
	for i := 0; i < 28; i++ {
		switchOrder = append(switchOrder, inputIDs[i%2])
	}
	streamsByID := inputsByID(inputs, inputIDs)
	lastSegmentCount := 0
	lastSegmentGrowth := time.Now()
	stallBudget := 3 * time.Second
	playlistPath := filepath.Join(outDir, "stream.m3u8")

	switchToInputAndMonitorGrowth(t, streamer, streamsByID, inputIDs[0], 2500*time.Millisecond, outDir, &lastSegmentCount, &lastSegmentGrowth, 5*time.Second)

	waitForHLSArtifacts(t, outDir, 8*time.Second, 2)
	assertHLSPlaylistLooksValid(t, playlistPath)

	cmd := exec.Command(
		"ffmpeg",
		"-v", "warning",
		"-analyzeduration", "0",
		"-probesize", "5000000",
		"-i", playlistPath,
		"-t", "24",
		"-map", "0",
		"-f", "null",
		"-",
	)
	resultCh, err := startCommandCaptureStderr(cmd)
	if err != nil {
		t.Fatalf("ffmpeg live-edge attach start failed: %v", err)
	}

	for i := 0; i < len(switchOrder); i++ {
		switchToInputAndMonitorGrowth(t, streamer, streamsByID, switchOrder[i], switchWindows[i%len(switchWindows)], outDir, &lastSegmentCount, &lastSegmentGrowth, stallBudget)
	}

	result := waitForCommandResult(t, resultCh, "ffmpeg live-edge attach mixed hls/rtmp output", 30*time.Second)
	assertNoForbiddenLiveEdgeWarnings(t, result.stderr)

	time.Sleep(4 * time.Second)
	streamer.Close()

	waitForHLSArtifacts(t, outDir, 8*time.Second, 10)
	assertHLSPlaylistLooksValid(t, playlistPath)
	assertRecentSegmentsHaveProbeableAudio(t, outDir, 4)
}

func inputsByID(inputs []Stream, inputIDs []string) map[string]Stream {
	byID := make(map[string]Stream, len(inputs))
	for i, input := range inputs {
		if i >= len(inputIDs) {
			continue
		}
		byID[inputIDs[i]] = input
	}
	return byID
}

func checkTransportStreamPacketsVideoRequired(segmentPath string) error {
	probe, err := dumpFrames(segmentPath)
	if err != nil {
		return err
	}

	var hasH264 bool
	for _, stream := range probe.Streams {
		if stream.CodecType == "video" && strings.Contains(strings.ToLower(stream.CodecName), "h264") {
			hasH264 = true
			break
		}
	}
	if !hasH264 {
		return fmt.Errorf("expected h264 video stream in %s", segmentPath)
	}

	videoPackets, audioPackets := splitPacketsByType(probe.Packets)
	if len(videoPackets) == 0 {
		return fmt.Errorf("expected video packets in %s", segmentPath)
	}
	if err := checkPacketTimeline(videoPackets, "video", segmentPath); err != nil {
		return err
	}
	if len(audioPackets) > 0 {
		if err := checkPacketTimeline(audioPackets, "audio", segmentPath); err != nil {
			return err
		}
	}

	return nil
}

func checkTransportStreamPacketsStrict(segmentPath string) error {
	probe, err := dumpFrames(segmentPath)
	if err != nil {
		return err
	}

	var hasH264 bool
	audioProbeable := false
	for _, stream := range probe.Streams {
		if stream.CodecType == "video" && strings.Contains(strings.ToLower(stream.CodecName), "h264") {
			hasH264 = true
		}
		if stream.CodecType == "audio" && strings.Contains(strings.ToLower(stream.CodecName), "aac") {
			if strings.TrimSpace(stream.SampleRate) != "" && stream.SampleRate != "0" && stream.Channels > 0 {
				audioProbeable = true
			}
		}
	}
	if !hasH264 {
		return fmt.Errorf("expected h264 video stream in %s", segmentPath)
	}

	videoPackets, audioPackets := splitPacketsByType(probe.Packets)
	if len(videoPackets) == 0 {
		return fmt.Errorf("expected video packets in %s", segmentPath)
	}
	if !packetIsKeyframe(videoPackets[0]) {
		return fmt.Errorf("expected first video packet in %s to be keyframe, got flags=%q", segmentPath, videoPackets[0].Flags)
	}
	if err := checkPacketTimeline(videoPackets, "video", segmentPath); err != nil {
		return err
	}
	if err := checkPacketJumpBudget(videoPackets, "video", segmentPath, 100*time.Millisecond); err != nil {
		return err
	}
	if len(audioPackets) > 0 {
		if !audioProbeable {
			return fmt.Errorf("audio exists in %s but ffprobe could not resolve sample_rate/channels", segmentPath)
		}
		if err := checkPacketTimeline(audioPackets, "audio", segmentPath); err != nil {
			return err
		}
		if err := checkPacketJumpBudget(audioPackets, "audio", segmentPath, 100*time.Millisecond); err != nil {
			return err
		}
	}

	return nil
}

func assertPlaylistTimestampsWithinJumpBudget(t *testing.T, playlistPath string, maxJump time.Duration) {
	t.Helper()

	probe, err := dumpFrames(playlistPath)
	if err != nil {
		t.Fatalf("dumpFrames failed on %s: %v", playlistPath, err)
	}
	videoPackets, audioPackets := splitPacketsByType(probe.Packets)
	if err := checkPacketJumpBudget(videoPackets, "video", playlistPath, maxJump); err != nil {
		t.Fatal(err)
	}
	if len(audioPackets) > 0 {
		if err := checkPacketJumpBudget(audioPackets, "audio", playlistPath, maxJump); err != nil {
			t.Fatal(err)
		}
	}
}

func checkPacketJumpBudget(packets []Packet, codecType, target string, maxJump time.Duration) error {
	maxJumpSeconds := maxJump.Seconds()
	var prevPTS float64
	var prevDTS float64
	var havePTS bool
	var haveDTS bool

	for i, packet := range packets {
		if pts, ok := parseFlexFloat(packet.PtsTime); ok {
			if havePTS {
				delta := pts - prevPTS
				if delta < 0 {
					return fmt.Errorf("%s packet pts moved backwards in %s at index %d: prev=%f next=%f", codecType, target, i, prevPTS, pts)
				}
				if delta > maxJumpSeconds {
					return fmt.Errorf("%s packet pts jumped by %.3fs in %s at index %d (budget %.3fs)", codecType, delta, target, i, maxJumpSeconds)
				}
			}
			prevPTS = pts
			havePTS = true
		}
		if dts, ok := parseFlexFloat(packet.DtsTime); ok {
			if haveDTS {
				delta := dts - prevDTS
				if delta < 0 {
					return fmt.Errorf("%s packet dts moved backwards in %s at index %d: prev=%f next=%f", codecType, target, i, prevDTS, dts)
				}
				if delta > maxJumpSeconds {
					return fmt.Errorf("%s packet dts jumped by %.3fs in %s at index %d (budget %.3fs)", codecType, delta, target, i, maxJumpSeconds)
				}
			}
			prevDTS = dts
			haveDTS = true
		}
	}

	return nil
}

func packetIsKeyframe(packet Packet) bool {
	return strings.Contains(packet.Flags, "K")
}

func countTransportSegments(t *testing.T, outDir string) int {
	t.Helper()

	files, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	return len(files)
}

func assertRecentSegmentsHaveProbeableAudio(t *testing.T, outDir string, recentCount int) {
	t.Helper()

	segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	sort.Strings(segmentFiles)
	if len(segmentFiles) < recentCount {
		t.Fatalf("need at least %d segments in %s, got %d", recentCount, outDir, len(segmentFiles))
	}

	recent := segmentFiles[len(segmentFiles)-recentCount:]
	for _, segmentPath := range recent {
		probe, err := dumpFrames(segmentPath)
		if err != nil {
			t.Fatalf("dumpFrames failed on %s: %v", segmentPath, err)
		}

		audioProbeable := false
		for _, stream := range probe.Streams {
			if stream.CodecType == "audio" && strings.Contains(strings.ToLower(stream.CodecName), "aac") {
				if strings.TrimSpace(stream.SampleRate) != "" && stream.SampleRate != "0" && stream.Channels > 0 {
					audioProbeable = true
					break
				}
			}
		}
		if !audioProbeable {
			t.Fatalf("recent segment %s lost probeable audio", segmentPath)
		}

		_, audioPackets := splitPacketsByType(probe.Packets)
		if len(audioPackets) == 0 {
			t.Fatalf("recent segment %s has no audio packets", segmentPath)
		}
		if err := checkPacketJumpBudget(audioPackets, "audio", segmentPath, 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
}

func switchToInputAndMonitorGrowth(t *testing.T, streamer *corehelpers.Streamer, streamsByID map[string]Stream, inputID string, wait time.Duration, outDir string, lastSegmentCount *int, lastSegmentGrowth *time.Time, stallBudget time.Duration) {
	t.Helper()

	if ok := streamer.Switch(inputID); !ok {
		t.Fatalf("Switch(%q) returned false", inputID)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := streamsByID[inputID].WaitForStart(waitCtx); err != nil {
		waitCancel()
		t.Fatalf("input %q WaitForStart() error after switch: %v", inputID, err)
	}
	waitCancel()
	time.Sleep(wait)

	segmentCount := countTransportSegments(t, outDir)
	if segmentCount > *lastSegmentCount {
		*lastSegmentCount = segmentCount
		*lastSegmentGrowth = time.Now()
		return
	}
	if time.Since(*lastSegmentGrowth) > stallBudget {
		t.Fatalf("HLS destination appears stalled during switching: segments=%d no growth for %s", segmentCount, time.Since(*lastSegmentGrowth).Truncate(100*time.Millisecond))
	}
}

func startCommandCaptureStderr(cmd *exec.Cmd) (<-chan commandResult, error) {
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	resultCh := make(chan commandResult, 1)
	go func() {
		resultCh <- commandResult{
			err:    cmd.Wait(),
			stderr: strings.TrimSpace(stderr.String()),
		}
	}()

	return resultCh, nil
}

func waitForCommandResult(t *testing.T, resultCh <-chan commandResult, label string, timeout time.Duration) commandResult {
	t.Helper()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("%s failed: %v\n%s", label, result.err, result.stderr)
		}
		return result
	case <-time.After(timeout):
		t.Fatalf("%s timed out after %s", label, timeout)
		return commandResult{}
	}
}

func assertNoForbiddenLiveEdgeWarnings(t *testing.T, stderr string) {
	t.Helper()

	if stderr == "" {
		return
	}

	lower := strings.ToLower(stderr)
	forbidden := []string{
		"could not find codec parameters",
		"unspecified sample rate",
		"0 channels",
		"non monotonically increasing dts",
		"decode_slice_header error",
		"missing picture in access unit",
		"error while decoding",
		"packet corrupt",
	}
	for _, needle := range forbidden {
		if strings.Contains(lower, needle) {
			t.Fatalf("ffmpeg live-edge attach produced forbidden warning %q:\n%s", needle, stderr)
		}
	}
}

func assertHLSPlaylistRemuxesWithoutTimestampWarnings(t *testing.T, playlistPath string) {
	t.Helper()

	cmd := exec.Command(
		"ffmpeg",
		"-v", "warning",
		"-i", playlistPath,
		"-map", "0",
		"-f", "null",
		"-",
	)
	runCmdEnsureNoStderrWithTimeout(t, cmd, "ffmpeg remux mixed hls/rtmp output", 120*time.Second)
}
