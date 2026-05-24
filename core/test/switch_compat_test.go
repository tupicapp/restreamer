package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corehelpers "github.com/tupicapp/restreamer/core"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
	"github.com/tupicapp/restreamer/core/streamfactory"
)

type videoAssertionWindow struct {
	inputID string
	start   float64
	end     float64
	hashes  []string
	pts     []float64
}

func TestSwitchRTMPCompatibleInputsKeepAVFlow(t *testing.T) {
	inputURLs := []string{
		testRTMPAVURL,
		testRTMPAudioLessURL,
		testRTMPVideoLessURL,
	}
	for _, inputURL := range inputURLs {
		requireRTMPPublishing(t, inputURL, 10*time.Second)
	}

	streamer := corehelpers.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	inputs := make([]Stream, 0, len(inputURLs))
	inputIDs := make([]string, 0, len(inputURLs))
	for i, inputURL := range inputURLs {
		inputID := "compat-switch-input-" + string(rune('1'+i))
		stream, err := streamfactory.NewInput(inputID, inputURL)
		if err != nil {
			t.Fatalf("NewSwitchInput(%q) error = %v", inputURL, err)
		}
		inputs = append(inputs, stream)
		inputIDs = append(inputIDs, inputID)
	}

	bufferingDest := NewBuffering("compat-switch-buffer")
	if err := streamer.UpdateStreams(inputs, []Stream{bufferingDest}); err != nil {
		t.Fatalf("UpdateStreams() error = %v", err)
	}
	streamer.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, input := range inputs {
		if err := input.WaitForStart(startCtx); err != nil {
			t.Fatalf("input %d WaitForStart() error = %v", i+1, err)
		}
	}
	if err := bufferingDest.WaitForStart(startCtx); err != nil {
		t.Fatalf("bufferingDest.WaitForStart() error = %v", err)
	}

	for _, inputID := range inputIDs {
		if ok := streamer.Switch(inputID); !ok {
			t.Fatalf("Switch(%q) returned false", inputID)
		}
		waitForAVGrowth(t, bufferingDest, 1500*time.Millisecond, 12, 8)
	}
}

func TestSwitchRTMPCompatibleInputsRemainDecodableAtRTMPOutput(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skipf("ffmpeg not available: %v", err)
	}

	inputURLs := []string{
		testRTMPAVURL,
		testRTMPAudioLessURL,
		testRTMPVideoLessURL,
	}
	for _, inputURL := range inputURLs {
		requireRTMPPublishing(t, inputURL, 10*time.Second)
	}

	streamer := corehelpers.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	inputs := make([]Stream, 0, len(inputURLs))
	inputIDs := make([]string, 0, len(inputURLs))
	for i, inputURL := range inputURLs {
		inputID := "compat-decode-input-" + string(rune('1'+i))
		stream, err := streamfactory.NewInput(inputID, inputURL)
		if err != nil {
			t.Fatalf("NewSwitchInput(%q) error = %v", inputURL, err)
		}
		inputs = append(inputs, stream)
		inputIDs = append(inputIDs, inputID)
	}

	outputURL := fmt.Sprintf("rtmp://localhost:1938/live/compat_out_decode_%d", time.Now().UnixNano())
	dest, err := outputs.NewRtmpWriter("compat-decode-out", outputURL)
	if err != nil {
		t.Fatalf("NewRtmpWriter() error = %v", err)
	}

	if err := streamer.UpdateStreams(inputs, []Stream{dest}); err != nil {
		t.Fatalf("UpdateStreams() error = %v", err)
	}
	streamer.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, input := range inputs {
		if err := input.WaitForStart(startCtx); err != nil {
			t.Fatalf("input %d WaitForStart() error = %v", i+1, err)
		}
	}
	if err := dest.WaitForStart(startCtx); err != nil {
		t.Fatalf("dest.WaitForStart() error = %v", err)
	}

	for _, inputID := range inputIDs {
		if ok := streamer.Switch(inputID); !ok {
			t.Fatalf("Switch(%q) returned false", inputID)
		}
		waitForRTMPOutputDecode(t, outputURL, 15*time.Second)
	}
}

func TestSwitchRTMPCompatibleInputsRemainDecodableAtHLSDestination(t *testing.T) {
	runSwitchCompatHLSTest(t,
		"compat-hls-mixed",
		[]string{
			testRTMPAVURL,
			testRTMPAudioLessURL,
			testRTMPVideoLessURL,
		},
		nil,
		false,
		false,
	)
}

func TestSwitchRTMPAudioLessInputsRemainDecodableAtHLSDestination(t *testing.T) {
	runSwitchCompatHLSTest(t,
		"compat-hls-audio-less",
		[]string{
			testRTMPAVURL,
			testRTMPVideoLessURL,
		},
		[]int{0, 1},
		false,
		true,
	)
}

func TestSwitchRTMPVideoLessInputsRemainDecodableAtHLSDestination(t *testing.T) {
	runSwitchCompatHLSTest(t,
		"compat-hls-video-less",
		[]string{
			testRTMPAVURL,
			testRTMPAudioLessURL,
		},
		nil,
		false,
		true,
	)
}

func runSwitchCompatHLSTest(t *testing.T, testID string, inputURLs []string, realVideoInputIndexes []int, requireSegmentBoundaryContinuity bool, requirePlaylistPacing bool) {
	requireBinary(t, "ffprobe")

	const (
		playWindow       = 5 * time.Second
		segmentFlushWait = 2 * time.Second
	)

	for _, inputURL := range inputURLs {
		requireRTMPPublishing(t, inputURL, 10*time.Second)
	}

	streamer := corehelpers.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	inputs := make([]Stream, 0, len(inputURLs))
	inputIDs := make([]string, 0, len(inputURLs))
	for i, inputURL := range inputURLs {
		inputID := testID + "-input-" + string(rune('1'+i))
		stream, err := streamfactory.NewInput(inputID, inputURL)
		if err != nil {
			t.Fatalf("NewSwitchInput(%q) error = %v", inputURL, err)
		}
		inputs = append(inputs, stream)
		inputIDs = append(inputIDs, inputID)
	}

	outDir := filepath.Join(t.TempDir(), testID+"-out")
	outFolder := storage.NewFolder(outDir)
	dest, err := outputs.NewHLSLiveDestination(
		testID+"-dest",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(1*time.Second),
		outputs.WithHLSPlaylistSize(24),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination() error = %v", err)
	}
	bufferingDest := NewBuffering(testID + "-buffer")

	if err := streamer.UpdateStreams(inputs, []Stream{dest, bufferingDest}); err != nil {
		t.Fatalf("UpdateStreams() error = %v", err)
	}
	streamer.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, input := range inputs {
		if err := input.WaitForStart(startCtx); err != nil {
			t.Fatalf("input %d WaitForStart() error = %v", i+1, err)
		}
	}
	if err := dest.WaitForStart(startCtx); err != nil {
		t.Fatalf("dest.WaitForStart() error = %v", err)
	}
	if err := bufferingDest.WaitForStart(startCtx); err != nil {
		t.Fatalf("bufferingDest.WaitForStart() error = %v", err)
	}

	playlistPath := filepath.Join(outDir, "stream.m3u8")
	for _, inputID := range inputIDs {
		if ok := streamer.Switch(inputID); !ok {
			t.Fatalf("Switch(%q) returned false", inputID)
		}

		time.Sleep(playWindow)
	}
	time.Sleep(segmentFlushWait)

	streamer.Close()
	var assertionWindows []videoAssertionWindow
	if len(realVideoInputIndexes) > 0 {
		bufferedVideoFrames := bufferingDest.GetVideoFrames()
		includeInputIDs := make(map[string]bool, len(realVideoInputIndexes))
		for _, idx := range realVideoInputIndexes {
			if idx < 0 || idx >= len(inputIDs) {
				t.Fatalf("invalid real video input index %d", idx)
			}
			includeInputIDs[inputIDs[idx]] = true
		}
		assertionWindows = buildVideoAssertionWindows(bufferedVideoFrames, includeInputIDs)
		if len(assertionWindows) == 0 {
			t.Fatal("expected buffered video frames for real-video assertions, got 0")
		}
	}

	minSegments := int(playWindow/time.Second) + 2
	assertFinalHLSSwitchOutput(t, outDir, playlistPath, minSegments, playWindow*time.Duration(len(inputIDs)), assertionWindows, requireSegmentBoundaryContinuity, requirePlaylistPacing)
}

func waitForAVGrowth(t *testing.T, dest *buffering, timeout time.Duration, minVideoDelta, minAudioDelta int) {
	t.Helper()

	startVideo := len(dest.GetVideoFrames())
	startAudio := len(dest.GetAudioFrames())
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		videoDelta := len(dest.GetVideoFrames()) - startVideo
		audioDelta := len(dest.GetAudioFrames()) - startAudio
		if videoDelta >= minVideoDelta && audioDelta >= minAudioDelta {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	videoDelta := len(dest.GetVideoFrames()) - startVideo
	audioDelta := len(dest.GetAudioFrames()) - startAudio
	t.Fatalf("expected AV growth within %v, got video delta=%d audio delta=%d", timeout, videoDelta, audioDelta)
}

func waitForRTMPOutputDecode(t *testing.T, outputURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		cmd := exec.CommandContext(ctx, "ffmpeg",
			"-v", "error",
			"-i", outputURL,
			"-frames:v", "60",
			"-frames:a", "100",
			"-f", "null", "-",
		)
		out, err := cmd.CombinedOutput()
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		_ = out
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("rtmp output %q was not decodable within %v: last error=%v", outputURL, timeout, lastErr)
}

func assertFinalHLSSwitchOutput(t *testing.T, outDir, playlistPath string, minSegments int, expectedDuration time.Duration, windows []videoAssertionWindow, requireSegmentBoundaryContinuity bool, requirePlaylistPacing bool) {
	t.Helper()

	waitForHLSArtifacts(t, outDir, 5*time.Second, minSegments)
	assertHLSPlaylistLooksValid(t, playlistPath)

	segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	sort.Strings(segmentFiles)

	if len(segmentFiles) < minSegments {
		t.Fatalf("expected at least %d finalized HLS segments, got %d", minSegments, len(segmentFiles))
	}

	content, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("read playlist %s: %v", playlistPath, err)
	}
	text := string(content)
	if !strings.Contains(text, "#EXTM3U") || !strings.Contains(text, "#EXTINF") {
		t.Fatalf("playlist %s is not yet valid", playlistPath)
	}
	if err := checkSegmentStartsIncrease(segmentFiles); err != nil {
		t.Fatal(err)
	}
	if requireSegmentBoundaryContinuity {
		if err := checkSegmentBoundaryContinuity(segmentFiles); err != nil {
			t.Fatal(err)
		}
	}
	if err := checkTransportStreamPackets(playlistPath); err != nil {
		t.Fatal(err)
	}
	if requirePlaylistPacing {
		if err := checkPlaybackPacingFromProbe(playlistPath, expectedDuration); err != nil {
			t.Fatal(err)
		}
	}
	if len(windows) > 0 {
		if err := checkWindowedVideoReplayAgainstInput(segmentFiles, playlistPath, windows); err != nil {
			t.Fatal(err)
		}
		if err := checkWindowedOutputVideoCadenceAgainstInput(playlistPath, windows); err != nil {
			t.Fatal(err)
		}
	}
	for _, segmentPath := range segmentFiles {
		if err := checkTransportStreamPackets(segmentPath); err != nil {
			t.Fatal(err)
		}
	}
}

func checkSegmentStartsIncrease(segments []string) error {
	if len(segments) < 2 {
		return nil
	}

	prev, err := firstPacketPTSSecondsCheck(segments[0])
	if err != nil {
		return err
	}
	for _, segmentPath := range segments[1:] {
		next, err := firstPacketPTSSecondsCheck(segmentPath)
		if err != nil {
			return err
		}
		if next <= prev {
			return fmt.Errorf("segment timeline did not increase: previous=%s (%f) next=%s (%f)", segments[0], prev, segmentPath, next)
		}
		prev = next
	}
	return nil
}

func buildVideoAssertionWindows(frames []*Frame, includeInputIDs map[string]bool) []videoAssertionWindow {
	windows := make([]videoAssertionWindow, 0, 4)
	var current *videoAssertionWindow

	flush := func() {
		if current == nil || len(current.hashes) == 0 || len(current.pts) == 0 {
			current = nil
			return
		}
		windows = append(windows, *current)
		current = nil
	}

	for _, frame := range frames {
		if frame == nil || !includeInputIDs[frame.InputID] {
			flush()
			continue
		}

		pts := frame.PTS.Seconds()
		if current == nil || current.inputID != frame.InputID {
			flush()
			current = &videoAssertionWindow{
				inputID: frame.InputID,
				start:   pts,
			}
		}

		current.end = pts
		current.hashes = append(current.hashes, frameHash(frame))
		current.pts = append(current.pts, pts)
	}
	flush()

	return windows
}

func checkWindowedVideoReplayAgainstInput(segmentFiles []string, playlistPath string, windows []videoAssertionWindow) error {
	probe, err := dumpFrames(playlistPath)
	if err != nil {
		return fmt.Errorf("dumpFrames failed on %s: %w", playlistPath, err)
	}
	outputVideoPackets, _ := splitPacketsByType(probe.Packets)

	for _, window := range windows {
		if len(window.hashes) < 10 {
			continue
		}
		windowPackets := filterPacketsToWindow(outputVideoPackets, window)
		outputVideoHashes := packetHashes(windowPackets)
		if len(outputVideoHashes) == 0 {
			return fmt.Errorf("no output video hashes collected from %s for window %s %.3f-%.3f", playlistPath, window.inputID, window.start, window.end)
		}

		inputDupRatio, inputMaxRun := duplicateStats(window.hashes)
		outputDupRatio, outputMaxRun := duplicateStats(outputVideoHashes)
		if outputMaxRun > inputMaxRun+2 && outputMaxRun > 3 {
			return fmt.Errorf("output consecutive duplicate video run too large for input %s: input max=%d output max=%d", window.inputID, inputMaxRun, outputMaxRun)
		}
		if outputDupRatio > inputDupRatio+0.10 && outputDupRatio > 0.12 {
			return fmt.Errorf("output duplicate video ratio too large for input %s: input=%.3f output=%.3f", window.inputID, inputDupRatio, outputDupRatio)
		}

		for i := 0; i < len(segmentFiles)-1; i++ {
			currentProbe, err := dumpFrames(segmentFiles[i])
			if err != nil {
				return fmt.Errorf("dumpFrames failed on %s: %w", segmentFiles[i], err)
			}
			nextProbe, err := dumpFrames(segmentFiles[i+1])
			if err != nil {
				return fmt.Errorf("dumpFrames failed on %s: %w", segmentFiles[i+1], err)
			}
			currentVideoPackets, _ := splitPacketsByType(currentProbe.Packets)
			nextVideoPackets, _ := splitPacketsByType(nextProbe.Packets)
			currentHashes := packetHashes(filterPacketsToWindow(currentVideoPackets, window))
			nextHashes := packetHashes(filterPacketsToWindow(nextVideoPackets, window))
			if len(currentHashes) == 0 || len(nextHashes) == 0 {
				continue
			}
			overlap := boundaryReplayOverlap(currentHashes, nextHashes, 6)
			if overlap > 2 {
				return fmt.Errorf("video boundary replay overlap too large for input %s: %s -> %s overlap=%d", window.inputID, segmentFiles[i], segmentFiles[i+1], overlap)
			}
		}
	}

	return nil
}

func checkWindowedOutputVideoCadenceAgainstInput(playlistPath string, windows []videoAssertionWindow) error {
	probe, err := dumpFrames(playlistPath)
	if err != nil {
		return fmt.Errorf("dumpFrames failed on %s: %w", playlistPath, err)
	}
	outputVideoPackets, _ := splitPacketsByType(probe.Packets)

	for _, window := range windows {
		if len(window.pts) < 10 {
			continue
		}
		windowPackets := filterPacketsToWindow(outputVideoPackets, window)
		outputPTS := collectPacketTimes(windowPackets, func(packet Packet) flexString { return packet.PtsTime })
		if len(outputPTS) < 10 {
			return fmt.Errorf("need at least 10 output video pts samples for input %s, got %d", window.inputID, len(outputPTS))
		}

		minCount := int(float64(len(window.pts)) * 0.90)
		maxCount := int(float64(len(window.pts))*1.10) + 2
		if len(outputPTS) < minCount || len(outputPTS) > maxCount {
			return fmt.Errorf("output video packet count drift too large for input %s: input=%d output=%d", window.inputID, len(window.pts), len(outputPTS))
		}

		inputAvgGap, inputMaxGap := packetGapStats(window.pts)
		outputAvgGap, outputMaxGap := packetGapStats(outputPTS)
		inputP95 := percentileGap(window.pts, 0.95)
		outputP95 := percentileGap(outputPTS, 0.95)

		if outputAvgGap > inputAvgGap+0.012 {
			return fmt.Errorf("output video avg gap too large vs input %s: input=%.3fs output=%.3fs", window.inputID, inputAvgGap, outputAvgGap)
		}
		if outputP95 > inputP95+0.020 {
			return fmt.Errorf("output video p95 gap too large vs input %s: input=%.3fs output=%.3fs", window.inputID, inputP95, outputP95)
		}

		maxAllowedGap := inputMaxGap + 0.050
		if maxAllowedGap < inputAvgGap*2.5 {
			maxAllowedGap = inputAvgGap * 2.5
		}
		if outputMaxGap > maxAllowedGap {
			return fmt.Errorf("output video max gap too large vs input %s: input=%.3fs output=%.3fs allowed=%.3fs", window.inputID, inputMaxGap, outputMaxGap, maxAllowedGap)
		}

		largeGapLimit := inputP95 + 0.020
		largeGapCount := countGapsAbove(outputPTS, largeGapLimit)
		if largeGapCount > 1 {
			return fmt.Errorf("output video has too many large gaps vs input %s baseline: limit=%.3fs count=%d", window.inputID, largeGapLimit, largeGapCount)
		}
	}

	return nil
}

func filterPacketsToWindow(packets []Packet, window videoAssertionWindow) []Packet {
	filtered := make([]Packet, 0, len(packets))
	start := window.start - 0.05
	end := window.end + 0.05
	for _, packet := range packets {
		pts, ok := parseFlexFloat(packet.PtsTime)
		if !ok {
			continue
		}
		if pts < start || pts > end {
			continue
		}
		filtered = append(filtered, packet)
	}
	return filtered
}

func firstPacketPTSSecondsCheck(segmentPath string) (float64, error) {
	cmd := exec.Command(
		"ffprobe",
		"-v", "error",
		"-show_entries", "packet=pts_time",
		"-of", "csv=p=0",
		"-read_intervals", "%+#1",
		segmentPath,
	)
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("ffprobe failed on %s: %w", segmentPath, err)
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return 0, fmt.Errorf("ffprobe returned no packet pts for %s", segmentPath)
	}

	for _, field := range strings.Split(line, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		v, parseErr := strconv.ParseFloat(field, 64)
		if parseErr == nil {
			return v, nil
		}
	}

	return 0, fmt.Errorf("failed to parse first packet pts from %s: %q", segmentPath, line)
}

func checkTransportStreamPackets(segmentPath string) error {
	probe, err := dumpFrames(segmentPath)
	if err != nil {
		return fmt.Errorf("dumpFrames failed on %s: %w", segmentPath, err)
	}

	var hasH264 bool
	var hasAAC bool
	for _, stream := range probe.Streams {
		switch stream.CodecType {
		case "video":
			if strings.Contains(strings.ToLower(stream.CodecName), "h264") {
				hasH264 = true
			}
		case "audio":
			if strings.Contains(strings.ToLower(stream.CodecName), "aac") {
				hasAAC = true
			}
		}
	}
	if !hasH264 {
		return fmt.Errorf("expected h264 video stream in %s", segmentPath)
	}
	if !hasAAC {
		return fmt.Errorf("expected aac audio stream in %s", segmentPath)
	}

	videoPackets, audioPackets := splitPacketsByType(probe.Packets)
	if len(videoPackets) == 0 {
		return fmt.Errorf("expected video packets in %s", segmentPath)
	}
	if len(audioPackets) == 0 {
		return fmt.Errorf("expected audio packets in %s", segmentPath)
	}

	if err := checkPacketTimeline(videoPackets, "video", segmentPath); err != nil {
		return err
	}
	if err := checkPacketTimeline(audioPackets, "audio", segmentPath); err != nil {
		return err
	}

	return nil
}

func checkPacketTimeline(packets []Packet, codecType, segmentPath string) error {
	var lastPTS float64
	var lastDTS float64
	var hasPTS bool
	var hasDTS bool

	for i, packet := range packets {
		if pts, ok := parseFlexFloat(packet.PtsTime); ok {
			if hasPTS && pts < lastPTS {
				return fmt.Errorf("%s packet pts moved backwards in %s at index %d: prev=%f next=%f", codecType, segmentPath, i, lastPTS, pts)
			}
			lastPTS = pts
			hasPTS = true
		}
		if dts, ok := parseFlexFloat(packet.DtsTime); ok {
			if hasDTS && dts < lastDTS {
				return fmt.Errorf("%s packet dts moved backwards in %s at index %d: prev=%f next=%f", codecType, segmentPath, i, lastDTS, dts)
			}
			lastDTS = dts
			hasDTS = true
		}
	}

	if !hasPTS {
		return fmt.Errorf("no %s pts_time values found in %s", codecType, segmentPath)
	}

	return nil
}

func parseFlexFloat(v flexString) (float64, bool) {
	s := strings.TrimSpace(string(v))
	if s == "" || s == "N/A" {
		return 0, false
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}

	return f, true
}

func checkTransportStreamDecodableAV(segmentPath string) error {
	if err := runFFmpegDecodeCheck(
		"ffmpeg decode TS video "+filepath.Base(segmentPath),
		[]string{
			"-v", "error",
			"-xerror",
			"-i", segmentPath,
			"-map", "0:v:0",
			"-frames:v", "40",
			"-f", "null",
			"-",
		},
		15*time.Second,
	); err != nil {
		return err
	}

	if err := runFFmpegDecodeCheck(
		"ffmpeg decode TS audio "+filepath.Base(segmentPath),
		[]string{
			"-v", "error",
			"-xerror",
			"-i", segmentPath,
			"-map", "0:a:0",
			"-frames:a", "100",
			"-f", "null",
			"-",
		},
		15*time.Second,
	); err != nil {
		return err
	}

	return nil
}

func runFFmpegDecodeCheck(label string, args []string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("%s failed: %w\n%s", label, err, stderr.String())
	}
	if stderr.Len() > 0 {
		return fmt.Errorf("%s produced stderr output:\n%s", label, stderr.String())
	}
	return nil
}
