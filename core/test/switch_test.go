package test

import (
	"context"
	"fmt"
	corehelpers "github.com/tupicapp/restreamer/core"
	streaminputs "github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/outputs"
	testtools "github.com/tupicapp/restreamer/core/test_tools"
	"testing"
	"time"
)

// TestStreamer_SwitchBetweenInputs tests switching between HLS and RTMP inputs
// and verifies that the output stream is healthy with correct GOP_IDs
func TestStreamer_SwitchBetweenInputs(t *testing.T) {
	// Inputs required by this test
	hlsM4S := TestVideoConfig{
		Name:        "hls_m4s",
		FilePath:    getConfiguredHLSFixtureURL("testdata/stream.m3u8"),
		Description: "Primary HLS test fixture",
	}
	hlsTS := TestVideoConfig{
		Name:        "hls_ts",
		FilePath:    getConfiguredHLSFixtureURL("testdata/stream.m3u8"),
		Description: "Primary HLS test fixture",
	}
	// hlsLiveURL := getConfiguredHLSLiveURL(t)
	// requireHTTPReachable(t, hlsLiveURL, 10*time.Second)

	rtmpURL := getConfiguredRTMPURL(t)
	requireRTMPPublishing(t, rtmpURL, 10*time.Second)
	if base := getRTMPBaseURL(t, rtmpURL); base != "" {
		t.Setenv("HLS_READER_LIVE_FFMPEG_RTMP_URL", base)
	}

	// t.Logf("Test HLS live URI: %s", hlsLiveURL)
	t.Logf("Test RTMP URL: %s", rtmpURL)

	// Test all combinations of streamer parameters
	combinations := []struct {
		name        string
		rateControl bool
		genPTS      bool
		ptsFilter   bool
	}{
		{"RateControl=true_genPTS=true_PTSFilter=true", true, true, true},
	}

	for _, combo := range combinations {
		testName := fmt.Sprintf("%s_OrderedSwitches", combo.name)
		t.Run(testName, func(t *testing.T) {

			hlsM4sInput := "hls-m4s-input"
			hlsTsInput := "hls-ts-input"
			// hlsLiveInput := "hls-live-input"
			rtmpInput := "rtmp-input"

			// hlsLiveStream := NewHLSLive(hlsLiveInput, hlsLiveURL)
			// hlsLiveID := hlsLiveStream.GetID()

			// Create inputs
			inputs := []inputSpec{
				{
					id:       hlsM4sInput,
					label:    "HLS M4S",
					stream:   streaminputs.NewHLS(hlsM4sInput, hlsM4S.FilePath),
					waitTime: 10 * time.Second,
				},
				{
					id:       hlsTsInput,
					label:    "HLS TS",
					stream:   streaminputs.NewHLS(hlsTsInput, hlsTS.FilePath, streaminputs.OptionWithRealTime(true)),
					waitTime: 10 * time.Second,
				},
				// {
				// 	id:       hlsLiveID,
				// 	label:    "HLS live",
				// 	stream:   hlsLiveStream,
				// 	waitTime: 30 * time.Second,
				// },
				{
					id:       rtmpInput,
					label:    "RTMP",
					stream:   streaminputs.NewRTMP(rtmpInput, rtmpURL),
					waitTime: 5 * time.Second,
				},
			}

			switches := []SwitchOperation{
				{InputID: rtmpInput, At: 0 * time.Second, Duration: 10 * time.Second},
				// {InputID: hlsLiveID, At: 10 * time.Second, Duration: 10 * time.Second},
				{InputID: hlsTsInput, At: 10 * time.Second, Duration: 5 * time.Second},
				{InputID: rtmpInput, At: 15 * time.Second, Duration: 5 * time.Second},
				// {InputID: hlsM4sInput, At: 30 * time.Second, Duration: 5 * time.Second},
				// {InputID: rtmpInput, At: 35 * time.Second, Duration: 5 * time.Second},
				// {InputID: hlsTsInput, At: 40 * time.Second, Duration: 5 * time.Second},
				// {InputID: rtmpInput, At: 45 * time.Second, Duration: 5 * time.Second},
			}
			testSwitchBetweenInputs(t, inputs, combo.rateControl, combo.genPTS,
				combo.ptsFilter, switches, "Switch between HLS live, HLS M4S, HLS TS, and RTMP")
		})
	}
}

// SwitchOperation represents a single input switch operation
type SwitchOperation struct {
	InputID  string
	At       time.Duration
	Duration time.Duration
}

type inputSpec struct {
	id       string
	stream   Stream
	label    string
	waitTime time.Duration
}

type mockWriter struct {
	videoFrames []*Frame
	audioFrames []*Frame
}

func (m *mockWriter) Write(f *Frame) error {
	switch f.Codec {
	case "h264":
		m.videoFrames = append(m.videoFrames, f)

	case "aac":
		m.audioFrames = append(m.audioFrames, f)
	default:
		return fmt.Errorf("unsupported codec: %s", f.Codec)
	}
	return nil
}

func (m *mockWriter) GetVideoFrames() []*Frame {
	return m.videoFrames
}

func (m *mockWriter) GetAudioFrames() []*Frame {
	return m.audioFrames
}

func testSwitchBetweenInputs(t *testing.T, inputs []inputSpec,
	rateControl, genPTS, ptsFilter bool, switches []SwitchOperation, description string) {

	t.Logf("Testing: %s", description)
	t.Logf("Parameters: RateControl=%v, genPTS=%v, PTSFilter=%v", rateControl, genPTS, ptsFilter)

	// Create streamer with specified parameters
	streamer := corehelpers.NewStreamer()
	defer streamer.Close()

	streamer.StartLife()

	// Create buffering destination
	bufferingDest := outputs.NewBuffering("buffering-dest-1")

	// Update streamer with both inputs and output
	streams := make([]Stream, 0, len(inputs))
	for _, input := range inputs {
		streams = append(streams, input.stream)
	}

	err := streamer.UpdateStreams(streams, []Stream{bufferingDest})
	if err != nil {
		t.Fatalf("Failed to update streams: %v", err)
	}

	// Start the streamer
	streamer.Start()

	// Wait for inputs to be ready
	availableInputs := map[string]bool{}

	for _, input := range inputs {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), input.waitTime)
		waitErr := input.stream.WaitForStart(waitCtx)
		waitCancel()
		if waitErr == nil {
			availableInputs[input.id] = true
			continue
		}
		t.Logf("%s input not available: %v", input.label, waitErr)
	}

	filteredSwitches := []SwitchOperation{}
	for _, sw := range switches {
		if availableInputs[sw.InputID] {
			filteredSwitches = append(filteredSwitches, sw)
		}
	}

	if len(filteredSwitches) == 0 {
		t.Skip("No valid switches available (no inputs started)")
	}
	switches = normalizeSwitchOperations(filteredSwitches)
	if countDistinctSwitchInputs(switches) < 2 {
		t.Skip("Need at least two available inputs to verify switching behavior")
	}

	// Keep the test within the remaining deadline if provided.
	if deadline, ok := t.Deadline(); ok {
		safety := 2 * time.Second
		remaining := time.Until(deadline) - safety
		if remaining > 0 {
			expected := time.Duration(0)
			for _, sw := range switches {
				expected += sw.Duration
			}
			expected += 2 * time.Second
			if expected > remaining {
				factor := float64(remaining) / float64(expected)
				if factor < 0.1 {
					factor = 0.1
				}
				scaled := make([]SwitchOperation, 0, len(switches))
				acc := time.Duration(0)
				for _, sw := range switches {
					newDur := time.Duration(float64(sw.Duration) * factor)
					if newDur < time.Second {
						newDur = time.Second
					}
					scaled = append(scaled, SwitchOperation{
						InputID:  sw.InputID,
						At:       acc,
						Duration: newDur,
					})
					acc += newDur
				}
				switches = scaled
			}
		}
	}

	// Record start time for timing checks
	startTime := time.Now()

	// Perform switches and track switch events
	switchEvents := []SwitchEvent{}
	t.Logf("Performing %d switches...", len(switches))
	for i, sw := range switches {
		targetTime := startTime.Add(sw.At)
		if delay := time.Until(targetTime); delay > 0 {
			time.Sleep(delay)
		}

		// Get current frames to find the last PTS before switch
		currentFrames := bufferingDest.GetVideoFrames()
		lastPTSBeforeSwitch := time.Duration(0)
		if len(currentFrames) > 0 {
			lastPTSBeforeSwitch = currentFrames[len(currentFrames)-1].PTS
		}

		switchTime := time.Now()
		t.Logf("Switch %d: Switching to %s at %v for %v (current PTS: %v)", i+1, sw.InputID, sw.At, sw.Duration, lastPTSBeforeSwitch)
		streamer.Switch(sw.InputID)

		// Record switch event
		switchEvents = append(switchEvents, SwitchEvent{
			SwitchIndex:      i + 1,
			TargetInputID:    sw.InputID,
			SwitchTime:       switchTime,
			PTSBeforeSwitch:  lastPTSBeforeSwitch,
			ExpectedDuration: sw.Duration,
		})

		// Wait for the duration of this input
		time.Sleep(sw.Duration)
	}

	// Wait a bit more for final frames to be processed
	// finalSleep := 1 * time.Second
	// if deadline, ok := t.Deadline(); ok {
	// 	remaining := time.Until(deadline) - time.Second
	// 	if remaining > 0 && remaining < finalSleep {
	// 		finalSleep = remaining
	// 	}
	// }
	// time.Sleep(finalSleep)

	actualTestDuration := time.Since(startTime)

	// Get final frames from destination
	destVideoFrames := bufferingDest.GetVideoFrames()
	destAudioFrames := bufferingDest.GetAudioFrames()

	t.Logf("Collected frames: video=%d, audio=%d", len(destVideoFrames), len(destAudioFrames))

	if len(destVideoFrames) == 0 && len(destAudioFrames) == 0 {
		t.Fatal("No frames collected from destination")
	}

	// Check stream health
	videoHealth := testtools.CheckStreamHealth(destVideoFrames, "video")
	audioHealth := testtools.CheckStreamHealth(destAudioFrames, "audio")

	testtools.PrintStreamHealth(t, videoHealth, "video")
	testtools.PrintStreamHealth(t, audioHealth, "audio")

	// Verify stream is healthy
	if !videoHealth.IsHealthy {
		t.Errorf("Video stream is not healthy: Monotonic PTS: %.2f%%, Monotonic DTS: %.2f%%, Valid Gaps: %.2f%%, Valid DTS: %.2f%%",
			videoHealth.MonotonicPTSPercent, videoHealth.MonotonicDTSPercent, videoHealth.ValidGapPercent, videoHealth.DTSValidPercent)
	}

	if !audioHealth.IsHealthy {
		t.Errorf("Audio stream is not healthy: Monotonic PTS: %.2f%%, Monotonic DTS: %.2f%%, Valid Gaps: %.2f%%, Valid DTS: %.2f%%",
			audioHealth.MonotonicPTSPercent, audioHealth.MonotonicDTSPercent, audioHealth.ValidGapPercent, audioHealth.DTSValidPercent)
	}

	// Verify DTS monotonicity
	if len(videoHealth.MonotonicDTSIssues) > 0 {
		t.Errorf("Video stream has %d frames where DTS is not monotonic (should never happen)", len(videoHealth.MonotonicDTSIssues))
	}
	if len(audioHealth.MonotonicDTSIssues) > 0 {
		t.Errorf("Audio stream has %d frames where DTS is not monotonic (should never happen)", len(audioHealth.MonotonicDTSIssues))
	}
	if videoHealth.MonotonicDTSPercent < 100.0 {
		t.Errorf("Video stream DTS monotonicity: %.2f%% (expected 100%%)", videoHealth.MonotonicDTSPercent)
	}
	if audioHealth.MonotonicDTSPercent < 100.0 {
		t.Errorf("Audio stream DTS monotonicity: %.2f%% (expected 100%%)", audioHealth.MonotonicDTSPercent)
	}

	// Verify DTS <= PTS for all frames
	if len(videoHealth.DTSIssues) > 0 {
		t.Errorf("Video stream has %d frames where DTS > PTS (should never happen)", len(videoHealth.DTSIssues))
	}
	if len(audioHealth.DTSIssues) > 0 {
		t.Errorf("Audio stream has %d frames where DTS > PTS (should never happen)", len(audioHealth.DTSIssues))
	}
	if videoHealth.DTSValidPercent < 100.0 {
		t.Errorf("Video stream DTS validation: %.2f%% (expected 100%%)", videoHealth.DTSValidPercent)
	}
	if audioHealth.DTSValidPercent < 100.0 {
		t.Errorf("Audio stream DTS validation: %.2f%% (expected 100%%)", audioHealth.DTSValidPercent)
	}

	// Verify GOP_ID correctness for video frames
	testtools.VerifyGOPIDCorrectness(t, destVideoFrames)

	// Check and print InputID changes
	testtools.CheckInputIDChanges(t, destVideoFrames, "video")
	// checkInputIDChanges(t, destAudioFrames, "audio")

	// os.Exit(1)

	// Measure switch latency
	testtools.MeasureSwitchLatency(t, destVideoFrames, destAudioFrames, switchEvents)

	// Ensure no drops while inputs are active (sequence continuity within each active window)
	checkNoDropsDuringActive(t, destVideoFrames, switchEvents, "video")
	checkNoDropsDuringActive(t, destAudioFrames, switchEvents, "audio")

	// Additional checks similar to RTMP timing test
	// Calculate actual test duration (wall-clock time)

	t.Logf("\n=== Additional Checks (Similar to RTMP Timing Test) ===")
	t.Logf("Actual test duration (wall-clock time): %v", actualTestDuration)

	// Check frame timing (PTS window should match actual elapsed time)
	// Use a more lenient threshold (20%) for switch test since switching can cause timing variations
	threshold := 0.2
	if len(destVideoFrames) > 0 {
		testtools.CheckFrameTiming(t, destVideoFrames, "video", actualTestDuration, actualTestDuration, threshold)
		// checkSequenceIDContinuity(t, destVideoFrames, "video")
		testtools.CheckH264FrameHealth(t, destVideoFrames)
	}

	if len(destAudioFrames) > 0 {
		testtools.CheckFrameTiming(t, destAudioFrames, "audio", actualTestDuration, actualTestDuration, threshold)
		// checkSequenceIDContinuity(t, destAudioFrames, "audio")
	}

	// DTS checks (same as PTS)
	// dtsVideoFrames := cloneFramesWithDTSAsPTS(destVideoFrames)
	// dtsAudioFrames := cloneFramesWithDTSAsPTS(destAudioFrames)

	// if len(dtsVideoFrames) > 0 {
	// 	testtools.CheckFrameTiming(t, dtsVideoFrames, "video-dts", actualTestDuration, actualTestDuration, threshold)
	// 	checkSequenceIDContinuity(t, dtsVideoFrames, "video-dts")
	// }

	// if len(dtsAudioFrames) > 0 {
	// 	testtools.CheckFrameTiming(t, dtsAudioFrames, "audio-dts", actualTestDuration, actualTestDuration, threshold)
	// 	checkSequenceIDContinuity(t, dtsAudioFrames, "audio-dts")
	// }
}

func checkNoDropsDuringActive(t *testing.T, frames []*Frame, switchEvents []SwitchEvent, frameType string) {
	if len(frames) == 0 || len(switchEvents) == 0 {
		return
	}

	t.Logf("\n=== %s Active-Window Drop Check ===", frameType)
	for _, event := range switchEvents {
		windowStart := event.SwitchTime
		windowEnd := event.SwitchTime.Add(event.ExpectedDuration)

		windowFrames := make([]*Frame, 0)
		for _, frame := range frames {
			if frame == nil || frame.InputID != event.TargetInputID {
				continue
			}
			if frame.Timestamp.IsZero() {
				continue
			}
			if !frame.Timestamp.Before(windowStart) && frame.Timestamp.Before(windowEnd) {
				windowFrames = append(windowFrames, frame)
			}
		}

		if len(windowFrames) == 0 {
			t.Errorf("%s: no frames for input '%s' during active window %v..%v",
				frameType, event.TargetInputID, windowStart.Sub(switchEvents[0].SwitchTime), windowEnd.Sub(switchEvents[0].SwitchTime))
			continue
		}

		t.Logf("\n--- %s InputID: %s (window %v..%v, frames=%d) ---",
			frameType, event.TargetInputID,
			windowStart.Sub(switchEvents[0].SwitchTime),
			windowEnd.Sub(switchEvents[0].SwitchTime),
			len(windowFrames))
		testtools.CheckSequenceIDContinuityForInput(t, windowFrames, frameType, event.TargetInputID)
	}
}

// checkInputIDChanges detects and prints when InputID changes in the frame sequence

// TestStreamer_HLSReaderTiming verifies that HLS reader timing matches elapsed time
// It checks that if streamer runs for a specified duration, the PTS window of packets matches elapsed time
func TestStreamer_HLSReaderTiming(t *testing.T) {
	playlistURI, _, cleanup := setupDeterministicLiveFixtureServer(t, 15*time.Second)
	defer cleanup()

	// List of HLS videos to test
	hlsVideos := []TestVideoConfig{
		{
			Name:        "hls_video_1",
			FilePath:    playlistURI,
			Description: "Deterministic HLS timing fixture",
			Skip:        false,
		},
		// Add more videos here as needed
	}

	// Test all combinations of streamer parameters
	combinations := []struct {
		name        string
		rateControl bool
		genPTS      bool
		ptsFilter   bool
	}{
		{"RateControl=true_genPTS=true_PTSFilter=true", true, true, true},
	}

	// Test each video with each parameter combination
	for _, video := range hlsVideos {
		t.Run(video.Name, func(t *testing.T) {
			t.Logf("Testing HLS video: %s (%s)", video.Name, video.Description)
			t.Logf("Test HLS URI: %s", video.FilePath)

			for _, combo := range combinations {
				t.Run(combo.name, func(t *testing.T) {
					testHLSReaderTiming(t, combo.rateControl, combo.genPTS, combo.ptsFilter, video.FilePath)
				})
			}
		})
	}
}

func testHLSReaderTiming(t *testing.T, rateControl, genPTS, ptsFilter bool, playlistURI string) {
	// Duration to run the streamer
	runDuration := 5 * time.Second
	// Threshold for timing checks (10% tolerance)
	threshold := 0.1 // 10%
	maxSegmentLead := 2 * time.Second

	t.Logf("Testing HLS reader timing: RateControl=%v, genPTS=%v, PTSFilter=%v", rateControl, genPTS, ptsFilter)
	t.Logf("Running streamer for %v", runDuration)

	// Create streamer with specified parameters
	streamer := corehelpers.NewStreamer()
	defer streamer.Close()

	streamer.StartLife()

	inputID := "hls-input-1"

	// Create HLS reader input
	hlsInput := streaminputs.NewHLS(inputID, playlistURI, streaminputs.OptionWithRealTime(true))

	// Create buffering destination
	bufferingDest := outputs.NewBuffering("buffering-dest-1")

	// Update streamer with input and output
	err := streamer.UpdateStreams([]Stream{hlsInput}, []Stream{bufferingDest})
	if err != nil {
		t.Fatalf("Failed to update streams: %v", err)
	}

	// Start the streamer
	streamer.Start()
	streamer.Switch(inputID)

	// Wait for input to start
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = hlsInput.WaitForStart(ctx)
	if err != nil {
		t.Fatalf("HLS input failed to start: %v", err)
	}

	// Wait for output to start
	err = bufferingDest.WaitForStart(ctx)
	if err != nil {
		t.Fatalf("Buffering destination failed to start: %v", err)
	}

	// Record start time
	startVideoIndex := len(bufferingDest.GetVideoFrames())
	startAudioIndex := len(bufferingDest.GetAudioFrames())
	startTime := time.Now()

	// Run for the specified duration
	t.Logf("Collecting frames for %v...", runDuration)
	time.Sleep(runDuration)

	// Record end time
	endTime := time.Now()
	actualElapsed := endTime.Sub(startTime)

	hlsInput.Close()

	// Wait a bit more for final frames to be processed
	time.Sleep(500 * time.Millisecond)

	// Get frames from destination
	allVideoFrames := bufferingDest.GetVideoFrames()
	allAudioFrames := bufferingDest.GetAudioFrames()

	destVideoFrames := allVideoFrames
	if startVideoIndex < len(allVideoFrames) {
		destVideoFrames = allVideoFrames[startVideoIndex:]
	} else {
		destVideoFrames = nil
	}

	destAudioFrames := allAudioFrames
	if startAudioIndex < len(allAudioFrames) {
		destAudioFrames = allAudioFrames[startAudioIndex:]
	} else {
		destAudioFrames = nil
	}

	t.Logf("Collected frames: video=%d, audio=%d", len(destVideoFrames), len(destAudioFrames))
	t.Logf("Actual elapsed time: %v", actualElapsed)

	if len(destVideoFrames) == 0 && len(destAudioFrames) == 0 {
		t.Fatal("No frames collected from destination")
	}

	// Check video timing
	if len(destVideoFrames) > 0 {
		checkSegmentedFrameTiming(t, destVideoFrames, "video", runDuration, actualElapsed, threshold, maxSegmentLead)
		testtools.CheckSequenceIDContinuity(t, destVideoFrames, "video")
		testtools.CheckH264FrameHealth(t, destVideoFrames)
	}

	// Check audio timing
	if len(destAudioFrames) > 0 {
		checkSegmentedFrameTiming(t, destAudioFrames, "audio", runDuration, actualElapsed, threshold, maxSegmentLead)
		testtools.CheckSequenceIDContinuity(t, destAudioFrames, "audio")
	}

	// DTS checks (same as PTS)
	dtsVideoFrames := cloneFramesWithDTSAsPTS(destVideoFrames)
	dtsAudioFrames := cloneFramesWithDTSAsPTS(destAudioFrames)

	if len(dtsVideoFrames) > 0 {
		checkSegmentedFrameTiming(t, dtsVideoFrames, "video-dts", runDuration, actualElapsed, threshold, maxSegmentLead)
		testtools.CheckSequenceIDContinuity(t, dtsVideoFrames, "video-dts")
	}

	if len(dtsAudioFrames) > 0 {
		checkSegmentedFrameTiming(t, dtsAudioFrames, "audio-dts", runDuration, actualElapsed, threshold, maxSegmentLead)
		testtools.CheckSequenceIDContinuity(t, dtsAudioFrames, "audio-dts")
	}
}

func checkSegmentedFrameTiming(t *testing.T, frames []*Frame, frameType string, expectedDuration, actualElapsed time.Duration, threshold float64, maxLead time.Duration) {
	t.Helper()

	if len(frames) == 0 {
		return
	}

	minPTS := frames[0].PTS
	maxPTS := frames[0].PTS
	for _, frame := range frames[1:] {
		if frame == nil {
			continue
		}
		if frame.PTS < minPTS {
			minPTS = frame.PTS
		}
		if frame.PTS > maxPTS {
			maxPTS = frame.PTS
		}
	}

	ptsWindow := maxPTS - minPTS
	minExpected := time.Duration(float64(expectedDuration) * (1 - threshold))
	maxExpected := actualElapsed + maxLead

	t.Logf("\n=== %s Segmented Timing Check ===", frameType)
	t.Logf("Actual elapsed time: %v", actualElapsed)
	t.Logf("Expected duration: %v", expectedDuration)
	t.Logf("PTS window: %v (from %v to %v)", ptsWindow, minPTS, maxPTS)
	t.Logf("Allowed window: [%v, %v]", minExpected, maxExpected)

	if ptsWindow < minExpected {
		t.Errorf("%s PTS window %v is shorter than expected lower bound %v", frameType, ptsWindow, minExpected)
	}
	if ptsWindow > maxExpected {
		t.Errorf("%s PTS window %v exceeds segmented upper bound %v", frameType, ptsWindow, maxExpected)
	}
}

func normalizeSwitchOperations(ops []SwitchOperation) []SwitchOperation {
	if len(ops) == 0 {
		return nil
	}

	normalized := make([]SwitchOperation, 0, len(ops))
	acc := time.Duration(0)

	for _, op := range ops {
		if len(normalized) > 0 && normalized[len(normalized)-1].InputID == op.InputID {
			normalized[len(normalized)-1].Duration += op.Duration
			acc += op.Duration
			continue
		}

		normalized = append(normalized, SwitchOperation{
			InputID:  op.InputID,
			At:       acc,
			Duration: op.Duration,
		})
		acc += op.Duration
	}

	return normalized
}

func countDistinctSwitchInputs(ops []SwitchOperation) int {
	seen := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		if op.InputID == "" {
			continue
		}
		seen[op.InputID] = struct{}{}
	}
	return len(seen)
}

// TestStreamer_RTMPReaderTiming verifies that RTMP reader timing matches elapsed time
// It checks that if streamer runs for 5 seconds, the PTS window of packets is also ~5 seconds
func TestStreamer_RTMPReaderTiming(t *testing.T) {
	// List of RTMP videos to test

	// Set RTMP URL from config/env if available
	rtmpURL := getConfiguredRTMPURL(t)
	if rtmpURL == "" {
		t.Skip("RTMP_URL environment variable not set, skipping test")
	}
	requireRTMPPublishing(t, rtmpURL, 10*time.Second)

	rtmpVideos := []TestVideoConfig{
		{
			Name:        "rtmp_env",
			FilePath:    rtmpURL, // Will be set from environment variable
			Description: "RTMP URL from RTMP_URL environment variable",
		},
		// Add more RTMP videos here as needed
	}

	// Filter out skipped videos
	var availableVideos []TestVideoConfig
	for _, video := range rtmpVideos {
		if video.Skip || video.FilePath == "" {
			continue
		}
		availableVideos = append(availableVideos, video)
	}

	if len(availableVideos) == 0 {
		t.Skip("No RTMP test videos available, skipping test")
	}
	rtmpVideos = availableVideos

	// Test all combinations of streamer parameters
	combinations := []struct {
		name        string
		rateControl bool
		genPTS      bool
		ptsFilter   bool
	}{
		{"RateControl=true_genPTS=true_PTSFilter=true", true, true, true},
	}

	// Test each video with each parameter combination
	for _, video := range rtmpVideos {
		t.Run(video.Name, func(t *testing.T) {
			t.Logf("Testing RTMP video: %s (%s)", video.Name, video.Description)
			t.Logf("Test RTMP URL: %s", video.FilePath)

			for _, combo := range combinations {
				t.Run(combo.name, func(t *testing.T) {
					testRTMPReaderTiming(t, combo.rateControl, combo.genPTS, combo.ptsFilter, video.FilePath)
				})
			}
		})
	}
}

func testRTMPReaderTiming(t *testing.T, rateControl, genPTS, ptsFilter bool, rtmpURL string) {
	// Duration to run the streamer
	runDuration := 10 * time.Second
	// Threshold for timing checks (10% tolerance)
	threshold := 0.1 // 10%

	t.Logf("Testing RTMP reader timing: RateControl=%v, genPTS=%v, PTSFilter=%v", rateControl, genPTS, ptsFilter)
	t.Logf("Running streamer for %v", runDuration)

	// Create streamer with specified parameters
	streamer := corehelpers.NewStreamer()
	defer streamer.Close()

	streamer.StartLife()

	inputID := "rtmp-input-1"

	// Create RTMP reader input
	rtmpInput := streaminputs.NewRTMP(inputID, rtmpURL)

	// Create buffering destination
	bufferingDest := outputs.NewBuffering("buffering-dest-1")

	// Update streamer with input and output
	err := streamer.UpdateStreams([]Stream{rtmpInput}, []Stream{bufferingDest})
	if err != nil {
		t.Fatalf("Failed to update streams: %v", err)
	}

	// Start the streamer
	streamer.Start()
	streamer.Switch(inputID)

	// Wait for input to start
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = rtmpInput.WaitForStart(ctx)
	if err != nil {
		t.Fatalf("RTMP input failed to start: %v", err)
	}

	// Wait for output to start
	err = bufferingDest.WaitForStart(ctx)
	if err != nil {
		t.Fatalf("Buffering destination failed to start: %v", err)
	}

	// Run for exactly runDuration, then stop the source so both stop
	startTime := time.Now()
	t.Logf("Collecting frames for %v...", runDuration)
	time.Sleep(runDuration)
	rtmpInput.Close()

	endTime := time.Now()

	actualElapsed := endTime.Sub(startTime)

	time.Sleep(500 * time.Millisecond) // drain in-flight frames into destination

	// Get frames from destination
	destVideoFrames := bufferingDest.GetVideoFrames()
	destAudioFrames := bufferingDest.GetAudioFrames()

	t.Logf("Collected frames: video=%d, audio=%d", len(destVideoFrames), len(destAudioFrames))
	t.Logf("Actual elapsed time: %v", actualElapsed)

	if len(destVideoFrames) == 0 && len(destAudioFrames) == 0 {
		t.Fatal("No frames collected from destination")
	}

	// Check video timing
	if len(destVideoFrames) > 0 {
		testtools.CheckFrameTiming(t, destVideoFrames, "video", runDuration, actualElapsed, threshold)
		testtools.CheckSequenceIDContinuity(t, destVideoFrames, "video")
		testtools.CheckH264FrameHealth(t, destVideoFrames)
	}

	// Check audio timing
	if len(destAudioFrames) > 0 {
		testtools.CheckFrameTiming(t, destAudioFrames, "audio", runDuration, actualElapsed, threshold)
		testtools.CheckSequenceIDContinuity(t, destAudioFrames, "audio")
	}

	// DTS checks (same as PTS)
	dtsVideoFrames := cloneFramesWithDTSAsPTS(destVideoFrames)
	dtsAudioFrames := cloneFramesWithDTSAsPTS(destAudioFrames)

	if len(dtsVideoFrames) > 0 {
		testtools.CheckFrameTiming(t, dtsVideoFrames, "video-dts", runDuration, actualElapsed, threshold)
		testtools.CheckSequenceIDContinuity(t, dtsVideoFrames, "video-dts")
	}

	if len(dtsAudioFrames) > 0 {
		testtools.CheckFrameTiming(t, dtsAudioFrames, "audio-dts", runDuration, actualElapsed, threshold)
		testtools.CheckSequenceIDContinuity(t, dtsAudioFrames, "audio-dts")
	}
}
