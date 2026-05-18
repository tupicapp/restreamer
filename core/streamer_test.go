package irajstreamer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	streaminputs "restreamer/core/inputs"
	"restreamer/core/outputs"
	"restreamer/core/test"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
)

const defaultFFmpegRTMPURL = "rtmp://127.0.0.1:1938/live/"

// referenceRTMPReader reads RTMP stream directly using gortmplib.Reader (reference implementation)
func referenceRTMPReader(rtmpURL string, timeout time.Duration) ([]*Frame, []*Frame, error) {
	var videoFrames []*Frame
	var audioFrames []*Frame
	var videoMu sync.Mutex
	var audioMu sync.Mutex

	videoSequenceID := int64(0)
	audioSequenceID := int64(0)

	u, err := url.Parse(addDefaultRTMPPort(rtmpURL))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse RTMP URL: %w", err)
	}

	c := &gortmplib.Client{
		URL:     u,
		Publish: false,
	}

	if err := c.Initialize(context.Background()); err != nil {
		return nil, nil, fmt.Errorf("failed to initialize RTMP client: %w", err)
	}

	r := &gortmplib.Reader{Conn: c}
	if err := r.Initialize(); err != nil {
		return nil, nil, fmt.Errorf("failed to initialize RTMP reader: %w", err)
	}

	// Find video and audio tracks
	var videoTrack *gortmplib.Track
	var audioTrack *gortmplib.Track

	for _, track := range r.Tracks() {
		switch track.Codec.(type) {
		case *codecs.H264:
			if videoTrack == nil {
				videoTrack = track
			}
		case *codecs.MPEG4Audio:
			if audioTrack == nil {
				audioTrack = track
			}
		}
	}

	// Set up callbacks
	if videoTrack != nil {
		r.OnDataH264(videoTrack, func(pts time.Duration, dts time.Duration, au [][]byte) {
			videoSequenceID++
			frame := &Frame{
				PTS:        pts,
				DTS:        dts,
				Payload:    cloneBytesSlice(au),
				Codec:      "h264",
				Timestamp:  time.Now(),
				SequenceID: videoSequenceID,
			}
			frame.IsKeyFrame = isKeyFrame(frame)
			videoMu.Lock()
			videoFrames = append(videoFrames, frame)
			videoMu.Unlock()
		})
	}

	if audioTrack != nil {
		r.OnDataMPEG4Audio(audioTrack, func(pts time.Duration, au []byte) {
			audioSequenceID++
			frame := &Frame{
				PTS:        pts,
				DTS:        pts,
				Payload:    [][]byte{cloneBytes(au)},
				Codec:      "aac",
				Timestamp:  time.Now(),
				IsKeyFrame: true,
				SequenceID: audioSequenceID,
			}
			audioMu.Lock()
			audioFrames = append(audioFrames, frame)
			audioMu.Unlock()
		})
	}

	// Read frames until timeout
	done := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		close(done)
	}()

	for {
		select {
		case <-done:
			videoMu.Lock()
			audioMu.Lock()
			defer videoMu.Unlock()
			defer audioMu.Unlock()
			return videoFrames, audioFrames, nil
		default:
			c.NetConn().SetReadDeadline(time.Now().Add(1 * time.Second))
			err := r.Read()
			if err != nil {
				// Continue reading on timeout
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
	}
}

func cloneBytesSlice(src [][]byte) [][]byte {
	dst := make([][]byte, len(src))
	for i, b := range src {
		dst[i] = cloneBytes(b)
	}
	return dst
}

func TestStreamer_HLSReaderToBufferingDestination(t *testing.T) {
	// List of HLS videos to test
	hlsVideos := []TestVideoConfig{
		{
			Name:        "hls_video_1",
			FilePath:    "http://127.0.0.1:8090/testdata/hls/ts_nob/index.m3u8",
			Description: "Primary HLS test video",
			Skip:        false,
		},
		// {
		// 	Name:        "hls_video_2",
		// 	FilePath:    "testdata/hls/m4s/stream_2/playlist.m3u8",
		// 	Description: "M4S HLS test video",
		// 	Skip:        false,
		// },
		// Add more videos here as needed
	}

	// Test all combinations of streamer parameters
	// Parameters: RateControl, genPTS, PTSFilter
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
			// playlistURI, fileServer, err := setupHLSVideoServer(t, video)
			// if err != nil {
			// 	t.Skipf("Failed to setup HLS video server for %s: %v", video.Name, err)
			// }
			// if fileServer != nil {
			// 	defer fileServer.Close()
			// }

			t.Logf("Testing HLS video: %s (%s)", video.Name, video.Description)
			t.Logf("Test HLS URI: %s", video.FilePath)

			// Reference implementation: read directly using mpegts.Reader
			// baseURL should be the directory containing the playlist for resolving relative segment URLs
			// Derive baseURL from playlistURI (directory containing the playlist file)
			playlistURL, err := url.Parse(video.FilePath)
			if err != nil {
				t.Fatalf("Failed to parse playlist URI: %v", err)
			}
			// Use path.Dir for URL paths (not filepath.Dir which is for file system paths)
			playlistURL.Path = path.Dir(playlistURL.Path) + "/"
			baseURL := playlistURL.String()
			t.Logf("Base URL for segments: %s", baseURL)
			t.Log("Reading with reference implementation (direct mpegts.Reader)...")
			refVideoFrames, refAudioFrames, err := streaminputs.ReferenceInput(baseURL, video.FilePath, 1.0/90000.0)
			if err != nil {
				t.Fatalf("Reference reader failed: %v", err)
			}

			if len(refVideoFrames) == 0 && len(refAudioFrames) == 0 {
				t.Fatal("Reference reader collected no frames")
			}

			sort.Slice(refVideoFrames, func(i, j int) bool { return int64(refVideoFrames[i].DTS) < int64(refVideoFrames[j].DTS) })
			sort.Slice(refAudioFrames, func(i, j int) bool { return int64(refAudioFrames[i].DTS) < int64(refAudioFrames[j].DTS) })

			t.Logf("Reference: collected %d video frames, %d audio frames", len(refVideoFrames), len(refAudioFrames))

			for _, combo := range combinations {
				t.Run(combo.name, func(t *testing.T) {
					testHLSReaderToBufferingDestination(t, combo.rateControl, combo.genPTS, combo.ptsFilter, video.FilePath, refVideoFrames, refAudioFrames)
				})
			}
		})
	}
}

func TestStreamer_HLSReaderLiveToBufferingDestination(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available, skipping HLS reader live test")
	}
	if !isRTMPURLAvailable(defaultFFmpegRTMPURL + "healthcheck") {
		t.Skip("RTMP server not available on 127.0.0.1:1938, skipping HLS reader live test")
	}

	// List of HLS videos to test
	hlsVideos := []TestVideoConfig{
		{
			Name:        "hls_video_2",
			FilePath:    "https://live-hls-web-aja-fa.getaj.net/AJA/03.m3u8",
			Description: "M4S HLS test video",
			Skip:        false,
		},
	}

	// Test all combinations of streamer parameters
	combinations := []struct {
		name        string
		rateControl bool
		genPTS      bool
		ptsFilter   bool
	}{
		{"RateControl=true_genPTS=true_PTSFilter=false", false, false, false},
	}

	for _, video := range hlsVideos {
		t.Run(video.Name, func(t *testing.T) {
			t.Logf("Testing HLS live video: %s (%s)", video.Name, video.Description)
			t.Logf("Test HLS URI: %s", video.FilePath)

			for _, combo := range combinations {
				t.Run(combo.name, func(t *testing.T) {
					testHLSReaderLiveToBufferingDestination(t, combo.rateControl, combo.genPTS, combo.ptsFilter, video.FilePath, video.Name)
				})
			}
		})
	}
}

func testHLSReaderToBufferingDestination(t *testing.T, rateControl, genPTS, ptsFilter bool, playlistURI string, refVideoFrames, refAudioFrames []*Frame) {
	// Create streamer with specified parameters
	streamer := NewStreamer(rateControl, genPTS, ptsFilter)
	defer streamer.Close()

	streamer.StartLife()

	inputID := "hls-input-1"

	// Create HLS reader input
	hlsInput := streaminputs.NewHLS("hls-input-1", playlistURI, streaminputs.OptionWithRealTime(false))

	// Create buffering destination
	bufferingDest := outputs.NewBuffering("buffering-dest-1")

	// Update streamer with input and output
	err := streamer.UpdateStreams([]Stream{hlsInput}, []Stream{bufferingDest})
	if err != nil {
		t.Fatalf("Failed to update streams: %v", err)
	}

	// Start the streamer
	streamer.Start()
	streamer.switchInput(inputID)

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

	// Record start time for elapsed time tracking
	startTime := time.Now()

	// Wait for all frames to be processed - wait longer and check more frequently
	timeout := 60 * time.Second
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	t.Log("Waiting for frames to be processed...")
	deadline := time.Now().Add(timeout)
	lastVideoCount := 0
	lastAudioCount := 0
	stalledCount := 0

	for time.Now().Before(deadline) {
		videoCount := len(bufferingDest.GetVideoFrames())
		audioCount := len(bufferingDest.GetAudioFrames())

		// Check if we're making progress
		if videoCount == lastVideoCount && audioCount == lastAudioCount {
			stalledCount++
			if stalledCount > 200 { // 10 seconds without progress
				t.Logf("Frame collection stalled: video=%d/%d, audio=%d/%d", videoCount, len(refVideoFrames), audioCount, len(refAudioFrames))
				break
			}
		} else {
			stalledCount = 0
		}

		if videoCount >= len(refVideoFrames) && audioCount >= len(refAudioFrames) {
			t.Logf("All frames collected: video=%d/%d, audio=%d/%d", videoCount, len(refVideoFrames), audioCount, len(refAudioFrames))
			break
		}

		lastVideoCount = videoCount
		lastAudioCount = audioCount
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("Final collected frames: video=%d, audio=%d", len(bufferingDest.GetVideoFrames()), len(bufferingDest.GetAudioFrames()))
	t.Logf("Progress: video=%d/%d, audio=%d/%d", len(bufferingDest.GetVideoFrames()), len(refVideoFrames), len(bufferingDest.GetAudioFrames()), len(refAudioFrames))

	// Get final frames from destination
	destVideoFrames := bufferingDest.GetVideoFrames()
	destAudioFrames := bufferingDest.GetAudioFrames()

	// Record end time and calculate elapsed time
	endTime := time.Now()
	actualElapsed := endTime.Sub(startTime)

	t.Logf("Final frames: video=%d, audio=%d", len(destVideoFrames), len(destAudioFrames))
	t.Logf("Reference frames: video=%d, audio=%d", len(refVideoFrames), len(refAudioFrames))
	t.Logf("Actual elapsed time: %v", actualElapsed)

	// Compare frame sequences
	compareStreamerSequences(t, destVideoFrames, destAudioFrames, refVideoFrames, refAudioFrames, rateControl, genPTS)

	// Run benchmarks
	t.Log("\n=== Running Stream Benchmarks ===")

	// Threshold for window matching (10% tolerance)
	threshold := 0.1 // 10%

	// Check overall PTS window vs elapsed time for destination streams
	// Also compare destination PTS window with reference PTS window
	if len(destVideoFrames) > 0 && len(refVideoFrames) > 0 {
		// Calculate destination PTS window
		destMinPTS := destVideoFrames[0].PTS
		destMaxPTS := destVideoFrames[0].PTS
		for _, frame := range destVideoFrames {
			if frame != nil {
				if frame.PTS < destMinPTS {
					destMinPTS = frame.PTS
				}
				if frame.PTS > destMaxPTS {
					destMaxPTS = frame.PTS
				}
			}
		}
		destPTSWindow := destMaxPTS - destMinPTS

		// Calculate reference PTS window
		refMinPTS := refVideoFrames[0].PTS
		refMaxPTS := refVideoFrames[0].PTS
		for _, frame := range refVideoFrames {
			if frame != nil {
				if frame.PTS < refMinPTS {
					refMinPTS = frame.PTS
				}
				if frame.PTS > refMaxPTS {
					refMaxPTS = frame.PTS
				}
			}
		}
		refPTSWindow := refMaxPTS - refMinPTS

		// Compare destination PTS window with reference PTS window
		destPTSSeconds := destPTSWindow.Seconds()
		refPTSSeconds := refPTSWindow.Seconds()
		if refPTSSeconds > 0 {
			ptsDiff := abs(destPTSSeconds - refPTSSeconds)
			ptsDiffPercent := (ptsDiff / refPTSSeconds) * 100.0
			t.Logf("Video PTS window: dest=%v (from %v to %v), ref=%v (from %v to %v), diff: %.2f%%",
				destPTSWindow, destMinPTS, destMaxPTS, refPTSWindow, refMinPTS, refMaxPTS, ptsDiffPercent)
			if ptsDiffPercent > threshold*100 {
				t.Errorf("Video PTS window (%.2fs) does not match reference PTS window (%.2fs): difference is %.2f%% (threshold: %.2f%%)",
					destPTSSeconds, refPTSSeconds, ptsDiffPercent, threshold*100)
			}
		}

		// For HLS file reading, elapsed time includes processing overhead, so we only check against reference
		// PTS window vs elapsed time check is skipped for HLS (it's more relevant for live streams like RTMP)
	}

	if len(destAudioFrames) > 0 && len(refAudioFrames) > 0 {
		// Calculate destination PTS window
		destMinPTS := destAudioFrames[0].PTS
		destMaxPTS := destAudioFrames[0].PTS
		for _, frame := range destAudioFrames {
			if frame != nil {
				if frame.PTS < destMinPTS {
					destMinPTS = frame.PTS
				}
				if frame.PTS > destMaxPTS {
					destMaxPTS = frame.PTS
				}
			}
		}
		destPTSWindow := destMaxPTS - destMinPTS

		// Calculate reference PTS window
		refMinPTS := refAudioFrames[0].PTS
		refMaxPTS := refAudioFrames[0].PTS
		for _, frame := range refAudioFrames {
			if frame != nil {
				if frame.PTS < refMinPTS {
					refMinPTS = frame.PTS
				}
				if frame.PTS > refMaxPTS {
					refMaxPTS = frame.PTS
				}
			}
		}
		refPTSWindow := refMaxPTS - refMinPTS

		// Compare destination PTS window with reference PTS window
		destPTSSeconds := destPTSWindow.Seconds()
		refPTSSeconds := refPTSWindow.Seconds()
		if refPTSSeconds > 0 {
			ptsDiff := abs(destPTSSeconds - refPTSSeconds)
			ptsDiffPercent := (ptsDiff / refPTSSeconds) * 100.0
			t.Logf("Audio PTS window: dest=%v (from %v to %v), ref=%v (from %v to %v), diff: %.2f%%",
				destPTSWindow, destMinPTS, destMaxPTS, refPTSWindow, refMinPTS, refMaxPTS, ptsDiffPercent)
			if ptsDiffPercent > threshold*100 {
				t.Errorf("Audio PTS window (%.2fs) does not match reference PTS window (%.2fs): difference is %.2f%% (threshold: %.2f%%)",
					destPTSSeconds, refPTSSeconds, ptsDiffPercent, threshold*100)
			}
		}

		// For HLS file reading, elapsed time includes processing overhead, so we only check against reference
		// PTS window vs elapsed time check is skipped for HLS (it's more relevant for live streams like RTMP)
	}

	videoWindowResult := windowMatchBenchmarkWithTiming(destVideoFrames, refVideoFrames, "video", actualElapsed, threshold)
	printWindowMatchBenchmark(t, videoWindowResult, "video")
	if len(videoWindowResult.MismatchContexts) > 0 {
		t.Errorf("Video window-match benchmark: found %d mismatches", len(videoWindowResult.MismatchContexts))
	}
	if videoWindowResult.MatchPercent < (1.0-threshold)*100.0 {
		t.Errorf("Video window-match benchmark: match percent is %.2f%% (expected >= %.2f%%)", videoWindowResult.MatchPercent, (1.0-threshold)*100.0)
	}

	audioWindowResult := windowMatchBenchmarkWithTiming(destAudioFrames, refAudioFrames, "audio", actualElapsed, threshold)
	printWindowMatchBenchmark(t, audioWindowResult, "audio")
	if len(audioWindowResult.MismatchContexts) > 0 {
		t.Errorf("Audio window-match benchmark: found %d mismatches", len(audioWindowResult.MismatchContexts))
	}
	if audioWindowResult.MatchPercent < (1.0-threshold)*100.0 {
		t.Errorf("Audio window-match benchmark: match percent is %.2f%% (expected >= %.2f%%)", audioWindowResult.MatchPercent, (1.0-threshold)*100.0)
	}

	// Equal-Packet-Rate Benchmark for video
	// Check if all reference packets exist in destination (reverse direction)
	videoPacketRateResult := equalPacketRateBenchmark(refVideoFrames, destVideoFrames, "video")
	printEqualPacketRateBenchmark(t, videoPacketRateResult, "video")
	if videoPacketRateResult.SuccessRate < 100.0 {
		t.Errorf("Video equal-packet-rate benchmark: success rate is %.2f%% (expected 100%%), %d reference packets not found in destination",
			videoPacketRateResult.SuccessRate, videoPacketRateResult.NotFoundPackets)
	}
	if videoPacketRateResult.NotFoundPackets > 0 {
		t.Errorf("Video equal-packet-rate benchmark: %d reference packets not found in destination stream", videoPacketRateResult.NotFoundPackets)
	}

	// Equal-Packet-Rate Benchmark for audio
	// Check if all reference packets exist in destination (reverse direction)
	audioPacketRateResult := equalPacketRateBenchmark(refAudioFrames, destAudioFrames, "audio")
	printEqualPacketRateBenchmark(t, audioPacketRateResult, "audio")
	if audioPacketRateResult.SuccessRate < 100.0 {
		t.Errorf("Audio equal-packet-rate benchmark: success rate is %.2f%% (expected 100%%), %d reference packets not found in destination",
			audioPacketRateResult.SuccessRate, audioPacketRateResult.NotFoundPackets)
	}
	if audioPacketRateResult.NotFoundPackets > 0 {
		t.Errorf("Audio equal-packet-rate benchmark: %d reference packets not found in destination stream", audioPacketRateResult.NotFoundPackets)
	}

	// Stream Health Check for destination video
	videoHealth := checkStreamHealth(destVideoFrames, "video")
	if !videoHealth.IsHealthy {
		t.Logf("Destination video stream health: ⚠ Issues found (PTS monotonicity: %.2f%%, Valid gaps: %.2f%%, %d PTS issues, %d gap issues)",
			videoHealth.MonotonicPTSPercent, videoHealth.ValidGapPercent,
			len(videoHealth.MonotonicPTSIssues), len(videoHealth.LargeGapIssues))
		if len(videoHealth.MonotonicPTSIssues) > 0 {
			for i, issue := range videoHealth.MonotonicPTSIssues {
				if i < 5 { // Show first 5 issues
					t.Logf("  PTS Issue %d: Frame %d, PTS decreased by %v (from %v to %v)",
						i+1, issue.FrameIndex, issue.Difference, issue.PreviousPTS, issue.CurrentPTS)
				}
			}
		}
		if len(videoHealth.LargeGapIssues) > 0 {
			for i, issue := range videoHealth.LargeGapIssues {
				if i < 5 { // Show first 5 issues
					t.Logf("  Gap Issue %d: Frame %d, Gap %v (from %v to %v)",
						i+1, issue.FrameIndex, issue.Gap, issue.PreviousPTS, issue.CurrentPTS)
				}
			}
		}
	} else {
		t.Logf("Destination video stream health: ✓ Healthy (PTS monotonicity: %.2f%%, Valid gaps: %.2f%%)",
			videoHealth.MonotonicPTSPercent, videoHealth.ValidGapPercent)
	}

	// Stream Health Check for destination audio
	audioHealth := checkStreamHealth(destAudioFrames, "audio")
	if !audioHealth.IsHealthy {
		t.Logf("Destination audio stream health: ⚠ Issues found (PTS monotonicity: %.2f%%, Valid gaps: %.2f%%, %d PTS issues, %d gap issues)",
			audioHealth.MonotonicPTSPercent, audioHealth.ValidGapPercent,
			len(audioHealth.MonotonicPTSIssues), len(audioHealth.LargeGapIssues))
		if len(audioHealth.MonotonicPTSIssues) > 0 {
			for i, issue := range audioHealth.MonotonicPTSIssues {
				if i < 5 { // Show first 5 issues
					t.Logf("  PTS Issue %d: Frame %d, PTS decreased by %v (from %v to %v)",
						i+1, issue.FrameIndex, issue.Difference, issue.PreviousPTS, issue.CurrentPTS)
				}
			}
		}
		if len(audioHealth.LargeGapIssues) > 0 {
			for i, issue := range audioHealth.LargeGapIssues {
				if i < 5 { // Show first 5 issues
					t.Logf("  Gap Issue %d: Frame %d, Gap %v (from %v to %v)",
						i+1, issue.FrameIndex, issue.Gap, issue.PreviousPTS, issue.CurrentPTS)
				}
			}
		}
	} else {
		t.Logf("Destination audio stream health: ✓ Healthy (PTS monotonicity: %.2f%%, Valid gaps: %.2f%%)",
			audioHealth.MonotonicPTSPercent, audioHealth.ValidGapPercent)
	}

	// DTS-based benchmarks (use DTS as PTS for ordering/health checks)
	dtsDestVideo := cloneFramesWithDTSAsPTS(destVideoFrames)
	dtsRefVideo := cloneFramesWithDTSAsPTS(refVideoFrames)
	dtsDestAudio := cloneFramesWithDTSAsPTS(destAudioFrames)
	dtsRefAudio := cloneFramesWithDTSAsPTS(refAudioFrames)

	videoDTSWindow := windowMatchBenchmarkWithTiming(dtsDestVideo, dtsRefVideo, "video-dts", actualElapsed, threshold)
	printWindowMatchBenchmark(t, videoDTSWindow, "video-dts")
	if len(videoDTSWindow.MismatchContexts) > 0 {
		t.Errorf("Video DTS window-match benchmark: found %d mismatches", len(videoDTSWindow.MismatchContexts))
	}
	if videoDTSWindow.MatchPercent < (1.0-threshold)*100.0 {
		t.Errorf("Video DTS window-match benchmark: match percent is %.2f%% (expected >= %.2f%%)", videoDTSWindow.MatchPercent, (1.0-threshold)*100.0)
	}

	audioDTSWindow := windowMatchBenchmarkWithTiming(dtsDestAudio, dtsRefAudio, "audio-dts", actualElapsed, threshold)
	printWindowMatchBenchmark(t, audioDTSWindow, "audio-dts")
	if len(audioDTSWindow.MismatchContexts) > 0 {
		t.Errorf("Audio DTS window-match benchmark: found %d mismatches", len(audioDTSWindow.MismatchContexts))
	}
	if audioDTSWindow.MatchPercent < (1.0-threshold)*100.0 {
		t.Errorf("Audio DTS window-match benchmark: match percent is %.2f%% (expected >= %.2f%%)", audioDTSWindow.MatchPercent, (1.0-threshold)*100.0)
	}

	videoDTSPacketRate := equalPacketRateBenchmark(dtsDestVideo, dtsRefVideo, "video-dts")
	printEqualPacketRateBenchmark(t, videoDTSPacketRate, "video-dts")
	if videoDTSPacketRate.SuccessRate < 100.0 || videoDTSPacketRate.NotFoundPackets > 0 {
		t.Errorf("Video DTS equal-packet-rate: success rate %.2f%%, missing %d packets", videoDTSPacketRate.SuccessRate, videoDTSPacketRate.NotFoundPackets)
	}

	audioDTSPacketRate := equalPacketRateBenchmark(dtsDestAudio, dtsRefAudio, "audio-dts")
	printEqualPacketRateBenchmark(t, audioDTSPacketRate, "audio-dts")
	if audioDTSPacketRate.SuccessRate < 100.0 || audioDTSPacketRate.NotFoundPackets > 0 {
		t.Errorf("Audio DTS equal-packet-rate: success rate %.2f%%, missing %d packets", audioDTSPacketRate.SuccessRate, audioDTSPacketRate.NotFoundPackets)
	}

	videoDTSHealth := checkStreamHealth(destVideoFrames, "video-dts")
	if !videoDTSHealth.IsHealthy {
		printStreamHealth(t, videoDTSHealth, "video-dts")
		t.Errorf("Destination video DTS health check failed: %d DTS issues, %d gap issues",
			len(videoDTSHealth.DTSIssues), len(videoDTSHealth.LargeGapIssues))
	} else {
		t.Logf("Destination video DTS health: ✓ Healthy (DTS monotonicity: %.2f%%, Valid gaps: %.2f%%)",
			videoDTSHealth.MonotonicPTSPercent, videoDTSHealth.ValidGapPercent)
	}

	audioDTSHealth := checkStreamHealth(destAudioFrames, "audio-dts")
	if !audioDTSHealth.IsHealthy {
		printStreamHealth(t, audioDTSHealth, "audio-dts")
		t.Errorf("Destination audio DTS health check failed: %d DTS issues, %d gap issues",
			len(audioDTSHealth.DTSIssues), len(audioDTSHealth.LargeGapIssues))
	} else {
		t.Logf("Destination audio DTS health: ✓ Healthy (DTS monotonicity: %.2f%%, Valid gaps: %.2f%%)",
			audioDTSHealth.MonotonicPTSPercent, audioDTSHealth.ValidGapPercent)
	}

	assertRTMPPushable(t, destVideoFrames, destAudioFrames)
	// flvPath := assertFLVPlayableWithFFprobe(t, destVideoFrames, destAudioFrames)
	// t.Logf("ffmpeg mux and ffprobe check succeeded, FLV written to %s", flvPath)
}

func testHLSReaderLiveToBufferingDestination(t *testing.T, rateControl, genPTS, ptsFilter bool, playlistURI, streamName string) {
	collectionDuration := 40 * time.Second

	streamer := NewStreamer(rateControl, genPTS, ptsFilter)
	defer streamer.Close()

	streamer.StartLife()

	inputID := fmt.Sprintf("hls-live-%s", streamName)

	// Create HLS reader live input
	hlsInput := streaminputs.NewHLSLive(inputID, playlistURI)

	// Create buffering destination
	bufferingDest := outputs.NewBuffering("buffering-dest-1")

	// Update streamer with input and output
	err := streamer.UpdateStreams([]Stream{hlsInput}, []Stream{bufferingDest})
	if err != nil {
		t.Fatalf("Failed to update streams: %v", err)
	}

	// Start the streamer
	streamer.Start()
	streamer.switchInput(inputID)

	time.Sleep(2 * time.Second)

	// Wait for input to start
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = hlsInput.WaitForStart(ctx)
	if err != nil {
		t.Fatalf("HLS live input failed to start: %v", err)
	}

	// Wait for output to start
	err = bufferingDest.WaitForStart(ctx)
	if err != nil {
		t.Fatalf("Buffering destination failed to start: %v", err)
	}

	t.Logf("Collecting frames for %v...", collectionDuration)
	time.Sleep(collectionDuration)

	destVideoFrames := bufferingDest.GetVideoFrames()
	destAudioFrames := bufferingDest.GetAudioFrames()

	if len(destVideoFrames) == 0 && len(destAudioFrames) == 0 {
		t.Fatal("Destination collected no frames")
	}

	t.Logf("Destination collected: video=%d, audio=%d", len(destVideoFrames), len(destAudioFrames))

	// Check stream health (same pattern as switch tests)
	videoHealth := checkStreamHealth(destVideoFrames, "video")
	audioHealth := checkStreamHealth(destAudioFrames, "audio")

	printStreamHealth(t, videoHealth, "video")
	printStreamHealth(t, audioHealth, "audio")

	if !videoHealth.IsHealthy {
		t.Errorf("Video stream is not healthy: Monotonic PTS: %.2f%%, Monotonic DTS: %.2f%%, Valid Gaps: %.2f%%, Valid DTS: %.2f%%",
			videoHealth.MonotonicPTSPercent, videoHealth.MonotonicDTSPercent, videoHealth.ValidGapPercent, videoHealth.DTSValidPercent)
	}
	if !audioHealth.IsHealthy {
		t.Errorf("Audio stream is not healthy: Monotonic PTS: %.2f%%, Monotonic DTS: %.2f%%, Valid Gaps: %.2f%%, Valid DTS: %.2f%%",
			audioHealth.MonotonicPTSPercent, audioHealth.MonotonicDTSPercent, audioHealth.ValidGapPercent, audioHealth.DTSValidPercent)
	}

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
}

func assertRTMPPushable(t *testing.T, videoFrames, audioFrames []*Frame) {
	t.Helper()

	if len(videoFrames) == 0 {
		t.Fatalf("video stream contains no frames, RTMP push requires both video and audio payloads")
	}
	if len(audioFrames) == 0 {
		t.Fatalf("audio stream contains no frames, RTMP push requires both video and audio payloads")
	}

	assertRTMPVideoFrames(t, videoFrames)
	assertRTMPAudioFrames(t, audioFrames)
}

func AssertRTMPPushable(t *testing.T, videoFrames, audioFrames []*Frame) {
	assertRTMPPushable(t, videoFrames, audioFrames)
}

func assertRTMPVideoFrames(t *testing.T, frames []*Frame) {
	t.Helper()

	keyframeCount := 0
	keyframeWithHeaders := 0

	for i, frame := range frames {
		if frame == nil {
			t.Fatalf("video frame %d is nil", i)
		}

		if !strings.EqualFold(frame.Codec, "h264") {
			t.Fatalf("video frame %d uses codec %q, expected h264 for RTMP/FLV", i, frame.Codec)
		}

		if len(frame.Payload) == 0 {
			t.Fatalf("video frame %d does not contain any NAL units", i)
		}

		if frame.IsKeyFrame {
			keyframeCount++
			if frameContainsSPSPPS(frame) {
				keyframeWithHeaders++
			}
		}
	}

	if keyframeCount == 0 {
		t.Fatal("video stream does not expose any keyframes, so RTMP/AVC headers cannot be built")
	}

	if keyframeWithHeaders == 0 {
		t.Fatalf("none of the %d keyframes include both SPS and PPS, so RTMP AVC sequence header cannot be assembled", keyframeCount)
	}
}

func frameContainsSPSPPS(frame *Frame) bool {
	hasSPS := false
	hasPPS := false

	for _, nalu := range frame.Payload {
		if len(nalu) == 0 {
			continue
		}

		stripped := stripAnnexB(nalu)
		if len(stripped) == 0 {
			continue
		}

		nalType := stripped[0] & 0x1F
		switch nalType {
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}

		if hasSPS && hasPPS {
			return true
		}
	}

	return false
}

func assertRTMPAudioFrames(t *testing.T, frames []*Frame) {
	t.Helper()

	for i, frame := range frames {
		if frame == nil {
			t.Fatalf("audio frame %d is nil", i)
		}

		if !strings.Contains(strings.ToLower(frame.Codec), "aac") {
			t.Fatalf("audio frame %d uses codec %q, expected AAC for RTMP/FLV", i, frame.Codec)
		}

		hasPayload := false
		for _, payload := range frame.Payload {
			if len(payload) > 0 {
				hasPayload = true
				break
			}
		}

		if !hasPayload {
			t.Fatalf("audio frame %d does not contain any AAC payload bytes", i)
		}
	}
}

func TestStreamer_RTMPReaderToBufferingDestination(t *testing.T) {
	// List of RTMP videos to test
	rtmpVideos := []TestVideoConfig{
		{
			Name:        "rtmp_env",
			FilePath:    "", // Will be set from environment variable
			Description: "RTMP URL from RTMP_URL environment variable",
			Skip:        false,
		},
		// Add more RTMP videos here as needed
	}

	// Set RTMP URL from config/env if available
	rtmpURL := getConfiguredRTMPURL(t)
	requireRTMPPublishing(t, rtmpURL, 10*time.Second)
	rtmpVideos[0].FilePath = rtmpURL

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
	// Parameters: RateControl, genPTS, PTSFilter
	combinations := []struct {
		name        string
		rateControl bool
		genPTS      bool
		ptsFilter   bool
	}{
		{"RateControl=true_genPTS=true_PTSFilter=false", true, true, true},
	}

	// Test each video with each parameter combination
	for _, video := range rtmpVideos {
		t.Run(video.Name, func(t *testing.T) {
			t.Logf("Testing RTMP video: %s (%s)", video.Name, video.Description)
			t.Logf("Test RTMP URL: %s", video.FilePath)

			for _, combo := range combinations {
				t.Run(combo.name, func(t *testing.T) {
					testRTMPReaderToBufferingDestination(t, combo.rateControl, combo.genPTS, combo.ptsFilter, video.FilePath)
				})
			}
		})
	}
}

func testRTMPReaderToBufferingDestination(t *testing.T, rateControl, genPTS, ptsFilter bool, rtmpURL string) {
	// Duration to collect frames from both streams simultaneously
	collectionDuration := 10 * time.Second

	// Channels to collect results from reference reader
	refVideoChan := make(chan []*Frame, 1)
	refAudioChan := make(chan []*Frame, 1)
	refErrChan := make(chan error, 1)

	// Start reference reader in a goroutine
	go func() {
		t.Log("Starting reference reader (direct gortmplib.Reader)...")
		refVideoFrames, refAudioFrames, err := referenceRTMPReader(rtmpURL, collectionDuration)
		if err != nil {
			refErrChan <- err
			return
		}
		refVideoChan <- refVideoFrames
		refAudioChan <- refAudioFrames
	}()

	// Start streamer and destination in parallel
	t.Log("Starting streamer with buffering destination...")
	streamer := NewStreamer(rateControl, genPTS, ptsFilter)
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
	streamer.switchInput(inputID)

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

	// Wait for collection duration (both readers are running concurrently)
	t.Logf("Collecting frames for %v (reference and destination reading simultaneously)...", collectionDuration)
	time.Sleep(collectionDuration)

	// Get frames from destination
	destVideoFrames := bufferingDest.GetVideoFrames()
	destAudioFrames := bufferingDest.GetAudioFrames()

	t.Logf("Destination collected: video=%d, audio=%d", len(destVideoFrames), len(destAudioFrames))

	// Wait for reference reader to finish
	select {
	case err := <-refErrChan:
		t.Fatalf("Reference reader failed: %v", err)
	case refVideoFrames := <-refVideoChan:
		refAudioFrames := <-refAudioChan
		t.Logf("Reference collected: video=%d, audio=%d", len(refVideoFrames), len(refAudioFrames))

		if len(refVideoFrames) == 0 && len(refAudioFrames) == 0 {
			t.Fatal("Reference reader collected no frames")
		}

		if len(destVideoFrames) == 0 && len(destAudioFrames) == 0 {
			t.Fatal("Destination collected no frames")
		}

		// Compare frames
		res1 := windowMatchBenchmark(destVideoFrames, refVideoFrames, "video")
		res2 := windowMatchBenchmark(destAudioFrames, refAudioFrames, "audio")

		res3 := equalPacketRateBenchmark(destVideoFrames, refVideoFrames, "video")
		res4 := equalPacketRateBenchmark(destAudioFrames, refAudioFrames, "audio")

		printWindowMatchBenchmark(t, res1, "video")
		printWindowMatchBenchmark(t, res2, "audio")

		printEqualPacketRateBenchmark(t, res3, "video")
		printEqualPacketRateBenchmark(t, res4, "audio")

		// Compare frame sequences
		// compareStreamerSequences(t, destVideoFrames, destAudioFrames, refVideoFrames, refAudioFrames)
	case <-time.After(15 * time.Second):
		t.Fatal("Timeout waiting for reference reader to complete")
	}
}

func TestHLSDestinationStream_FromRTMPReader_ProducesPlayableHLS(t *testing.T) {
	t.Skip("config and storage packages not available in current codebase")
	// requireBinary(t, "ffprobe")
	// requireBinary(t, "ffmpeg")
	//
	// output := "recordings/hls/inputs"
	//
	// rtmpURL := getConfiguredRTMPURL(t)
	// if !isRTMPURLAvailable(rtmpURL) {
	// 	t.Skipf("RTMP stream is not reachable or not publishing: %s", rtmpURL)
	// }
	//
	// channelID := fmt.Sprintf("test-channel-%d", time.Now().UnixNano())
	// programID := "program-1"
	// outDir := filepath.Join(output, channelID, programID)
	// t.Cleanup(func() {
	// 	_ = os.RemoveAll("recordings")
	// })
	//
	// streamer := NewStreamer(true, true, true)
	// streamer.StartLife()
	// defer streamer.Close()
	//
	// inputID := "rtmp-reader-input"
	// rtmpInput := streaminputs.NewRTMP(inputID, rtmpURL)
	// hlsDestination, err := outputs.NewHLSLiveDestination("hls-file-destination", storage.NewLocal(&config.Config{
	// 	Storage: config.Storage{RecordingsRoot: output},
	// }).RecordingsRoot().Folder(filepath.Join(channelID, programID)))
	// if err != nil {
	// 	t.Fatalf("NewHLSLiveDestination failed: %v", err)
	// }
	//
	// if err := streamer.UpdateStreams([]shared.Stream{rtmpInput}, []shared.Stream{hlsDestination}); err != nil {
	// 	t.Fatalf("UpdateStreams failed: %v", err)
	// }
	//
	// if !streamer.Switch(inputID) {
	// 	t.Fatalf("failed to switch to input %q", inputID)
	// }
	// streamer.Start()
	//
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	//
	// if err := rtmpInput.WaitForStart(ctx); err != nil {
	// 	t.Fatalf("RTMP reader failed to start: %v", err)
	// }
	// if err := hlsDestination.WaitForStart(ctx); err != nil {
	// 	t.Fatalf("HLS destination failed to start: %v", err)
	// }
	//
	// waitForHLSArtifacts(t, outDir, 15*time.Second, 2)
	//
	// // Close to finalize the current segment and make the generated playlist static for probing.
	// streamer.Close()
	//
	// playlistPath := filepath.Join(outDir, "stream.m3u8")
	// assertHLSPlaylistLooksValid(t, playlistPath)
	// assertHLSPlaylistHasDiscontinuities(t, playlistPath)
	// assertHLSSegmentTimelineIsContinuous(t, outDir)
	// assertHLSPlayableWithFFmpeg(t, playlistPath)
}

func waitForHLSArtifacts(t *testing.T, outDir string, timeout time.Duration, minSegments int) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	playlistPath := filepath.Join(outDir, "stream.m3u8")

	for time.Now().Before(deadline) {
		playlistExists := false
		if _, err := os.Stat(playlistPath); err == nil {
			playlistExists = true
		}

		segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
		if err != nil {
			t.Fatalf("glob segments failed: %v", err)
		}

		if playlistExists && len(segmentFiles) >= minSegments {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	segmentFiles, _ := filepath.Glob(filepath.Join(outDir, "*.ts"))
	t.Fatalf("timed out waiting for HLS output in %s (segments=%d)", outDir, len(segmentFiles))
}

func assertHLSPlaylistLooksValid(t *testing.T, playlistPath string) {
	t.Helper()

	content, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("failed reading playlist %s: %v", playlistPath, err)
	}

	text := string(content)
	if !strings.Contains(text, "#EXTM3U") {
		t.Fatalf("playlist %s is missing #EXTM3U", playlistPath)
	}
	if !strings.Contains(text, "#EXTINF") {
		t.Fatalf("playlist %s has no segments", playlistPath)
	}
}

func assertHLSPlayableWithFFmpeg(t *testing.T, playlistPath string) {
	t.Helper()

	info, err := test.ProbeStream(playlistPath)
	if err != nil {
		t.Fatalf("ffprobe failed on %s: %v", playlistPath, err)
	}

	format := strings.ToLower(info.Format)
	if !strings.Contains(format, "hls") && !strings.Contains(format, "applehttp") {
		t.Fatalf("ffprobe format %q is not recognized as HLS", info.Format)
	}

	videoCodec := strings.ToLower(info.VideoCodec)
	audioCodec := strings.ToLower(info.AudioCodec)
	if videoCodec == "" || !strings.Contains(videoCodec, "h264") {
		t.Fatalf("ffprobe video codec %q is not recognized as h264", info.VideoCodec)
	}
	if audioCodec == "" || !strings.Contains(audioCodec, "aac") {
		t.Fatalf("ffprobe audio codec %q is not recognized as AAC", info.AudioCodec)
	}

	cmd := exec.Command(
		"ffmpeg",
		"-v", "error",
		"-i", playlistPath,
		"-t", "6",
		"-map", "0",
		"-f", "null",
		"-",
	)
	runCmdEnsureNoStderrWithTimeout(t, cmd, "ffmpeg verify HLS playback", 60*time.Second)
}

func runCmdEnsureNoStderrWithTimeout(t *testing.T, cmd *exec.Cmd, label string, timeout time.Duration) {
	t.Helper()

	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("%s: failed to start: %v", label, err)
	}

	waitErrChan := make(chan error, 1)
	go func() {
		waitErrChan <- cmd.Wait()
	}()

	select {
	case waitErr := <-waitErrChan:
		stderrText := strings.TrimSpace(stderr.String())
		if stderrText != "" {
			t.Logf("ffmpeg stderr: %s", stderrText)
		}
		if waitErr != nil {
			t.Fatalf("%s failed: %v\n%s", label, waitErr, stderrText)
		}
		if stderrText != "" {
			t.Fatalf("%s produced stderr output; treat as failure:\n%s", label, stderrText)
		}
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		t.Fatalf("%s timed out after %s", label, timeout)
	}
}

func assertHLSPlaylistHasDiscontinuities(t *testing.T, playlistPath string) {
	t.Helper()

	content, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("failed reading playlist %s: %v", playlistPath, err)
	}

	if !strings.Contains(string(content), "#EXT-X-DISCONTINUITY") {
		t.Fatalf("playlist %s should contain discontinuities between standalone TS segments", playlistPath)
	}
}

func assertHLSSegmentTimelineIsContinuous(t *testing.T, outDir string) {
	t.Helper()

	segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	if len(segmentFiles) < 2 {
		t.Fatalf("need at least 2 segments to validate timeline continuity, got %d", len(segmentFiles))
	}

	sort.Strings(segmentFiles)
	firstStart := firstPacketPTSSeconds(t, segmentFiles[0])
	secondStart := firstPacketPTSSeconds(t, segmentFiles[1])

	if secondStart <= firstStart {
		t.Fatalf("expected second segment to continue timeline after first: first_start=%f second_start=%f", firstStart, secondStart)
	}
}

func firstPacketPTSSeconds(t *testing.T, segmentPath string) float64 {
	t.Helper()

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
		t.Fatalf("ffprobe failed on %s: %v", segmentPath, err)
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		t.Fatalf("ffprobe returned no packet pts for %s", segmentPath)
	}

	for _, field := range strings.Split(line, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		v, parseErr := strconv.ParseFloat(field, 64)
		if parseErr == nil {
			return v
		}
	}

	t.Fatalf("failed to parse first packet pts from %s: %q", segmentPath, line)
	return 0
}

// compareStreamerSequences compares the frames from destination with reference frames
func compareStreamerSequences(t *testing.T, destVideo, destAudio, refVideo, refAudio []*Frame, rateControl, genPTS bool) {
	const maxPrint = 30

	// Slices to store last matched frames info
	var lastMatchedVideoFrames []string
	var lastMatchedAudioFrames []string

	// Helper to print context before and after failure for video
	printVideoContext := func(idx int, destFrames, refFrames []*Frame, context string) {
		start := idx - maxPrint
		if start < 0 {
			start = 0
		}
		end := idx + maxPrint
		if end > len(destFrames) {
			end = len(destFrames)
		}
		t.Logf("\n--- %s Video frames [%d:%d] ---", context, start, end)
		for i := start; i < end; i++ {
			dest := destFrames[i]
			ref := refFrames[i]
			gotStr := fmt.Sprintf("Video frame %d: got      - PTS=%v SeqId=%v InputID=%v Key=%v Codec=%v", i, dest.PTS, dest.SequenceID, dest.InputID, dest.IsKeyFrame, dest.Codec)
			expStr := fmt.Sprintf("Video frame %d: expected - PTS=%v SeqId=%v InputID=%v Key=%v Codec=%v", i, ref.PTS, ref.SequenceID, ref.InputID, ref.IsKeyFrame, ref.Codec)
			t.Log(gotStr)
			t.Log(expStr)
		}
	}
	// Helper to print context before and after failure for audio
	printAudioContext := func(idx int, destFrames, refFrames []*Frame, context string) {
		start := idx - maxPrint
		if start < 0 {
			start = 0
		}
		end := idx + maxPrint
		if end > len(destFrames) {
			end = len(destFrames)
		}
		t.Logf("\n--- %s Audio frames [%d:%d] ---", context, start, end)
		for i := start; i < end; i++ {
			dest := destFrames[i]
			ref := refFrames[i]
			gotStr := fmt.Sprintf("Audio frame %d: got      - PTS=%v SeqId=%v InputID=%v Key=%v Codec=%v", i, dest.PTS, dest.SequenceID, dest.InputID, dest.IsKeyFrame, dest.Codec)
			expStr := fmt.Sprintf("Audio frame %d: expected - PTS=%v SeqId=%v InputID=%v Key=%v Codec=%v", i, ref.PTS, ref.SequenceID, ref.InputID, ref.IsKeyFrame, ref.Codec)
			t.Log(gotStr)
			t.Log(expStr)
		}
	}

	// Compare video frames - match by hash instead of index
	refVideoMap := make(map[string]*Frame)
	for _, frame := range refVideo {
		refVideoMap[frameHash(frame)] = frame
	}

	matchedCount := 0
	for i, dest := range destVideo {
		destHash := frameHash(dest)
		ref, found := refVideoMap[destHash]
		if !found {
			printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
			printVideoContext(i, destVideo, refVideo, "Context")
			t.Fatalf("Video frame %d: frame not found in reference (hash: %s) - got PTS=%v SeqId=%v", i, destHash[:16], dest.PTS, dest.SequenceID)
		}

		// Compare PTS (skip if genPTS or RateControl is enabled, as they modify PTS values)
		if !genPTS && !rateControl {
			ptsDiff := dest.PTS - ref.PTS
			if ptsDiff < 0 {
				ptsDiff = -ptsDiff
			}
			if ptsDiff > 1*time.Millisecond {
				printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
				printVideoContext(i, destVideo, refVideo, "Context")
				t.Fatalf("Video frame %d: PTS mismatch - got %v,%v - expected %v,%v (diff: %v)", i, dest.SequenceID, dest.PTS, ref.SequenceID, ref.PTS, ptsDiff)
			}
		}

		// Compare codec
		if dest.Codec != ref.Codec {
			printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
			printVideoContext(i, destVideo, refVideo, "Context")
			t.Fatalf("Video frame %d: codec mismatch - got %s, expected %s", i, dest.Codec, ref.Codec)
		}

		// Compare keyframe flag
		if dest.IsKeyFrame != ref.IsKeyFrame {
			printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
			printVideoContext(i, destVideo, refVideo, "Context")
			t.Fatalf("Video frame %d: IsKeyFrame mismatch - got %v, expected %v", i, dest.IsKeyFrame, ref.IsKeyFrame)
		}

		matchedCount++
		gotStr := fmt.Sprintf("Video frame %d: match - got %v - %v - %v - %v - %v", i, dest.PTS, dest.SequenceID, dest.InputID, dest.IsKeyFrame, dest.Codec)
		expStr := fmt.Sprintf("Video frame %d: match - expected %v - %v - %v - %v - %v", i, ref.PTS, ref.SequenceID, ref.InputID, ref.IsKeyFrame, ref.Codec)

		lastMatchedVideoFrames = append(lastMatchedVideoFrames, gotStr+"\n"+expStr)
		if len(lastMatchedVideoFrames) > maxPrint {
			lastMatchedVideoFrames = lastMatchedVideoFrames[1:]
		}
	}

	// All frames must match - no drops allowed
	if matchedCount != len(refVideo) {
		t.Errorf("Video frames: matched %d/%d frames - missing %d frames", matchedCount, len(refVideo), len(refVideo)-matchedCount)
	}

	// Compare audio frames - match by sequence ID (since PTS is synchronized with video)
	refAudioMap := make(map[int64]*Frame)
	for _, frame := range refAudio {
		refAudioMap[frame.SequenceID] = frame
	}

	matchedCount = 0
	for i, dest := range destAudio {
		ref, found := refAudioMap[dest.SequenceID]
		if !found {
			printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
			printAudioContext(i, destAudio, refAudio, "Context")
			t.Fatalf("Audio frame %d: frame not found in reference (SeqId: %d) - got PTS=%v", i, dest.SequenceID, dest.PTS)
		}

		// Compare payload hash to ensure it's the same frame
		destHash := frameHash(dest)
		refHash := frameHash(ref)
		if destHash != refHash {
			printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
			printAudioContext(i, destAudio, refAudio, "Context")
			t.Fatalf("Audio frame %d: payload hash mismatch (SeqId: %d) - got %s, expected %s", i, dest.SequenceID, destHash[:16], refHash[:16])
		}

		// Skip PTS comparison for audio since AAC/MPEG4Audio is synchronized with video PTS

		// Compare codec
		if dest.Codec != ref.Codec {
			printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
			printAudioContext(i, destAudio, refAudio, "Context")
			t.Fatalf("Audio frame %d: codec mismatch - got %s, expected %s", i, dest.Codec, ref.Codec)
		}

		// Compare keyframe flag
		if dest.IsKeyFrame != ref.IsKeyFrame {
			printLastMatchedFrames(t, lastMatchedVideoFrames, lastMatchedAudioFrames)
			printAudioContext(i, destAudio, refAudio, "Context")
			t.Fatalf("Audio frame %d: IsKeyFrame mismatch - got %v, expected %v", i, dest.IsKeyFrame, ref.IsKeyFrame)
		}

		matchedCount++
		gotStr := fmt.Sprintf("Audio frame %d: match - got %v - %v - %v - %v - %v", i, dest.PTS, dest.SequenceID, dest.InputID, dest.IsKeyFrame, dest.Codec)
		expStr := fmt.Sprintf("Audio frame %d: match - expected %v - %v - %v - %v - %v", i, ref.PTS, ref.SequenceID, ref.InputID, ref.IsKeyFrame, ref.Codec)
		lastMatchedAudioFrames = append(lastMatchedAudioFrames, gotStr+"\n"+expStr)
		if len(lastMatchedAudioFrames) > maxPrint {
			lastMatchedAudioFrames = lastMatchedAudioFrames[1:]
		}
	}

	// All frames must match - no drops allowed
	if matchedCount != len(refAudio) {
		t.Errorf("Audio frames: matched %d/%d frames - missing %d frames", matchedCount, len(refAudio), len(refAudio)-matchedCount)
	}

	if len(destVideo) == len(refVideo) && len(destAudio) == len(refAudio) {
		t.Log("All frames match perfectly!")
	}
}

// Helper to print last matched frames before fatal
func printLastMatchedFrames(t *testing.T, lastMatchedVideoFrames, lastMatchedAudioFrames []string) {
	t.Log("\n--- Last matched VIDEO frames ---")
	for _, s := range lastMatchedVideoFrames {
		t.Log(s)
	}
	t.Log("\n--- Last matched AUDIO frames ---")
	for _, s := range lastMatchedAudioFrames {
		t.Log(s)
	}
}
