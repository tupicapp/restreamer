package inputs

import (
	"context"
	"sync"
	"testing"
	"time"
)

// collectFramesSlowRTMP reads frames slowly from RTMP source with timeout longer than writer timeout
func collectFramesSlowRTMP(reader Stream, readDelay time.Duration, totalTimeout time.Duration) ([]*Frame, []*Frame, error) {
	var videoFrames []*Frame
	var audioFrames []*Frame
	var videoMu sync.Mutex
	var audioMu sync.Mutex

	done := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	videoClosed := false
	audioClosed := false

	// Collect video frames slowly
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case frame, ok := <-reader.GetVideoChan():
				if !ok {
					videoClosed = true
					return
				}
				if frame != nil {
					videoMu.Lock()
					videoFrames = append(videoFrames, frame)
					videoMu.Unlock()
					// Sleep longer than writer timeout (200ms) to cause drops
					select {
					case <-time.After(readDelay):
					case <-done:
						return
					}
				}
			}
		}
	}()

	// Collect audio frames slowly
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case frame, ok := <-reader.GetAudioChan():
				if !ok {
					audioClosed = true
					return
				}
				if frame != nil {
					audioMu.Lock()
					audioFrames = append(audioFrames, frame)
					audioMu.Unlock()
					// Sleep longer than writer timeout (200ms) to cause drops
					select {
					case <-time.After(readDelay):
					case <-done:
						return
					}
				}
			}
		}
	}()

	// Wait for timeout or until both channels are closed
	timeoutChan := time.After(totalTimeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutChan:
			close(done)
			wg.Wait()
			videoMu.Lock()
			audioMu.Lock()
			defer videoMu.Unlock()
			defer audioMu.Unlock()
			return videoFrames, audioFrames, nil
		case <-ticker.C:
			if videoClosed && audioClosed {
				close(done)
				wg.Wait()
				videoMu.Lock()
				audioMu.Lock()
				defer videoMu.Unlock()
				defer audioMu.Unlock()
				return videoFrames, audioFrames, nil
			}
		}
	}
}

func TestRTMPSource_SlowConsumptionCorrectness(t *testing.T) {
	rtmp_url := "rtmp://localhost:1938/live/1"
	// List of RTMP videos to test
	rtmpVideos := []TestVideoConfig{
		{
			Name:        "rtmp_env",
			FilePath:    rtmp_url, // Will be set from environment variable
			Description: "RTMP URL from RTMP_URL environment variable",
			Skip:        false,
		},
		// Add more RTMP videos here as needed
	}

	// Set RTMP URL from environment variable if available
	// rtmpURL := getConfiguredRTMPURL(t)
	requireRTMPPublishing(t, rtmp_url, 10*time.Second)
	// rtmpVideos[0].FilePath = rtmpURL

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

	// Test each video
	for _, video := range rtmpVideos {
		t.Run(video.Name, func(t *testing.T) {
			t.Logf("Testing RTMP video: %s (%s)", video.Name, video.Description)
			t.Logf("Test RTMP URL: %s", video.FilePath)

			// Create RTMP reader
			reader := NewRTMP("test-rtmp-reader", video.FilePath)
			reader.Start()

			// Wait for reader to start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := reader.WaitForStart(ctx)
			if err != nil {
				t.Fatalf("Failed to start RTMP reader: %v", err)
			}
			defer reader.Close()

			// Collect frames slowly - sleep 250ms between reads (longer than 200ms writer timeout)
			readDelay := 250 * time.Millisecond
			totalTimeout := 20 * time.Second
			videoFrames, audioFrames, err := collectFramesSlowRTMP(reader, readDelay, totalTimeout)
			if err != nil {
				t.Fatalf("Failed to collect frames: %v", err)
			}

			if len(videoFrames) == 0 && len(audioFrames) == 0 {
				t.Fatal("No frames collected")
			}

			t.Logf("Collected %d video frames, %d audio frames", len(videoFrames), len(audioFrames))

			// Verify correctness
			verifyFrameCorrectness(t, videoFrames, audioFrames)
		})
	}
}

func verifyFrameCorrectness(t *testing.T, videoFrames []*Frame, audioFrames []*Frame) {
	// 1. Verify sequence IDs are sequential (incrementing by 1)
	// verifySequenceIDs(t, videoFrames, "video")
	// verifySequenceIDs(t, audioFrames, "audio")

	// 2. Verify GOP IDs are correct (should match last keyframe sequence ID)
	verifyGOPIDs(t, videoFrames, "video")
	verifyGOPIDs(t, audioFrames, "audio")

	// 3. Verify video and audio timestamps match (same time ranges)
	verifyTimestampAlignment(t, videoFrames, audioFrames)
}

func verifyGOPIDs(t *testing.T, frames []*Frame, frameType string) {
	if len(frames) == 0 {
		t.Logf("No %s frames to verify GOP IDs", frameType)
		return
	}

	lastKeyframeSeqID := int64(0)
	for i, frame := range frames {
		if frame.IsKeyFrame {
			lastKeyframeSeqID = frame.SequenceID
		}

		if frame.GOPID != lastKeyframeSeqID {
			t.Errorf("%s frame %d: GOP ID mismatch - expected %d (last keyframe seq ID), got %d (seq ID: %d, isKeyFrame: %v)",
				frameType, i, lastKeyframeSeqID, frame.GOPID, frame.SequenceID, frame.IsKeyFrame)
		}
	}

	t.Logf("%s frames: all %d frames have correct GOP IDs", frameType, len(frames))
}

func verifyTimestampAlignment(t *testing.T, videoFrames []*Frame, audioFrames []*Frame) {
	if len(videoFrames) == 0 || len(audioFrames) == 0 {
		t.Log("Skipping timestamp alignment check - missing video or audio frames")
		return
	}

	// Find time ranges for video
	videoRanges := findTimeRanges(videoFrames)
	audioRanges := findTimeRanges(audioFrames)

	t.Logf("Video time ranges: %v", videoRanges)
	t.Logf("Audio time ranges: %v", audioRanges)

	// Check if ranges match (allowing small tolerance)
	tolerance := 100 * time.Millisecond
	mismatches := 0

	for _, vr := range videoRanges {
		found := false
		for _, ar := range audioRanges {
			if rangesOverlap(vr.start, vr.end, ar.start, ar.end, tolerance) {
				found = true
				break
			}
		}
		if !found {
			mismatches++
			t.Errorf("Video time range [%v - %v] has no matching audio range", vr.start, vr.end)
		}
	}

	for _, ar := range audioRanges {
		found := false
		for _, vr := range videoRanges {
			if rangesOverlap(ar.start, ar.end, vr.start, vr.end, tolerance) {
				found = true
				break
			}
		}
		if !found {
			mismatches++
			t.Errorf("Audio time range [%v - %v] has no matching video range", ar.start, ar.end)
		}
	}

	if mismatches == 0 {
		t.Logf("Timestamp alignment: video and audio time ranges match")
	} else {
		t.Errorf("Timestamp alignment: %d mismatches found", mismatches)
	}
}

type timeRange struct {
	start time.Duration
	end   time.Duration
}

func findTimeRanges(frames []*Frame) []timeRange {
	if len(frames) == 0 {
		return nil
	}

	var ranges []timeRange
	currentStart := frames[0].PTS
	currentEnd := frames[0].PTS
	gapThreshold := 1 * time.Second // Consider it a gap if more than 1 second

	for i := 1; i < len(frames); i++ {
		gap := frames[i].PTS - currentEnd
		if gap > gapThreshold {
			// Gap detected, save current range and start new one
			ranges = append(ranges, timeRange{start: currentStart, end: currentEnd})
			currentStart = frames[i].PTS
			currentEnd = frames[i].PTS
		} else {
			currentEnd = frames[i].PTS
		}
	}

	// Add final range
	ranges = append(ranges, timeRange{start: currentStart, end: currentEnd})

	return ranges
}

func rangesOverlap(start1, end1, start2, end2, tolerance time.Duration) bool {
	// Check if ranges overlap with tolerance
	return (start1-tolerance <= end2+tolerance) && (start2-tolerance <= end1+tolerance)
}
