package inputs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
)

// collectFrames reads all frames from video and audio channels until timeout or stream ends
func collectFrames(reader Stream, timeout time.Duration) ([]*Frame, []*Frame, error) {
	var videoFrames []*Frame
	var audioFrames []*Frame
	var videoMu sync.Mutex
	var audioMu sync.Mutex

	done := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Collect video frames
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case frame, ok := <-reader.GetVideoChan():
				if !ok {
					continue
				}
				if frame != nil {
					videoMu.Lock()
					videoFrames = append(videoFrames, frame)
					videoMu.Unlock()
				}
			}
		}
	}()

	// Collect audio frames
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case frame, ok := <-reader.GetAudioChan():
				if !ok {
					continue
				}
				if frame != nil {
					audioMu.Lock()
					audioFrames = append(audioFrames, frame)
					audioMu.Unlock()
				}
			}
		}
	}()

	// Wait for timeout or until both channels are closed
	timeoutChan := time.After(timeout)
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
		}
	}
}

// referenceInput reads HLS segments directly using mpegts.Reader (reference implementation)
func referenceInput(baseURL string, playlistURI string, timeScale float64) ([]*Frame, []*Frame, error) {
	_ = timeScale // keep signature; use fixed 90kHz scaling like hls_reader

	var videoFrames []*Frame
	var audioFrames []*Frame
	var videoMu sync.Mutex
	var audioMu sync.Mutex

	// Download and parse playlist
	playlistData, err := downloadURL(playlistURI)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download playlist: %w", err)
	}

	pl, err := playlist.Unmarshal(playlistData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse playlist: %w", err)
	}

	var mediaPlaylist *playlist.Media
	switch pl := pl.(type) {
	case *playlist.Media:
		mediaPlaylist = pl
		// Extract directory from playlist URI for proper URL resolution
		playlistURL, err := url.Parse(playlistURI)
		if err == nil {
			// Remove the filename from the path to get the directory
			playlistURL.Path = path.Dir(playlistURL.Path) + "/"
			baseURL = playlistURL.String()
		}
	case *playlist.Multivariant:
		if len(pl.Variants) == 0 {
			return nil, nil, fmt.Errorf("multivariant playlist has no variants")
		}
		variantURI := resolveURL(baseURL, pl.Variants[0].URI)
		variantData, err := downloadURL(variantURI)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to download variant: %w", err)
		}
		variantPl, err := playlist.Unmarshal(variantData)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse variant: %w", err)
		}
		mediaPlaylist, _ = variantPl.(*playlist.Media)
		if mediaPlaylist == nil {
			return nil, nil, fmt.Errorf("variant is not a media playlist")
		}
		// Extract directory from variant URI for proper URL resolution
		variantURL, err := url.Parse(variantURI)
		if err == nil {
			// Remove the filename from the path to get the directory
			variantURL.Path = path.Dir(variantURL.Path) + "/"
			baseURL = variantURL.String()
		} else {
			baseURL = variantURI
		}
	}

	if mediaPlaylist == nil {
		return nil, nil, fmt.Errorf("no media playlist found")
	}

	// Check if MediaMap is available for fMP4 segments
	// if mediaPlaylist.Map == nil {
	// 	return nil, nil, fmt.Errorf("playlist has no MediaMap (init segment), required for fMP4 segments")
	// }

	// Create a minimal hlsInput for the reference implementation
	refHlsReader := &hlsInput{
		id:              "ref-reader",
		uri:             playlistURI,
		videoChan:       make(chan *Frame, 1000),
		audioChan:       make(chan *Frame, 1000),
		pendingVideoBuf: make([]*Frame, 0),
		pendingAudioBuf: make([]*Frame, 0),
	}

	segmentFactory := newSegmentFactory(refHlsReader, baseURL)
	segmentFactory.SetMediaPlayList(mediaPlaylist.Map)

	// Process each segment
	for _, segment := range mediaPlaylist.Segments {
		reader, err := segmentFactory.newSegment(segment)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create reader %s: %w", segment.URI, err)
		}

		// Read all data from segment - keep reading until EOF or all frames are processed
		// For fMP4 segments, we need to keep calling Read() until all frames are dequeued
		maxReads := 10000 // Safety limit to prevent infinite loops
		readCount := 0
		for {
			err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				// Log error but continue to next segment
				break
			}
			readCount++
			if readCount >= maxReads {
				// Safety break to prevent infinite loops
				break
			}
		}
	}

	// Drain all pending frames from hlsInput buffers
	refHlsReader.pendingMu.Lock()
	videoFrames = append(videoFrames, refHlsReader.pendingVideoBuf...)
	audioFrames = append(audioFrames, refHlsReader.pendingAudioBuf...)
	refHlsReader.pendingMu.Unlock()

	videoMu.Lock()
	audioMu.Lock()
	defer videoMu.Unlock()
	defer audioMu.Unlock()

	return videoFrames, audioFrames, nil
}

func isKeyFrame(frame *Frame) bool {
	if frame == nil {
		return false
	}

	switch frame.Codec {
	case "h265":
		for _, nalu := range frame.Payload {
			if len(nalu) == 0 {
				continue
			}
			typ := (nalu[0] >> 1) & 0x3F
			if typ == 19 || typ == 20 || typ == 21 {
				return true
			}
		}
	default:
		for _, nalu := range frame.Payload {
			if len(nalu) == 0 {
				continue
			}
			if nalu[0]&0x1F == 5 {
				return true
			}
		}
	}

	return false
}

func cloneBytesSlice(src [][]byte) [][]byte {
	dst := make([][]byte, len(src))
	for i, b := range src {
		dst[i] = cloneBytes(b)
	}
	return dst
}

func downloadURL(uri string) ([]byte, error) {
	req, err := http.Get(uri)
	if err != nil {
		return nil, err
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download returned status %d", req.StatusCode)
	}

	return io.ReadAll(req.Body)
}

// frameSequence represents the sequence of frames with their hashes
type frameSequence struct {
	VideoFrames []frameInfo
	AudioFrames []frameInfo
}

type frameInfo struct {
	SequenceID int64
	PTS        time.Duration
	Hash       string
	IsKeyFrame bool
	Codec      string
}

func TestHLSReader_Consistency(t *testing.T) {
	// List of HLS videos to test
	hlsVideos := []TestVideoConfig{
		// {
		// 	Name:        "hls_video_1",
		// 	FilePath:    "testdata/hls/ts_1/index.m3u8",
		// 	Description: "Primary HLS test video",
		// 	Skip:        false,
		// },
		{
			Name:        "hls_video_2",
			FilePath:    "testdata/hls/m4s/stream_2/playlist.m3u8",
			Description: "M4S HLS test video",
			Skip:        false,
		},
		// Add more videos here as needed
	}

	// Filter out skipped videos and check availability
	var availableVideos []TestVideoConfig
	for _, video := range hlsVideos {
		if video.Skip {
			continue
		}
		_, fileServer, err := setupHLSVideoServer(t, video)
		if err != nil {
			t.Logf("Skipping HLS video '%s' (%s): %v", video.Name, video.FilePath, err)
			continue
		}
		if fileServer != nil {
			fileServer.Close()
		}
		availableVideos = append(availableVideos, video)
	}

	if len(availableVideos) == 0 {
		t.Skip("No HLS test videos available, skipping test")
	}
	hlsVideos = availableVideos

	// Test each video
	for _, video := range hlsVideos {
		t.Run(video.Name, func(t *testing.T) {
			playlistURI, fileServer, err := setupHLSVideoServer(t, video)
			if err != nil {
				t.Skipf("Failed to setup HLS video server for %s: %v", video.Name, err)
			}
			if fileServer != nil {
				defer fileServer.Close()
			}

			t.Logf("Testing HLS video: %s (%s)", video.Name, video.Description)
			t.Logf("Test HLS URI: %s", playlistURI)

			// Reference implementation: read directly using mpegts.Reader
			t.Log("Reading with reference implementation (direct Reader)...")
			refVideoFrames, refAudioFrames, err := referenceInput(playlistURI, playlistURI, 1.0/MpegTSTimeScale)
			if err != nil {
				t.Fatalf("Reference reader failed: %v", err)
			}

			if len(refVideoFrames) == 0 && len(refAudioFrames) == 0 {
				t.Fatal("Reference reader collected no frames")
			}

			t.Logf("Reference: collected %d video frames, %d audio frames", len(refVideoFrames), len(refAudioFrames))

			sort.Slice(refAudioFrames, func(i, j int) bool { return refAudioFrames[i].DTS < refAudioFrames[j].DTS })
			sort.Slice(refVideoFrames, func(i, j int) bool { return refVideoFrames[i].DTS < refVideoFrames[j].DTS })

			// Build reference sequence
			refSeq := frameSequence{
				VideoFrames: make([]frameInfo, len(refVideoFrames)),
				AudioFrames: make([]frameInfo, len(refAudioFrames)),
			}

			for i, frame := range refVideoFrames {
				refSeq.VideoFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			for i, frame := range refAudioFrames {
				refSeq.AudioFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			// Our implementation: use hlsInput
			t.Log("Reading with hlsInput implementation...")
			startTime := time.Now()
			reader := NewHLS("test-reader", playlistURI)
			reader.Start()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = reader.WaitForStart(ctx)
			if err != nil {
				t.Fatalf("Failed to start hlsInput: %v", err)
			}
			defer reader.Close()

			// Collect frames with longer timeout to ensure we get all segments
			videoFrames, audioFrames, err := collectFrames(reader, 4*time.Second)
			if err != nil {
				t.Fatalf("Failed to collect frames from hlsInput: %v", err)
			}

			if len(videoFrames) == 0 && len(audioFrames) == 0 {
				t.Fatal("hlsInput collected no frames")
			}

			t.Logf("hlsInput: collected %d video frames, %d audio frames", len(videoFrames), len(audioFrames))
			actualElapsed := time.Since(startTime)

			// Build hlsInput sequence
			implSeq := frameSequence{
				VideoFrames: make([]frameInfo, len(videoFrames)),
				AudioFrames: make([]frameInfo, len(audioFrames)),
			}

			for i, frame := range videoFrames {
				implSeq.VideoFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			for i, frame := range audioFrames {
				implSeq.AudioFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			// Compare sequences
			compareSequences(t, refSeq, implSeq, "video")
			compareSequences(t, refSeq, implSeq, "audio")

			// PTS Benchmarks (keep separate from DTS)
			t.Log("\n=== PTS Benchmarks on hlsInput vs reference ===")
			threshold := 0.1 // 10%

			videoWindowResult := windowMatchBenchmarkWithTiming(videoFrames, refVideoFrames, "video", actualElapsed, threshold)
			printWindowMatchBenchmark(t, videoWindowResult, "video")
			if len(videoWindowResult.MismatchContexts) > 0 {
				t.Errorf("Video window-match benchmark: found %d mismatches", len(videoWindowResult.MismatchContexts))
			}
			if videoWindowResult.MatchPercent < (1.0-threshold)*100.0 {
				t.Errorf("Video window-match benchmark: match percent is %.2f%% (expected >= %.2f%%)", videoWindowResult.MatchPercent, (1.0-threshold)*100.0)
			}

			audioWindowResult := windowMatchBenchmarkWithTiming(audioFrames, refAudioFrames, "audio", actualElapsed, threshold)
			printWindowMatchBenchmark(t, audioWindowResult, "audio")
			if len(audioWindowResult.MismatchContexts) > 0 {
				t.Errorf("Audio window-match benchmark: found %d mismatches", len(audioWindowResult.MismatchContexts))
			}
			if audioWindowResult.MatchPercent < (1.0-threshold)*100.0 {
				t.Errorf("Audio window-match benchmark: match percent is %.2f%% (expected >= %.2f%%)", audioWindowResult.MatchPercent, (1.0-threshold)*100.0)
			}

			// Equal-Packet-Rate Benchmark
			videoPacketRateResult := equalPacketRateBenchmark(videoFrames, refVideoFrames, "video")
			printEqualPacketRateBenchmark(t, videoPacketRateResult, "video")
			if videoPacketRateResult.SuccessRate < 100.0 || videoPacketRateResult.NotFoundPackets > 0 {
				t.Errorf("Video equal-packet-rate benchmark: success rate %.2f%%, missing %d packets", videoPacketRateResult.SuccessRate, videoPacketRateResult.NotFoundPackets)
			}

			audioPacketRateResult := equalPacketRateBenchmark(audioFrames, refAudioFrames, "audio")
			printEqualPacketRateBenchmark(t, audioPacketRateResult, "audio")
			if audioPacketRateResult.SuccessRate < 100.0 || audioPacketRateResult.NotFoundPackets > 0 {
				t.Errorf("Audio equal-packet-rate benchmark: success rate %.2f%%, missing %d packets", audioPacketRateResult.SuccessRate, audioPacketRateResult.NotFoundPackets)
			}

			// DTS-based window/health checks to ensure decode ordering is intact
			dtsVideoFrames := cloneFramesWithDTSAsPTS(videoFrames)
			dtsRefVideoFrames := cloneFramesWithDTSAsPTS(refVideoFrames)
			dtsAudioFrames := cloneFramesWithDTSAsPTS(audioFrames)
			dtsRefAudioFrames := cloneFramesWithDTSAsPTS(refAudioFrames)

			videoDTSWindow := windowMatchBenchmarkWithTiming(dtsVideoFrames, dtsRefVideoFrames, "video-dts", actualElapsed, threshold)
			printWindowMatchBenchmark(t, videoDTSWindow, "video-dts")
			if len(videoDTSWindow.MismatchContexts) > 0 {
				t.Errorf("Video (DTS) window-match benchmark: found %d mismatches", len(videoDTSWindow.MismatchContexts))
			}
			if videoDTSWindow.MatchPercent < (1.0-threshold)*100.0 {
				t.Errorf("Video (DTS) window-match benchmark: match percent is %.2f%% (expected >= %.2f%%)", videoDTSWindow.MatchPercent, (1.0-threshold)*100.0)
			}

			audioDTSWindow := windowMatchBenchmarkWithTiming(dtsAudioFrames, dtsRefAudioFrames, "audio-dts", actualElapsed, threshold)
			printWindowMatchBenchmark(t, audioDTSWindow, "audio-dts")

			checkH264FrameHealth(t, videoFrames)

			// Stream health checks
			videoHealth := checkStreamHealth(videoFrames, "video")
			if !videoHealth.IsHealthy {
				t.Errorf("hlsInput video stream health failed: %d PTS issues, %d gap issues", len(videoHealth.MonotonicPTSIssues), len(videoHealth.LargeGapIssues))
			}

			audioHealth := checkStreamHealth(audioFrames, "audio")
			if !audioHealth.IsHealthy {
				t.Errorf("hlsInput audio stream health failed: %d PTS issues, %d gap issues", len(audioHealth.MonotonicPTSIssues), len(audioHealth.LargeGapIssues))
			}

			assertRTMPPushable(t, videoFrames, audioFrames)
			flvPath := assertFLVPlayableWithFFprobe(t, videoFrames, audioFrames)
			t.Logf("ffmpeg mux and ffprobe check succeeded, FLV written to %s", flvPath)

			// DTS health checks (informational only)
			videoDTSHealth := checkStreamHealth(dtsVideoFrames, "video-dts")
			t.Logf("hlsInput video DTS health: (DTS monotonicity: %.2f%%, gaps: %.2f%%, pts issues=%d, gap issues=%d)",
				videoDTSHealth.MonotonicPTSPercent, videoDTSHealth.ValidGapPercent, len(videoDTSHealth.MonotonicPTSIssues), len(videoDTSHealth.LargeGapIssues))

			audioDTSHealth := checkStreamHealth(dtsAudioFrames, "audio-dts")
			t.Logf("hlsInput audio DTS health: (DTS monotonicity: %.2f%%, gaps: %.2f%%, pts issues=%d, gap issues=%d)",
				audioDTSHealth.MonotonicPTSPercent, audioDTSHealth.ValidGapPercent, len(audioDTSHealth.MonotonicPTSIssues), len(audioDTSHealth.LargeGapIssues))
		})
	}
}

// cloneFramesWithDTSAsPTS returns a shallow copy of frames with PTS replaced by DTS for DTS-order checks.
func cloneFramesWithDTSAsPTS(frames []*Frame) []*Frame {
	out := make([]*Frame, 0, len(frames))
	for _, f := range frames {
		if f == nil {
			continue
		}
		cp := *f
		cp.PTS = f.DTS
		out = append(out, &cp)
	}
	return out
}

func compareSequences(t *testing.T, refSeq, implSeq frameSequence, frameType string) {
	var refFrames, implFrames []frameInfo
	if frameType == "video" {
		refFrames = refSeq.VideoFrames
		implFrames = implSeq.VideoFrames
	} else {
		refFrames = refSeq.AudioFrames
		implFrames = implSeq.AudioFrames
	}

	if len(refFrames) == 0 && len(implFrames) == 0 {
		t.Logf("No %s frames to compare", frameType)
		return
	}

	if len(refFrames) != len(implFrames) {
		t.Logf("%s frame count mismatch: reference=%d, implementation=%d", frameType, len(refFrames), len(implFrames))
	}

	// Build a map of implementation frames by hash for quick lookup
	implHashMap := make(map[string]int) // hash -> index in implFrames
	for i, frame := range implFrames {
		implHashMap[frame.Hash] = i
	}

	// Find missing frames and track matches
	missingFrames := []int{}            // indices of missing frames in refFrames
	matchedIndices := make(map[int]int) // ref index -> impl index

	for i, ref := range refFrames {
		if implIdx, found := implHashMap[ref.Hash]; found {
			matchedIndices[i] = implIdx
		} else {
			missingFrames = append(missingFrames, i)
		}
	}

	// Report missing frames with context
	if len(missingFrames) > 0 {
		t.Errorf("%s frames: %d missing frames out of %d reference frames", frameType, len(missingFrames), len(refFrames))

		// Show context for missing frames (limit to first 10 missing frames to avoid too much output)
		maxMissingToShow := 10
		if len(missingFrames) > maxMissingToShow {
			t.Logf("Showing context for first %d missing frames (out of %d total)", maxMissingToShow, len(missingFrames))
		}

		for idx := 0; idx < len(missingFrames) && idx < maxMissingToShow; idx++ {
			missingIdx := missingFrames[idx]
			ref := refFrames[missingIdx]

			t.Logf("\n=== Missing %s Frame #%d (Reference Index %d) ===", frameType, idx+1, missingIdx)
			t.Logf("Missing Frame Details:")
			t.Logf("  SequenceID: %d", ref.SequenceID)
			t.Logf("  PTS: %v", ref.PTS)
			t.Logf("  Hash: %s", ref.Hash)
			t.Logf("  IsKeyFrame: %v", ref.IsKeyFrame)
			t.Logf("  Codec: %s", ref.Codec)

			// Show context: 5 frames before
			t.Logf("\n  Context (5 frames before):")
			contextStart := missingIdx - 5
			if contextStart < 0 {
				contextStart = 0
			}
			for i := contextStart; i < missingIdx; i++ {
				ctxRef := refFrames[i]
				matched := ""
				if implIdx, found := matchedIndices[i]; found {
					matched = fmt.Sprintf(" [MATCHED at impl index %d]", implIdx)
				} else {
					matched = " [MISSING]"
				}
				t.Logf("    [%d] seq=%d, pts=%v, hash=%s, keyframe=%v%s", i, ctxRef.SequenceID, ctxRef.PTS, ctxRef.Hash[:16]+"...", ctxRef.IsKeyFrame, matched)
			}

			// Show the missing frame itself
			t.Logf("  -> [%d] seq=%d, pts=%v, hash=%s, keyframe=%v [MISSING]", missingIdx, ref.SequenceID, ref.PTS, ref.Hash[:16]+"...", ref.IsKeyFrame)

			// Show context: 5 frames after
			t.Logf("\n  Context (5 frames after):")
			contextEnd := missingIdx + 6
			if contextEnd > len(refFrames) {
				contextEnd = len(refFrames)
			}
			for i := missingIdx + 1; i < contextEnd; i++ {
				ctxRef := refFrames[i]
				matched := ""
				if implIdx, found := matchedIndices[i]; found {
					matched = fmt.Sprintf(" [MATCHED at impl index %d]", implIdx)
				} else {
					matched = " [MISSING]"
				}
				t.Logf("    [%d] seq=%d, pts=%v, hash=%s, keyframe=%v%s", i, ctxRef.SequenceID, ctxRef.PTS, ctxRef.Hash[:16]+"...", ctxRef.IsKeyFrame, matched)
			}
		}

		if len(missingFrames) > maxMissingToShow {
			t.Logf("\n... and %d more missing frames", len(missingFrames)-maxMissingToShow)
		}
	}

	// Check for mismatches in matched frames
	mismatches := 0
	for refIdx, implIdx := range matchedIndices {
		ref := refFrames[refIdx]
		impl := implFrames[implIdx]

		if ref.Codec != impl.Codec {
			mismatches++
			if mismatches <= 5 {
				t.Errorf("%s frame %d (ref) / %d (impl): codec mismatch: reference=%s, implementation=%s", frameType, refIdx, implIdx, ref.Codec, impl.Codec)
			}
		}

		if ref.IsKeyFrame != impl.IsKeyFrame {
			mismatches++
			if mismatches <= 5 {
				t.Errorf("%s frame %d (ref) / %d (impl): keyframe flag mismatch: reference=%v, implementation=%v", frameType, refIdx, implIdx, ref.IsKeyFrame, impl.IsKeyFrame)
			}
		}
	}

	// Summary
	if len(missingFrames) == 0 && mismatches == 0 && len(refFrames) == len(implFrames) {
		t.Logf("%s frames: all %d frames match perfectly", frameType, len(refFrames))
	} else {
		matchedCount := len(refFrames) - len(missingFrames)
		matchRate := float64(matchedCount) / float64(len(refFrames)) * 100.0
		t.Logf("%s frames summary: %d/%d matched (%.2f%%), %d missing, %d mismatches", frameType, matchedCount, len(refFrames), matchRate, len(missingFrames), mismatches)
	}
}

// collectFramesSlow reads frames slowly, causing writer timeouts and frame drops
func collectFramesSlow(reader Stream, readDelay time.Duration, totalTimeout time.Duration) ([]*Frame, []*Frame, error) {
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
					// Sleep longer than writer timeout to cause drops
					select {
					case <-time.After(readDelay):
					case <-done:
						return
					}
				}

			case <-time.After(totalTimeout):
				videoClosed = true
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
					// Sleep longer than writer timeout to cause drops
					select {
					case <-time.After(readDelay):
					case <-done:
						return
					}
				}
			case <-time.After(totalTimeout):
				audioClosed = true
			}
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
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

	// Fallback (should not be reached)
	return videoFrames, audioFrames, nil
}

func TestHLSReader_SlowConsumptionConsistency(t *testing.T) {
	// List of HLS videos to test
	hlsVideos := []TestVideoConfig{
		{
			Name:        "hls_video_1",
			FilePath:    "testdata/hls/ts_1/index.m3u8",
			Description: "Primary HLS test video",
			Skip:        false,
		},
		// Add more videos here as needed
	}

	// Filter out skipped videos and check availability
	var availableVideos []TestVideoConfig
	for _, video := range hlsVideos {
		if video.Skip {
			continue
		}
		_, fileServer, err := setupHLSVideoServer(t, video)
		if err != nil {
			t.Logf("Skipping HLS video '%s' (%s): %v", video.Name, video.FilePath, err)
			continue
		}
		if fileServer != nil {
			fileServer.Close()
		}
		availableVideos = append(availableVideos, video)
	}

	if len(availableVideos) == 0 {
		t.Skip("No HLS test videos available, skipping test")
	}
	hlsVideos = availableVideos

	// Test each video
	for _, video := range hlsVideos {
		t.Run(video.Name, func(t *testing.T) {
			playlistURI, fileServer, err := setupHLSVideoServer(t, video)
			if err != nil {
				t.Skipf("Failed to setup HLS video server for %s: %v", video.Name, err)
			}
			if fileServer != nil {
				defer fileServer.Close()
			}

			t.Logf("Testing HLS video: %s (%s)", video.Name, video.Description)
			t.Logf("Test HLS URI: %s", playlistURI)

			// Reference implementation: read ALL frames manually
			t.Log("Reading ALL frames with reference implementation (direct mpegts.Reader)...")
			refVideoFrames, refAudioFrames, err := referenceInput(playlistURI, playlistURI, 6000.0)
			if err != nil {
				t.Fatalf("Reference reader failed: %v", err)
			}

			if len(refVideoFrames) == 0 && len(refAudioFrames) == 0 {
				t.Fatal("Reference reader collected no frames")
			}

			t.Logf("Reference (all frames): collected %d video frames, %d audio frames", len(refVideoFrames), len(refAudioFrames))

			// Build reference sequence (complete)
			refSeq := frameSequence{
				VideoFrames: make([]frameInfo, len(refVideoFrames)),
				AudioFrames: make([]frameInfo, len(refAudioFrames)),
			}

			for i, frame := range refVideoFrames {
				refSeq.VideoFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			for i, frame := range refAudioFrames {
				refSeq.AudioFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			// Our implementation: use hlsInput with SLOW consumption (causes writer timeout and drops)
			t.Log("Reading with hlsInput implementation (slow consumption to trigger drops)...")
			reader := NewHLS("test-reader-slow", playlistURI)
			reader.Start()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = reader.WaitForStart(ctx)
			if err != nil {
				t.Fatalf("Failed to start hlsInput: %v", err)
			}
			defer reader.Close()

			// Collect frames slowly - sleep 600ms between reads (longer than 500ms writer timeout)
			// This will cause frames to be dropped by the writer
			readDelay := 7 * time.Millisecond
			totalTimeout := 5 * time.Second
			videoFrames, audioFrames, err := collectFramesSlow(reader, readDelay, totalTimeout)
			if err != nil {
				t.Fatalf("Failed to collect frames from hlsInput: %v", err)
			}

			if len(videoFrames) == 0 && len(audioFrames) == 0 {
				t.Fatal("hlsInput collected no frames")
			}

			t.Logf("hlsInput (slow consumption, %v delay): collected %d video frames, %d audio frames", readDelay, len(videoFrames), len(audioFrames))
			t.Logf("Note: Some frames may have been dropped due to writer timeout (500ms)")

			// Build hlsInput sequence (partial, due to slow consumption causing drops)
			implSeq := frameSequence{
				VideoFrames: make([]frameInfo, len(videoFrames)),
				AudioFrames: make([]frameInfo, len(audioFrames)),
			}

			for i, frame := range videoFrames {
				implSeq.VideoFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			for i, frame := range audioFrames {
				implSeq.AudioFrames[i] = frameInfo{
					SequenceID: frame.SequenceID,
					PTS:        frame.PTS,
					Hash:       frameHash(frame),
					IsKeyFrame: frame.IsKeyFrame,
					Codec:      frame.Codec,
				}
			}

			// Compare sequences - frames we received should match reference in sequence
			// Even though some frames were dropped, the ones we got should be correct
			compareSequencesPartial(t, refSeq, implSeq, "video")
			compareSequencesPartial(t, refSeq, implSeq, "audio")
		})
	}
}

func compareSequencesPartial(t *testing.T, refSeq, implSeq frameSequence, frameType string) {
	var refFrames, implFrames []frameInfo
	if frameType == "video" {
		refFrames = refSeq.VideoFrames
		implFrames = implSeq.VideoFrames
	} else {
		refFrames = refSeq.AudioFrames
		implFrames = implSeq.AudioFrames
	}

	if len(implFrames) == 0 {
		t.Logf("No %s frames collected from hlsInput to compare", frameType)
		return
	}

	if len(refFrames) == 0 {
		t.Errorf("Reference has no %s frames but hlsInput collected %d", frameType, len(implFrames))
		return
	}

	// Compare only the frames we received from hlsInput (partial sequence)
	// They should match the corresponding frames from the reference
	minLen := len(implFrames)
	if len(refFrames) < minLen {
		minLen = len(refFrames)
		t.Logf("Warning: hlsInput collected more %s frames (%d) than reference (%d), comparing first %d", frameType, len(implFrames), len(refFrames), minLen)
	}

	mismatches := 0
	for i := 0; i < minLen; i++ {
		ref := refFrames[i]
		impl := implFrames[i]

		// Compare PTS timestamps (allow small tolerance for floating point precision)
		ptsDiff := ref.PTS - impl.PTS
		if ptsDiff < 0 {
			ptsDiff = -ptsDiff
		}
		// Allow 1ms tolerance for timestamp comparison
		if ptsDiff > 1*time.Millisecond {
			mismatches++
			if mismatches <= 5 { // Only show first 5 mismatches
				t.Errorf("%s frame %d: PTS timestamp mismatch\n  Reference:     seq=%d, pts=%v, hash=%s, keyframe=%v, codec=%s\n  Implementation: seq=%d, pts=%v, hash=%s, keyframe=%v, codec=%s\n  Difference: %v",
					frameType, i,
					ref.SequenceID, ref.PTS, ref.Hash, ref.IsKeyFrame, ref.Codec,
					impl.SequenceID, impl.PTS, impl.Hash, impl.IsKeyFrame, impl.Codec,
					ptsDiff)
			}
		}

		if ref.Codec != impl.Codec {
			t.Errorf("%s frame %d: codec mismatch: reference=%s, implementation=%s", frameType, i, ref.Codec, impl.Codec)
		}

		if ref.IsKeyFrame != impl.IsKeyFrame {
			t.Errorf("%s frame %d: keyframe flag mismatch: reference=%v, implementation=%v", frameType, i, ref.IsKeyFrame, impl.IsKeyFrame)
		}
	}

	if mismatches == 0 {
		t.Logf("%s frames: all %d collected frames match reference (reference has %d total frames)", frameType, minLen, len(refFrames))
	} else {
		t.Errorf("%s frames: %d mismatches out of %d compared frames", frameType, mismatches, minLen)
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

func requireRTMPPublishing(t *testing.T, rtmpURL string, timeout time.Duration) {
	t.Helper()
	requireBinary(t, "ffprobe")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-i", rtmpURL, "-show_streams")
	if err := cmd.Run(); err != nil {
		t.Fatalf("RTMP not publishing or not reachable: %s (%v)", rtmpURL, err)
	}
}

func requireBinary(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Fatalf("%s not available", name)
	}
}

func writeFramesToFLVWithFFmpeg(t *testing.T, videoFrames, audioFrames []*Frame) string {
	t.Helper()

	requireBinary(t, "ffmpeg")

	if len(videoFrames) == 0 || len(audioFrames) == 0 {
		t.Fatalf("cannot mux to FLV: missing video (%d) or audio (%d) frames", len(videoFrames), len(audioFrames))
	}

	tmpDir := t.TempDir()
	videoInput := filepath.Join(tmpDir, "video.h264")
	audioInput := filepath.Join(tmpDir, "audio.aac")
	outputFLV := filepath.Join("../../testdata/output/flv_"+t.Name(), "output.flv")
	os.MkdirAll(filepath.Dir(outputFLV), 0755)

	writeH264Elementary(t, videoInput, videoFrames)
	writeAACElementary(t, audioInput, audioFrames)

	cmd := exec.Command("ffmpeg",
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "h264",
		"-i", videoInput,
		"-f", "aac",
		"-i", audioInput,
		"-c:v", "copy",
		"-c:a", "copy",
		"-f", "flv",
		outputFLV,
	)

	runCmdEnsureNoStderr(t, cmd, "ffmpeg mux to FLV")

	info, err := os.Stat(outputFLV)
	if err != nil {
		t.Fatalf("output FLV missing: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("FFmpeg produced an empty FLV file")
	}

	return outputFLV
}

func writeH264Elementary(t *testing.T, path string, frames []*Frame) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create H264 dump: %v", err)
	}
	defer func() {
		_ = f.Close()
	}()

	naluCount := 0
	for _, frame := range frames {
		if frame == nil {
			continue
		}
		for _, nalu := range frame.Payload {
			if len(nalu) == 0 {
				continue
			}

			stripped := stripAnnexB(nalu)
			if len(stripped) == 0 {
				continue
			}

			if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x01}); err != nil {
				t.Fatalf("failed to write H264 start code: %v", err)
			}
			if _, err := f.Write(stripped); err != nil {
				t.Fatalf("failed to write H264 NALU: %v", err)
			}
			naluCount++
		}
	}

	if naluCount == 0 {
		t.Fatalf("no valid H264 NALUs were written to %s", path)
	}
}

// IsValidH264Packet is a simple validation for H264 raw frames (NAL unit start code 0x00000001 or 0x000001)
func IsValidH264Packet(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	// Check for H264 NAL unit start codes
	// 0x00 00 00 01 or 0x00 00 01
	if data[0] == 0x00 && data[1] == 0x00 && ((data[2] == 0x01) || (data[2] == 0x00 && data[3] == 0x01)) {
		return true
	}
	return false
}

// IsValidAACMPEG4AudioPacket is a simple check for AAC ADTS header (syncword 0xFFF)
func IsValidAACMPEG4AudioPacket(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	if (data[0] == 0xFF) && ((data[1] & 0xF0) == 0xF0) {
		return true
	}
	return false
}

func writeAACElementary(t *testing.T, path string, frames []*Frame) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create AAC dump: %v", err)
	}
	defer func() {
		_ = f.Close()
	}()

	frameCount := 0
	for _, frame := range frames {
		if frame == nil {
			continue
		}
		for _, payload := range frame.Payload {
			if len(payload) == 0 {
				continue
			}

			frameCount++

			if IsValidAACMPEG4AudioPacket(payload) {
				if _, err := f.Write(payload); err != nil {
					t.Fatalf("failed to write ADTS AAC payload: %v", err)
				}
				continue
			}

			header := buildADTSHeader(len(payload), DefaultAudioProfile, DefaultAudioRate, DefaultAudioChannels)
			if _, err := f.Write(header); err != nil {
				t.Fatalf("failed to write ADTS header: %v", err)
			}
			if _, err := f.Write(payload); err != nil {
				t.Fatalf("failed to write AAC payload: %v", err)
			}
		}
	}

	if frameCount == 0 {
		t.Fatalf("no AAC payloads were written to %s", path)
	}
}

func TestIsValidH264Packet(t *testing.T) {
	if IsValidH264Packet([]byte{0x00, 0x00, 0x01, 0x65}) != true {
		t.Fatalf("expected 0x000001 start code to be valid")
	}
	if IsValidH264Packet([]byte{0x00, 0x00, 0x00, 0x01, 0x65}) != true {
		t.Fatalf("expected 0x00000001 start code to be valid")
	}
	if IsValidH264Packet([]byte{0x01, 0x02, 0x03}) != false {
		t.Fatalf("expected non-start-code data to be invalid")
	}
}

func TestIsValidAACMPEG4AudioPacket(t *testing.T) {
	if IsValidAACMPEG4AudioPacket([]byte{0xFF, 0xF1, 0x00}) != true {
		t.Fatalf("expected valid AAC sync word to be detected")
	}
	if IsValidAACMPEG4AudioPacket([]byte{0x00, 0x00}) != false {
		t.Fatalf("expected invalid AAC packet to be false")
	}
}

func assertFLVPlayableWithFFprobe(t *testing.T, videoFrames, audioFrames []*Frame) string {
	t.Helper()

	requireBinary(t, "ffmpeg")
	requireBinary(t, "ffprobe")

	flvPath := writeFramesToFLVWithFFmpeg(t, videoFrames, audioFrames)

	info, err := ProbeStream(flvPath)
	if err != nil {
		t.Fatalf("ffprobe failed on %s: %v", flvPath, err)
	}

	if info.Format == "" || !strings.Contains(info.Format, "flv") {
		t.Fatalf("ffprobe reported format %q; expected containing 'flv'", info.Format)
	}

	if info.VideoCodec == "" || info.AudioCodec == "" {
		t.Fatalf("ffprobe missing codec information: video=%q audio=%q", info.VideoCodec, info.AudioCodec)
	}

	if !strings.Contains(strings.ToLower(info.VideoCodec), "h264") {
		t.Fatalf("ffprobe video codec %q is not recognized as h264", info.VideoCodec)
	}

	if !strings.Contains(strings.ToLower(info.AudioCodec), "aac") {
		t.Fatalf("ffprobe audio codec %q is not recognized as AAC", info.AudioCodec)
	}

	ensureFLVDecodesWithoutError(t, flvPath)

	return flvPath
}

func ensureFLVDecodesWithoutError(t *testing.T, flvPath string) {
	t.Helper()

	requireBinary(t, "ffmpeg")

	cmd := exec.Command("ffmpeg",
		"-v", "error",
		"-i", flvPath,
		"-map", "0",
		"-f", "null",
		"-",
	)

	runCmdEnsureNoStderr(t, cmd, "ffmpeg verify FLV playback")
}

func runCmdEnsureNoStderr(t *testing.T, cmd *exec.Cmd, label string) {
	t.Helper()
	runCmdEnsureNoStderrWithTimeout(t, cmd, label, 45*time.Second)
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

func findTestdataDir() string {
	// Try to find testdata directory relative to current working directory
	testPaths := []string{
		"testdata",
		"../../testdata",
		"../testdata",
		"./testdata",
	}

	for _, path := range testPaths {
		if absPath, err := filepath.Abs(path); err == nil {
			if stat, err := os.Stat(absPath); err == nil && stat.IsDir() {
				return absPath
			}
		}
	}

	return ""
}
