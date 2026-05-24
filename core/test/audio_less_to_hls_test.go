package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	streaminputs "github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
)

func TestAudioLessToHLS(t *testing.T) {
	runCompatRTMPToHLSTest(t, "audio-less", testRTMPAudioLessURL, true)
}

func runCompatRTMPToHLSTest(t *testing.T, testID, sourceURL string, realVideoExpected bool) {
	t.Helper()

	requireBinary(t, "ffprobe")

	const (
		runDuration      = 6 * time.Second
		noFrameTimeout   = 2 * time.Second
		minFinalSegments = 4
	)

	requireRTMPPublishing(t, sourceURL, 10*time.Second)

	outDir := filepath.Join(t.TempDir(), testID+"-hls-out")
	outFolder := storage.NewFolder(outDir)

	input := streaminputs.NewCompatibleInput(streaminputs.NewRTMP(testID+"-compat-in", sourceURL))
	dest, err := outputs.NewHLSLiveDestination(
		testID+"-hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(1*time.Second),
		outputs.WithHLSPlaylistSize(12),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination() error = %v", err)
	}

	videoIn := input.GetVideoChan()
	audioIn := input.GetAudioChan()
	videoOut := dest.GetVideoChan()
	audioOut := dest.GetAudioChan()

	var forwardWG sync.WaitGroup
	var videoCount int64
	var audioCount int64
	var inputVideoHashesMu sync.Mutex
	inputVideoHashes := make([]string, 0, 256)
	var inputVideoPTSMu sync.Mutex
	inputVideoPTS := make([]float64, 0, 256)
	lastFrameAt := atomic.Int64{}
	lastFrameAt.Store(time.Now().UnixNano())

	forwardWG.Add(2)
	go func() {
		defer forwardWG.Done()
		for {
			frame, ok := <-videoIn
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			atomic.AddInt64(&videoCount, 1)
			inputVideoHashesMu.Lock()
			inputVideoHashes = append(inputVideoHashes, frameHash(frame))
			inputVideoHashesMu.Unlock()
			inputVideoPTSMu.Lock()
			inputVideoPTS = append(inputVideoPTS, frame.PTS.Seconds())
			inputVideoPTSMu.Unlock()
			lastFrameAt.Store(time.Now().UnixNano())
			videoOut <- frame
		}
	}()

	go func() {
		defer forwardWG.Done()
		for {
			frame, ok := <-audioIn
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			atomic.AddInt64(&audioCount, 1)
			lastFrameAt.Store(time.Now().UnixNano())
			audioOut <- frame
		}
	}()

	input.Start()
	dest.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := input.(interface{ WaitForStart(context.Context) error }).WaitForStart(startCtx); err != nil {
		t.Fatalf("input WaitForStart() error = %v", err)
	}
	if err := dest.WaitForStart(startCtx); err != nil {
		t.Fatalf("dest WaitForStart() error = %v", err)
	}

	// Align cadence measurement window to steady-state runtime after both sides start.
	atomic.StoreInt64(&videoCount, 0)
	atomic.StoreInt64(&audioCount, 0)
	inputVideoHashesMu.Lock()
	inputVideoHashes = inputVideoHashes[:0]
	inputVideoHashesMu.Unlock()
	inputVideoPTSMu.Lock()
	inputVideoPTS = inputVideoPTS[:0]
	inputVideoPTSMu.Unlock()
	lastFrameAt.Store(time.Now().UnixNano())

	deadline := time.Now().Add(runDuration)
	for time.Now().Before(deadline) {
		last := time.Unix(0, lastFrameAt.Load())
		if time.Since(last) > noFrameTimeout {
			t.Fatalf("no frames forwarded for %v during run", noFrameTimeout)
		}
		time.Sleep(100 * time.Millisecond)
	}

	input.Close()
	forwardWG.Wait()
	dest.Close()

	if atomic.LoadInt64(&videoCount) == 0 {
		t.Fatal("expected forwarded video frames, got 0")
	}
	if atomic.LoadInt64(&audioCount) == 0 {
		t.Fatal("expected forwarded audio frames from compat input, got 0")
	}

	playlistPath := filepath.Join(outDir, "stream.m3u8")
	waitForHLSArtifacts(t, outDir, 5*time.Second, minFinalSegments)
	assertHLSPlaylistLooksValid(t, playlistPath)

	content, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("read playlist %s: %v", playlistPath, err)
	}
	if len(content) == 0 {
		t.Fatalf("playlist %s is empty", playlistPath)
	}

	segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	sort.Strings(segmentFiles)
	if len(segmentFiles) < minFinalSegments {
		t.Fatalf("expected at least %d finalized HLS segments, got %d", minFinalSegments, len(segmentFiles))
	}

	if err := checkSegmentStartsIncrease(segmentFiles); err != nil {
		t.Fatal(err)
	}
	if err := checkSegmentBoundaryContinuity(segmentFiles); err != nil {
		t.Fatal(err)
	}
	if err := checkTransportStreamPackets(playlistPath); err != nil {
		t.Fatal(err)
	}
	if err := checkPlaybackPacingFromProbe(playlistPath, runDuration); err != nil {
		t.Fatal(err)
	}
	if realVideoExpected {
		inputVideoHashesMu.Lock()
		forwardedVideoHashes := append([]string(nil), inputVideoHashes...)
		inputVideoHashesMu.Unlock()
		if err := checkVideoReplayAgainstInput(segmentFiles, playlistPath, forwardedVideoHashes); err != nil {
			t.Fatal(err)
		}

		inputVideoPTSMu.Lock()
		forwardedVideoPTS := append([]float64(nil), inputVideoPTS...)
		inputVideoPTSMu.Unlock()
		if err := checkOutputVideoCadenceAgainstInput(playlistPath, forwardedVideoPTS); err != nil {
			t.Fatal(err)
		}
	}
	for _, segmentPath := range segmentFiles {
		if err := checkTransportStreamPackets(segmentPath); err != nil {
			t.Fatal(err)
		}
	}
}

func checkPlaybackPacingFromProbe(target string, expectedDuration time.Duration) error {
	probe, err := dumpFrames(target)
	if err != nil {
		return fmt.Errorf("dumpFrames failed on %s: %w", target, err)
	}

	videoPackets, audioPackets := splitPacketsByType(probe.Packets)
	if len(videoPackets) < 60 {
		return fmt.Errorf("expected at least 60 video packets in %s, got %d", target, len(videoPackets))
	}
	if len(audioPackets) < 120 {
		return fmt.Errorf("expected at least 120 audio packets in %s, got %d", target, len(audioPackets))
	}

	videoPTS := collectPacketTimes(videoPackets, func(packet Packet) flexString { return packet.PtsTime })
	audioPTS := collectPacketTimes(audioPackets, func(packet Packet) flexString { return packet.PtsTime })
	if len(videoPTS) < 2 {
		return fmt.Errorf("expected at least 2 video pts values in %s, got %d", target, len(videoPTS))
	}
	if len(audioPTS) < 2 {
		return fmt.Errorf("expected at least 2 audio pts values in %s, got %d", target, len(audioPTS))
	}

	videoSpan := videoPTS[len(videoPTS)-1] - videoPTS[0]
	audioSpan := audioPTS[len(audioPTS)-1] - audioPTS[0]
	expectedSeconds := expectedDuration.Seconds()
	if videoSpan < expectedSeconds-1.5 {
		return fmt.Errorf("video pts span too short in %s: got %.3fs want at least %.3fs", target, videoSpan, expectedSeconds-1.5)
	}
	if videoSpan > expectedSeconds+1.5 {
		return fmt.Errorf("video pts span too long in %s: got %.3fs want at most %.3fs", target, videoSpan, expectedSeconds+1.5)
	}
	if audioSpan < expectedSeconds-1.5 {
		return fmt.Errorf("audio pts span too short in %s: got %.3fs want at least %.3fs", target, audioSpan, expectedSeconds-1.5)
	}
	if audioSpan > expectedSeconds+1.5 {
		return fmt.Errorf("audio pts span too long in %s: got %.3fs want at most %.3fs", target, audioSpan, expectedSeconds+1.5)
	}

	videoAvgGap, videoMaxGap := packetGapStats(videoPTS)
	if videoAvgGap > 0.10 {
		return fmt.Errorf("video avg pts gap too large in %s: got %.3fs", target, videoAvgGap)
	}
	if videoMaxGap > 0.25 {
		return fmt.Errorf("video max pts gap too large in %s: got %.3fs", target, videoMaxGap)
	}

	audioAvgGap, audioMaxGap := packetGapStats(audioPTS)
	if audioAvgGap > 0.05 {
		return fmt.Errorf("audio avg pts gap too large in %s: got %.3fs", target, audioAvgGap)
	}
	if audioMaxGap > 0.20 {
		return fmt.Errorf("audio max pts gap too large in %s: got %.3fs", target, audioMaxGap)
	}

	endSkew := videoPTS[len(videoPTS)-1] - audioPTS[len(audioPTS)-1]
	if endSkew < 0 {
		endSkew = -endSkew
	}
	if endSkew > 0.5 {
		return fmt.Errorf("audio/video end skew too large in %s: got %.3fs", target, endSkew)
	}

	return nil
}

func checkSegmentBoundaryContinuity(segmentFiles []string) error {
	if len(segmentFiles) < 2 {
		return nil
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

		currentVideo, currentAudio := splitPacketsByType(currentProbe.Packets)
		nextVideo, nextAudio := splitPacketsByType(nextProbe.Packets)
		if err := checkBoundaryGap(currentVideo, nextVideo, "video", segmentFiles[i], segmentFiles[i+1], 0.20); err != nil {
			return err
		}
		if err := checkBoundaryGap(currentAudio, nextAudio, "audio", segmentFiles[i], segmentFiles[i+1], 0.10); err != nil {
			return err
		}
	}

	return nil
}

func checkBoundaryGap(current, next []Packet, codecType, currentPath, nextPath string, maxGap float64) error {
	currentPTS := collectPacketTimes(current, func(packet Packet) flexString { return packet.PtsTime })
	nextPTS := collectPacketTimes(next, func(packet Packet) flexString { return packet.PtsTime })
	if len(currentPTS) == 0 || len(nextPTS) == 0 {
		return fmt.Errorf("missing %s pts across segment boundary: %s -> %s", codecType, currentPath, nextPath)
	}

	gap := nextPTS[0] - currentPTS[len(currentPTS)-1]
	if gap < 0 {
		gap = -gap
	}
	if gap > maxGap {
		return fmt.Errorf("%s segment boundary gap too large: %s -> %s gap=%.3fs", codecType, currentPath, nextPath, gap)
	}

	return nil
}

func collectPacketTimes(packets []Packet, pick func(Packet) flexString) []float64 {
	values := make([]float64, 0, len(packets))
	for _, packet := range packets {
		if v, ok := parseFlexFloat(pick(packet)); ok {
			values = append(values, v)
		}
	}
	return values
}

func packetGapStats(values []float64) (avg float64, max float64) {
	if len(values) < 2 {
		return 0, 0
	}

	var sum float64
	for i := 1; i < len(values); i++ {
		gap := values[i] - values[i-1]
		sum += gap
		if gap > max {
			max = gap
		}
	}

	return sum / float64(len(values)-1), max
}

func checkVideoReplayAgainstInput(segmentFiles []string, playlistPath string, inputVideoHashes []string) error {
	if len(inputVideoHashes) == 0 {
		return fmt.Errorf("no input video hashes collected")
	}

	outputProbe, err := dumpFrames(playlistPath)
	if err != nil {
		return fmt.Errorf("dumpFrames failed on %s: %w", playlistPath, err)
	}
	outputVideoPackets, _ := splitPacketsByType(outputProbe.Packets)
	outputVideoHashes := packetHashes(outputVideoPackets)
	if len(outputVideoHashes) == 0 {
		return fmt.Errorf("no output video hashes collected from %s", playlistPath)
	}

	inputDupRatio, inputMaxRun := duplicateStats(inputVideoHashes)
	outputDupRatio, outputMaxRun := duplicateStats(outputVideoHashes)
	if outputMaxRun > inputMaxRun+2 && outputMaxRun > 3 {
		return fmt.Errorf("output consecutive duplicate video run too large: input max=%d output max=%d", inputMaxRun, outputMaxRun)
	}
	if outputDupRatio > inputDupRatio+0.10 && outputDupRatio > 0.12 {
		return fmt.Errorf("output duplicate video ratio too large: input=%.3f output=%.3f", inputDupRatio, outputDupRatio)
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
		overlap := boundaryReplayOverlap(packetHashes(currentVideoPackets), packetHashes(nextVideoPackets), 6)
		if overlap > 2 {
			return fmt.Errorf("video boundary replay overlap too large: %s -> %s overlap=%d", segmentFiles[i], segmentFiles[i+1], overlap)
		}
	}

	return nil
}

func packetHashes(packets []Packet) []string {
	hashes := make([]string, 0, len(packets))
	for _, packet := range packets {
		hash := packetDataHash(packet)
		if hash == "" {
			continue
		}
		hashes = append(hashes, hash)
	}
	return hashes
}

func packetDataHash(packet Packet) string {
	data := normalizeFFprobeHexDump(packet.Data)
	if len(data) == 0 {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func duplicateStats(hashes []string) (ratio float64, maxRun int) {
	if len(hashes) < 2 {
		if len(hashes) == 1 {
			return 0, 1
		}
		return 0, 0
	}

	maxRun = 1
	currentRun := 1
	duplicateEdges := 0
	for i := 1; i < len(hashes); i++ {
		if hashes[i] == hashes[i-1] {
			duplicateEdges++
			currentRun++
			if currentRun > maxRun {
				maxRun = currentRun
			}
			continue
		}
		currentRun = 1
	}

	return float64(duplicateEdges) / float64(len(hashes)-1), maxRun
}

func boundaryReplayOverlap(prev, next []string, limit int) int {
	if len(prev) == 0 || len(next) == 0 {
		return 0
	}
	if limit > len(prev) {
		limit = len(prev)
	}
	if limit > len(next) {
		limit = len(next)
	}

	best := 0
	for overlap := 1; overlap <= limit; overlap++ {
		match := true
		for i := 0; i < overlap; i++ {
			if prev[len(prev)-overlap+i] != next[i] {
				match = false
				break
			}
		}
		if match {
			best = overlap
		}
	}

	return best
}

func checkOutputVideoCadenceAgainstInput(playlistPath string, inputPTS []float64) error {
	if len(inputPTS) < 10 {
		return fmt.Errorf("need at least 10 input video pts samples, got %d", len(inputPTS))
	}

	probe, err := dumpFrames(playlistPath)
	if err != nil {
		return fmt.Errorf("dumpFrames failed on %s: %w", playlistPath, err)
	}
	outputVideoPackets, _ := splitPacketsByType(probe.Packets)
	outputPTS := collectPacketTimes(outputVideoPackets, func(packet Packet) flexString { return packet.PtsTime })
	if len(outputPTS) < 10 {
		return fmt.Errorf("need at least 10 output video pts samples, got %d", len(outputPTS))
	}

	minCount := int(float64(len(inputPTS)) * 0.90)
	maxCount := int(float64(len(inputPTS))*1.10) + 2
	if len(outputPTS) < minCount || len(outputPTS) > maxCount {
		return fmt.Errorf("output video packet count drift too large: input=%d output=%d", len(inputPTS), len(outputPTS))
	}

	inputAvgGap, inputMaxGap := packetGapStats(inputPTS)
	outputAvgGap, outputMaxGap := packetGapStats(outputPTS)
	inputP95 := percentileGap(inputPTS, 0.95)
	outputP95 := percentileGap(outputPTS, 0.95)

	if outputAvgGap > inputAvgGap+0.012 {
		return fmt.Errorf("output video avg gap too large vs input: input=%.3fs output=%.3fs", inputAvgGap, outputAvgGap)
	}
	if outputP95 > inputP95+0.020 {
		return fmt.Errorf("output video p95 gap too large vs input: input=%.3fs output=%.3fs", inputP95, outputP95)
	}

	maxAllowedGap := inputMaxGap + 0.050
	if maxAllowedGap < inputAvgGap*2.5 {
		maxAllowedGap = inputAvgGap * 2.5
	}
	if outputMaxGap > maxAllowedGap {
		return fmt.Errorf("output video max gap too large vs input: input=%.3fs output=%.3fs allowed=%.3fs", inputMaxGap, outputMaxGap, maxAllowedGap)
	}

	largeGapLimit := inputP95 + 0.020
	largeGapCount := countGapsAbove(outputPTS, largeGapLimit)
	if largeGapCount > 1 {
		return fmt.Errorf("output video has too many large gaps vs input baseline: limit=%.3fs count=%d", largeGapLimit, largeGapCount)
	}

	return nil
}

func percentileGap(values []float64, pct float64) float64 {
	if len(values) < 2 {
		return 0
	}

	gaps := make([]float64, 0, len(values)-1)
	for i := 1; i < len(values); i++ {
		gaps = append(gaps, values[i]-values[i-1])
	}
	sort.Float64s(gaps)

	if pct <= 0 {
		return gaps[0]
	}
	if pct >= 1 {
		return gaps[len(gaps)-1]
	}

	index := int(float64(len(gaps)-1) * pct)
	return gaps[index]
}

func countGapsAbove(values []float64, limit float64) int {
	if len(values) < 2 {
		return 0
	}

	count := 0
	for i := 1; i < len(values); i++ {
		if values[i]-values[i-1] > limit {
			count++
		}
	}
	return count
}
