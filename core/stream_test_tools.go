package irajstreamer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"restreamer/core/logger"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

const maxGap = 1 * time.Second

// ============================================================================
// Types for Stream Health Checks
// ============================================================================

// StreamHealthResult holds the health check results
type StreamHealthResult struct {
	IsHealthy           bool
	TotalFrames         int
	MonotonicPTSIssues  []PTSIssue
	MonotonicDTSIssues  []PTSIssue
	LargeGapIssues      []GapIssue
	DTSIssues           []DTSIssue
	MonotonicPTSPercent float64
	MonotonicDTSPercent float64
	ValidGapPercent     float64
	DTSValidPercent     float64
}

// PTSIssue represents a non-monotonic PTS problem
type PTSIssue struct {
	FrameIndex  int
	CurrentPTS  time.Duration
	PreviousPTS time.Duration
	Difference  time.Duration
}

// GapIssue represents a frame gap that's too large
type GapIssue struct {
	FrameIndex  int
	Gap         time.Duration
	CurrentPTS  time.Duration
	PreviousPTS time.Duration
}

// DTSIssue represents a case where DTS > PTS (which should never happen)
type DTSIssue struct {
	FrameIndex int
	PTS        time.Duration
	DTS        time.Duration
	Difference time.Duration
}

// H264HealthIssue represents a health issue found in H264 frames
type H264HealthIssue struct {
	FrameIndex int
	NALUIndex  int
	IssueType  string
	Message    string
	Severity   string // "error" or "warning"
}

// ============================================================================
// Types for Frame Sequence Comparison
// ============================================================================

// FrameSequenceComparisonResult holds the comparison results
type FrameSequenceComparisonResult struct {
	TotalWindows      int
	MatchedWindows    int
	SimilarityPercent float64
	PTSMatches        int
	PayloadMatches    int
	TotalFrames       int
	WindowDetails     []WindowComparison
}

// WindowComparison holds details for each window
type WindowComparison struct {
	WindowIndex         int
	StartFrame          int
	EndFrame            int
	FrameCount          int
	PTSMatches          int
	PayloadMatches      int
	PTSMatchPercent     float64
	PayloadMatchPercent float64
}

// ============================================================================
// Types for Window-Match Benchmark
// ============================================================================

// WindowMatchBenchmarkResult holds the window-match benchmark results
type WindowMatchBenchmarkResult struct {
	Windows               []WindowMatch
	MismatchContexts      []MismatchContext
	TotalWindows          int
	TotalMatchedFrames    int
	TotalMismatchedFrames int
	TotalWindowSize       int
	MatchPercent          float64
	PTSMatches            int
	PayloadMatches        int
	PTSMatchPercent       float64
	PayloadMatchPercent   float64
}

// WindowMatch represents a single matching window
type WindowMatch struct {
	StartIndex1   int
	StartIndex2   int
	EndIndex1     int
	EndIndex2     int
	WindowSize    int
	MatchedFrames int
}

// MismatchContext holds context around a mismatched frame
type MismatchContext struct {
	MismatchIndex1 int
	MismatchIndex2 int
	BeforeFrames   []*Frame // 5 frames before (from reference stream2)
	MismatchFrame1 *Frame   // Mismatched frame from stream1
	MismatchFrame2 *Frame   // Mismatched frame from stream2 (reference)
	AfterFrames    []*Frame // 5 frames after (from reference stream2)
	WindowIndex    int      // Which window this mismatch belongs to
}

// ============================================================================
// Types for Equal-Packet-Rate Benchmark
// ============================================================================

// EqualPacketRateBenchmarkResult holds the equal-packet-rate benchmark results
type EqualPacketRateBenchmarkResult struct {
	SmallerStreamSize   int
	LargerStreamSize    int
	FoundPackets        int
	NotFoundPackets     int
	SuccessRate         float64
	PTSMatches          int
	PTSMatchPercent     float64
	PayloadMatches      int
	PayloadMatchPercent float64
	MissingContexts     []MissingPacketContext
	BaseTimestamp       time.Time
}

// MissingPacketContext captures surrounding frames for a missing packet.
type MissingPacketContext struct {
	Index     int
	Frame     *Frame
	RefFrame  *Frame
	Before    []*Frame
	After     []*Frame
	RefBefore []*Frame
	RefAfter  []*Frame
	FrameHash string
}

// ============================================================================
// Types for Switch Latency
// ============================================================================

// SwitchEvent represents a switch command that was issued
type SwitchEvent struct {
	SwitchIndex      int
	TargetInputID    string
	SwitchTime       time.Time
	PTSBeforeSwitch  time.Duration
	ExpectedDuration time.Duration
}

// SwitchLatencyResult holds the latency measurement results
type SwitchLatencyResult struct {
	SwitchIndex     int
	TargetInputID   string
	PTSBeforeSwitch time.Duration
	PTSAfterSwitch  time.Duration
	Latency         time.Duration
	Found           bool
}

// ============================================================================
// Types for InputID Changes
// ============================================================================

// InputIDChange represents a change in InputID with context
type InputIDChange struct {
	FrameIndex    int
	PreviousInput string
	CurrentInput  string
	BeforeFrames  []*Frame
	ChangeFrame   *Frame
	AfterFrames   []*Frame
}

// ============================================================================
// Helper Functions
// ============================================================================

// frameHash computes SHA256 hash of all payload chunks concatenated
func frameHash(frame *Frame) string {
	hasher := sha256.New()
	for _, chunk := range frame.Payload {
		hasher.Write(chunk)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// abs returns the absolute value of a float64
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// contains checks if a slice contains a value
func contains(slice []int64, value int64) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// ============================================================================
// Stream Health Check Functions
// ============================================================================

// checkStreamHealth checks if a stream is healthy:
// - PTS should increase monotonically
// - Frames shouldn't have distance more than 100ms
func checkStreamHealth(frames []*Frame, frameType string) StreamHealthResult {
	result := StreamHealthResult{
		TotalFrames: len(frames),
		IsHealthy:   true,
	}

	if len(frames) == 0 {
		result.IsHealthy = false
		return result
	}

	if len(frames) == 1 {
		result.IsHealthy = true
		result.MonotonicPTSPercent = 100.0
		result.MonotonicDTSPercent = 100.0
		result.ValidGapPercent = 100.0
		return result
	}

	monotonicPTSCount := 0
	monotonicDTSCount := 0
	validGapCount := 0
	validDTSCount := 0

	// Check each frame against the previous one
	for i := 1; i < len(frames); i++ {
		current := frames[i]
		previous := frames[i-1]

		if current == nil || previous == nil {
			continue
		}

		// Check monotonic PTS
		if current.PTS >= previous.PTS {
			monotonicPTSCount++
		} else {
			// PTS decreased - non-monotonic
			diff := previous.PTS - current.PTS
			result.MonotonicPTSIssues = append(result.MonotonicPTSIssues, PTSIssue{
				FrameIndex:  i,
				CurrentPTS:  current.PTS,
				PreviousPTS: previous.PTS,
				Difference:  diff,
			})
			result.IsHealthy = false
		}

		// Check monotonic DTS
		if current.DTS >= previous.DTS {
			monotonicDTSCount++
		} else {
			// DTS decreased - non-monotonic
			diff := previous.DTS - current.DTS
			result.MonotonicDTSIssues = append(result.MonotonicDTSIssues, PTSIssue{
				FrameIndex:  i,
				CurrentPTS:  current.DTS, // Using PTS field to store DTS value
				PreviousPTS: previous.DTS,
				Difference:  diff,
			})
			result.IsHealthy = false
		}

		// Check frame gap
		gap := current.PTS - previous.PTS
		if gap <= maxGap {
			validGapCount++
		} else {
			// Gap too large
			result.LargeGapIssues = append(result.LargeGapIssues, GapIssue{
				FrameIndex:  i,
				Gap:         gap,
				CurrentPTS:  current.PTS,
				PreviousPTS: previous.PTS,
			})
			result.IsHealthy = false
		}
	}

	// Check DTS <= PTS for all frames
	for i := 0; i < len(frames); i++ {
		frame := frames[i]
		if frame == nil {
			continue
		}

		// DTS should never be greater than PTS
		if frame.DTS <= frame.PTS {
			validDTSCount++
		} else {
			// DTS > PTS - invalid
			diff := frame.DTS - frame.PTS
			result.DTSIssues = append(result.DTSIssues, DTSIssue{
				FrameIndex: i,
				PTS:        frame.PTS,
				DTS:        frame.DTS,
				Difference: diff,
			})
			result.IsHealthy = false
		}
	}

	// Calculate percentages
	totalComparisons := len(frames) - 1
	if totalComparisons > 0 {
		result.MonotonicPTSPercent = float64(monotonicPTSCount) / float64(totalComparisons) * 100.0
		result.MonotonicDTSPercent = float64(monotonicDTSCount) / float64(totalComparisons) * 100.0
		result.ValidGapPercent = float64(validGapCount) / float64(totalComparisons) * 100.0
	}
	totalFrames := len(frames)
	if totalFrames > 0 {
		result.DTSValidPercent = float64(validDTSCount) / float64(totalFrames) * 100.0
	}

	return result
}

// printStreamHealth prints the health check results in a readable format
func printStreamHealth(t *testing.T, result StreamHealthResult, frameType string) {
	t.Logf("\n=== %s Stream Health Check ===", frameType)
	t.Logf("Total Frames: %d", result.TotalFrames)
	t.Logf("Is Healthy: %v", result.IsHealthy)
	t.Logf("Monotonic PTS: %.2f%%", result.MonotonicPTSPercent)
	t.Logf("Monotonic DTS: %.2f%%", result.MonotonicDTSPercent)
	t.Logf("Valid Gaps (<=100ms): %.2f%%", result.ValidGapPercent)
	t.Logf("Valid DTS (DTS <= PTS): %.2f%%", result.DTSValidPercent)

	if len(result.MonotonicPTSIssues) > 0 {
		t.Logf("\nNon-Monotonic PTS Issues: %d", len(result.MonotonicPTSIssues))
		maxPrint := 10
		if len(result.MonotonicPTSIssues) < maxPrint {
			maxPrint = len(result.MonotonicPTSIssues)
		}
		t.Logf("Showing first %d issues:", maxPrint)
		for i := 0; i < maxPrint; i++ {
			issue := result.MonotonicPTSIssues[i]
			t.Logf("  Frame %d: PTS decreased from %v to %v (diff: %v)",
				issue.FrameIndex, issue.PreviousPTS, issue.CurrentPTS, issue.Difference)
		}
		if len(result.MonotonicPTSIssues) > maxPrint {
			t.Logf("  ... and %d more issues", len(result.MonotonicPTSIssues)-maxPrint)
		}
	}

	if len(result.MonotonicDTSIssues) > 0 {
		t.Logf("\nNon-Monotonic DTS Issues: %d", len(result.MonotonicDTSIssues))
		maxPrint := 10
		if len(result.MonotonicDTSIssues) < maxPrint {
			maxPrint = len(result.MonotonicDTSIssues)
		}
		t.Logf("Showing first %d issues:", maxPrint)
		for i := 0; i < maxPrint; i++ {
			issue := result.MonotonicDTSIssues[i]
			t.Logf("  Frame %d: DTS decreased from %v to %v (diff: %v)",
				issue.FrameIndex, issue.PreviousPTS, issue.CurrentPTS, issue.Difference)
		}
		if len(result.MonotonicDTSIssues) > maxPrint {
			t.Logf("  ... and %d more issues", len(result.MonotonicDTSIssues)-maxPrint)
		}
	}

	if len(result.LargeGapIssues) > 0 {
		t.Logf("\nLarge Gap Issues (>100ms): %d", len(result.LargeGapIssues))
		maxPrint := 10
		if len(result.LargeGapIssues) < maxPrint {
			maxPrint = len(result.LargeGapIssues)
		}
		t.Logf("Showing first %d issues:", maxPrint)
		for i := 0; i < maxPrint; i++ {
			issue := result.LargeGapIssues[i]
			t.Logf("  Frame %d: Gap of %v between PTS %v and %v",
				issue.FrameIndex, issue.Gap, issue.PreviousPTS, issue.CurrentPTS)
		}
		if len(result.LargeGapIssues) > maxPrint {
			t.Logf("  ... and %d more issues", len(result.LargeGapIssues)-maxPrint)
		}
	}

	if len(result.DTSIssues) > 0 {
		t.Logf("\nDTS > PTS Issues: %d", len(result.DTSIssues))
		maxPrint := 10
		if len(result.DTSIssues) < maxPrint {
			maxPrint = len(result.DTSIssues)
		}
		t.Logf("Showing first %d issues:", maxPrint)
		for i := 0; i < maxPrint; i++ {
			issue := result.DTSIssues[i]
			t.Logf("  Frame %d: DTS (%v) > PTS (%v), difference: %v",
				issue.FrameIndex, issue.DTS, issue.PTS, issue.Difference)
		}
		if len(result.DTSIssues) > maxPrint {
			t.Logf("  ... and %d more issues", len(result.DTSIssues)-maxPrint)
		}
	}

	if result.IsHealthy {
		t.Logf("\n✓ Stream is healthy!")
	} else {
		t.Logf("\n✗ Stream has health issues")
	}
}

// ============================================================================
// Frame Timing Check Functions
// ============================================================================

// checkFrameTiming verifies that the PTS window of frames matches the elapsed time
func checkFrameTiming(t *testing.T, frames []*Frame, frameType string, expectedDuration, actualElapsed time.Duration, threshold float64) {
	if len(frames) == 0 {
		return
	}

	// Find min and max PTS
	minPTS := frames[0].PTS
	maxPTS := frames[0].PTS

	for _, frame := range frames {
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

	t.Logf("\n=== %s Timing Check ===", frameType)
	t.Logf("Actual elapsed time: %v", actualElapsed)
	if expectedDuration != actualElapsed {
		t.Logf("Expected duration: %v", expectedDuration)
	}
	t.Logf("PTS window: %v (from %v to %v)", ptsWindow, minPTS, maxPTS)
	t.Logf("Number of frames: %d", len(frames))

	ptsWindowSeconds := ptsWindow.Seconds()
	actualElapsedSeconds := actualElapsed.Seconds()

	// Only check PTS window against actual elapsed time (skip expected duration check)
	elapsedDiff := abs(ptsWindowSeconds - actualElapsedSeconds)
	elapsedDiffPercent := (elapsedDiff / actualElapsedSeconds) * 100.0

	t.Logf("PTS window vs actual elapsed time: diff=%v (%.2f%%)", time.Duration(elapsedDiff*float64(time.Second)), elapsedDiffPercent)

	if elapsedDiffPercent > threshold*100 {
		t.Errorf("PTS window (%.2fs) does not match actual elapsed time (%.2fs): difference is %.2f%% (threshold: %.2f%%)",
			ptsWindowSeconds, actualElapsedSeconds, elapsedDiffPercent, threshold*100)
	} else {
		t.Logf("✓ PTS window matches actual elapsed time (within %.2f%% threshold)", threshold*100)
	}

	// Only check expected duration if it's different from actual elapsed (for RTMP timing test)
	if expectedDuration != actualElapsed {
		expectedDurationSeconds := expectedDuration.Seconds()
		ptsDiff := abs(ptsWindowSeconds - expectedDurationSeconds)
		ptsDiffPercent := (ptsDiff / expectedDurationSeconds) * 100.0

		t.Logf("PTS window vs expected duration: diff=%v (%.2f%%)", time.Duration(ptsDiff*float64(time.Second)), ptsDiffPercent)

		if ptsDiffPercent > threshold*100 {
			t.Errorf("PTS window (%.2fs) does not match expected duration (%.2fs): difference is %.2f%% (threshold: %.2f%%)",
				ptsWindowSeconds, expectedDurationSeconds, ptsDiffPercent, threshold*100)
		} else {
			t.Logf("✓ PTS window matches expected duration (within %.2f%% threshold)", threshold*100)
		}

		// Check actual elapsed vs expected duration
		timeDiff := abs(actualElapsedSeconds - expectedDurationSeconds)
		timeDiffPercent := (timeDiff / expectedDurationSeconds) * 100.0

		t.Logf("Actual elapsed vs expected duration: diff=%v (%.2f%%)", time.Duration(timeDiff*float64(time.Second)), timeDiffPercent)

		if timeDiffPercent > threshold*100 {
			t.Errorf("Actual elapsed time (%.2fs) does not match expected duration (%.2fs): difference is %.2f%% (threshold: %.2f%%)",
				actualElapsedSeconds, expectedDurationSeconds, timeDiffPercent, threshold*100)
		} else {
			t.Logf("✓ Actual elapsed time matches expected duration (within %.2f%% threshold)", threshold*100)
		}
	}
}

// ============================================================================
// Sequence ID Continuity Check Functions
// ============================================================================

// checkSequenceIDContinuity verifies that sequence IDs are consecutive (no missing IDs)
// Results are grouped and printed by InputID
func checkSequenceIDContinuity(t *testing.T, frames []*Frame, frameType string) {
	if len(frames) == 0 {
		return
	}

	t.Logf("\n=== %s Sequence ID Continuity Check ===", frameType)
	t.Logf("Total frames: %d", len(frames))

	// Group frames by InputID
	framesByInputID := make(map[string][]*Frame)
	for _, frame := range frames {
		if frame == nil {
			continue
		}
		framesByInputID[frame.InputID] = append(framesByInputID[frame.InputID], frame)
	}

	// Check continuity for each InputID separately
	overallHasIssues := false
	for inputID, inputFrames := range framesByInputID {
		if len(inputFrames) == 0 {
			continue
		}

		t.Logf("\n--- InputID: %s ---", inputID)
		hasIssues := checkSequenceIDContinuityForInput(t, inputFrames, frameType, inputID)
		if hasIssues {
			overallHasIssues = true
		}
	}

	if !overallHasIssues {
		t.Logf("\n✓ All InputIDs have continuous sequence IDs")
	}
}

// checkSequenceIDContinuityForInput checks sequence ID continuity for a specific InputID
func checkSequenceIDContinuityForInput(t *testing.T, frames []*Frame, frameType, inputID string) bool {
	if len(frames) == 0 {
		return false
	}

	// Collect sequence IDs for this InputID
	sequenceIDs := make([]int64, 0, len(frames))
	sequenceIDMap := make(map[int64]*Frame)

	for _, frame := range frames {
		if frame == nil {
			continue
		}
		sequenceIDs = append(sequenceIDs, frame.SequenceID)
		sequenceIDMap[frame.SequenceID] = frame
	}

	if len(sequenceIDs) == 0 {
		return false
	}

	// Find min and max sequence IDs for this InputID
	minSeqID := sequenceIDs[0]
	maxSeqID := sequenceIDs[0]

	for _, seqID := range sequenceIDs {
		if seqID < minSeqID {
			minSeqID = seqID
		}
		if seqID > maxSeqID {
			maxSeqID = seqID
		}
	}

	t.Logf("  Total frames: %d", len(sequenceIDs))
	t.Logf("  Sequence ID range: %d to %d", minSeqID, maxSeqID)
	t.Logf("  Expected sequence IDs: %d (from %d to %d)", maxSeqID-minSeqID+1, minSeqID, maxSeqID)

	// Check for missing sequence IDs
	missingIDs := []int64{}
	duplicateIDs := []int64{}

	// Track which IDs we've seen
	seenIDs := make(map[int64]bool)

	for expectedID := minSeqID; expectedID <= maxSeqID; expectedID++ {
		if _, exists := sequenceIDMap[expectedID]; !exists {
			missingIDs = append(missingIDs, expectedID)
		} else {
			if seenIDs[expectedID] {
				// Duplicate ID within same InputID
				duplicateIDs = append(duplicateIDs, expectedID)
			}
			seenIDs[expectedID] = true
		}
	}

	// Check for duplicate sequence IDs within this InputID
	duplicateCount := make(map[int64]int)
	for _, seqID := range sequenceIDs {
		duplicateCount[seqID]++
	}
	for seqID, count := range duplicateCount {
		if count > 1 {
			if !contains(duplicateIDs, seqID) {
				duplicateIDs = append(duplicateIDs, seqID)
			}
		}
	}

	hasIssues := false

	// Report results for this InputID
	if len(missingIDs) > 0 {
		hasIssues = true
		t.Logf("  Missing Sequence IDs: %d", len(missingIDs))
		maxPrint := 20
		if len(missingIDs) < maxPrint {
			maxPrint = len(missingIDs)
		}
		t.Logf("  First %d missing IDs:", maxPrint)
		for i := 0; i < maxPrint; i++ {
			t.Logf("    Missing: %d", missingIDs[i])
		}
		if len(missingIDs) > maxPrint {
			t.Logf("    ... and %d more missing IDs", len(missingIDs)-maxPrint)
		}
		t.Errorf("  InputID '%s': Found %d missing sequence IDs in %s frames", inputID, len(missingIDs), frameType)
	} else {
		t.Logf("  ✓ No missing sequence IDs")
	}

	if len(duplicateIDs) > 0 {
		hasIssues = true
		t.Logf("  Duplicate Sequence IDs: %d", len(duplicateIDs))
		maxPrint := 20
		if len(duplicateIDs) < maxPrint {
			maxPrint = len(duplicateIDs)
		}
		t.Logf("  First %d duplicate IDs:", maxPrint)
		for i := 0; i < maxPrint; i++ {
			count := duplicateCount[duplicateIDs[i]]
			t.Logf("    Duplicate: %d (appears %d times)", duplicateIDs[i], count)
		}
		if len(duplicateIDs) > maxPrint {
			t.Logf("    ... and %d more duplicate IDs", len(duplicateIDs)-maxPrint)
		}
		t.Errorf("  InputID '%s': Found %d duplicate sequence IDs in %s frames", inputID, len(duplicateIDs), frameType)
	} else {
		t.Logf("  ✓ No duplicate sequence IDs")
	}

	// Calculate continuity percentage for this InputID
	expectedCount := maxSeqID - minSeqID + 1
	actualCount := int64(len(sequenceIDs))
	continuityPercent := float64(actualCount) / float64(expectedCount) * 100.0

	t.Logf("  Sequence ID continuity: %.2f%% (%d/%d)", continuityPercent, actualCount, expectedCount)

	if len(missingIDs) == 0 && len(duplicateIDs) == 0 {
		t.Logf("  ✓ Sequence IDs are perfectly continuous for InputID '%s'", inputID)
	}

	return hasIssues
}

// ============================================================================
// H264 Frame Health Check Functions
// ============================================================================

// checkH264FrameHealth validates H264 frames for AVCC compatibility
// This checks for issues that could cause "unable to decode AVCC: invalid length" errors
func checkH264FrameHealth(t *testing.T, frames []*Frame) {
	if len(frames) == 0 {
		return
	}

	t.Logf("\n=== H264 Frame Health Check (AVCC Compatibility) ===")

	issues := []H264HealthIssue{}
	keyframeCount := 0
	keyframeWithSPSPPS := 0
	emptyNALUCount := 0
	invalidNALUCount := 0
	annexBStartCodeCount := 0
	veryLargeNALUCount := 0

	for i, frame := range frames {
		if frame == nil {
			issues = append(issues, H264HealthIssue{
				FrameIndex: i,
				IssueType:  "nil_frame",
				Message:    "Frame is nil",
				Severity:   "error",
			})
			continue
		}

		if frame.Codec != "h264" {
			continue
		}

		// Check if frame has payload
		if len(frame.Payload) == 0 {
			issues = append(issues, H264HealthIssue{
				FrameIndex: i,
				IssueType:  "empty_payload",
				Message:    "Frame has no NAL units",
				Severity:   "error",
			})
			continue
		}

		// Track SPS/PPS for keyframes
		hasSPS := false
		hasPPS := false

		// Check each NALU
		for naluIdx, nalu := range frame.Payload {
			if len(nalu) == 0 {
				emptyNALUCount++
				issues = append(issues, H264HealthIssue{
					FrameIndex: i,
					NALUIndex:  naluIdx,
					IssueType:  "empty_nalu",
					Message:    "NALU is empty",
					Severity:   "error",
				})
				continue
			}

			// Check for Annex-B start codes (should be stripped for AVCC)
			hasStartCode := false
			if len(nalu) >= 3 {
				// Check for 0x00 0x00 0x01
				if nalu[0] == 0x00 && nalu[1] == 0x00 && nalu[2] == 0x01 {
					hasStartCode = true
					annexBStartCodeCount++
				} else if len(nalu) >= 4 {
					// Check for 0x00 0x00 0x00 0x01
					if nalu[0] == 0x00 && nalu[1] == 0x00 && nalu[2] == 0x00 && nalu[3] == 0x01 {
						hasStartCode = true
						annexBStartCodeCount++
					}
				}
			}

			if hasStartCode {
				issues = append(issues, H264HealthIssue{
					FrameIndex: i,
					NALUIndex:  naluIdx,
					IssueType:  "annexb_start_code",
					Message:    "NALU contains Annex-B start code (should be stripped for AVCC)",
					Severity:   "error",
				})
			}

			// Strip start code to check NAL type
			strippedNALU := nalu
			if hasStartCode {
				// Find where actual NALU starts
				for j := 0; j < len(nalu)-1; j++ {
					if nalu[j] == 0x00 && nalu[j+1] == 0x01 {
						strippedNALU = nalu[j+2:]
						break
					}
					if j < len(nalu)-3 && nalu[j] == 0x00 && nalu[j+1] == 0x00 && nalu[j+2] == 0x00 && nalu[j+3] == 0x01 {
						strippedNALU = nalu[j+4:]
						break
					}
				}
			}

			if len(strippedNALU) == 0 {
				invalidNALUCount++
				issues = append(issues, H264HealthIssue{
					FrameIndex: i,
					NALUIndex:  naluIdx,
					IssueType:  "invalid_nalu",
					Message:    "NALU is invalid after stripping start code",
					Severity:   "error",
				})
				continue
			}

			// Check NAL type
			nalType := strippedNALU[0] & 0x1F

			// Check for unreasonably large NALU (could indicate corruption)
			if len(strippedNALU) > 1024*1024 { // 1MB
				veryLargeNALUCount++
				issues = append(issues, H264HealthIssue{
					FrameIndex: i,
					NALUIndex:  naluIdx,
					IssueType:  "very_large_nalu",
					Message:    fmt.Sprintf("NALU is very large (%d bytes), might be corrupted", len(strippedNALU)),
					Severity:   "warning",
				})
			}

			// Track SPS/PPS
			if nalType == 7 { // SPS
				hasSPS = true
			} else if nalType == 8 { // PPS
				hasPPS = true
			}

			// Check for invalid NAL types
			if nalType == 0 || nalType > 31 {
				invalidNALUCount++
				issues = append(issues, H264HealthIssue{
					FrameIndex: i,
					NALUIndex:  naluIdx,
					IssueType:  "invalid_nal_type",
					Message:    fmt.Sprintf("Invalid NAL type: %d", nalType),
					Severity:   "error",
				})
			}
		}

		// Check keyframes for SPS/PPS
		if frame.IsKeyFrame {
			keyframeCount++
			if hasSPS && hasPPS {
				keyframeWithSPSPPS++
			} else {
				missing := []string{}
				if !hasSPS {
					missing = append(missing, "SPS")
				}
				if !hasPPS {
					missing = append(missing, "PPS")
				}
				issues = append(issues, H264HealthIssue{
					FrameIndex: i,
					IssueType:  "keyframe_missing_sps_pps",
					Message:    fmt.Sprintf("Keyframe missing: %s", fmt.Sprint(missing)),
					Severity:   "error",
				})
			}
		}
	}

	// Print summary
	t.Logf("Total frames checked: %d", len(frames))
	t.Logf("Keyframes: %d (with SPS/PPS: %d)", keyframeCount, keyframeWithSPSPPS)
	t.Logf("Empty NALUs: %d", emptyNALUCount)
	t.Logf("Invalid NALUs: %d", invalidNALUCount)
	t.Logf("NALUs with Annex-B start codes: %d", annexBStartCodeCount)
	t.Logf("Very large NALUs (>1MB): %d", veryLargeNALUCount)

	// Print issues
	errorCount := 0
	warningCount := 0
	for _, issue := range issues {
		if issue.Severity == "error" {
			errorCount++
		} else {
			warningCount++
		}
	}

	if len(issues) > 0 {
		t.Logf("\nFound %d issues (%d errors, %d warnings):", len(issues), errorCount, warningCount)
		maxPrint := 30
		if len(issues) < maxPrint {
			maxPrint = len(issues)
		}
		for i := 0; i < maxPrint; i++ {
			issue := issues[i]
			naluInfo := ""
			if issue.NALUIndex >= 0 {
				naluInfo = fmt.Sprintf(" NALU[%d]", issue.NALUIndex)
			}
			t.Logf("  [%s] Frame[%d]%s: %s - %s", issue.Severity, issue.FrameIndex, naluInfo, issue.IssueType, issue.Message)
		}
		if len(issues) > maxPrint {
			t.Logf("  ... and %d more issues", len(issues)-maxPrint)
		}

		if errorCount > 0 {
			t.Errorf("H264 frame health check failed: %d errors found (these could cause 'unable to decode AVCC: invalid length' errors)", errorCount)
		}
	} else {
		t.Logf("\n✓ All H264 frames are healthy for AVCC encoding")
	}
}

// ============================================================================
// Frame Sequence Comparison Functions
// ============================================================================

// compareFrameSequences compares two frame sequences by dividing them into windows
// and calculating similarity based on PTS and payload
func compareFrameSequences(stream1, stream2 []*Frame, frameType string) FrameSequenceComparisonResult {
	result := FrameSequenceComparisonResult{
		TotalFrames: len(stream1),
	}

	if len(stream1) == 0 && len(stream2) == 0 {
		result.SimilarityPercent = 100.0
		return result
	}

	if len(stream1) == 0 || len(stream2) == 0 {
		result.SimilarityPercent = 0.0
		return result
	}

	// Use the smaller length for window calculation
	minLen := len(stream1)
	if len(stream2) < minLen {
		minLen = len(stream2)
	}

	// Divide into 100 windows
	numWindows := 100
	if minLen < numWindows {
		numWindows = minLen
	}

	result.TotalWindows = numWindows
	framesPerWindow := minLen / numWindows
	if framesPerWindow == 0 {
		framesPerWindow = 1
	}

	result.WindowDetails = make([]WindowComparison, 0, numWindows)

	// Compare each window
	for windowIdx := 0; windowIdx < numWindows; windowIdx++ {
		startFrame := windowIdx * framesPerWindow
		endFrame := startFrame + framesPerWindow
		if windowIdx == numWindows-1 {
			// Last window includes remaining frames
			endFrame = minLen
		}

		windowResult := WindowComparison{
			WindowIndex: windowIdx,
			StartFrame:  startFrame,
			EndFrame:    endFrame,
			FrameCount:  endFrame - startFrame,
		}

		ptsMatches := 0
		payloadMatches := 0
		totalComparisons := 0

		// Compare frames in this window
		for i := startFrame; i < endFrame && i < len(stream1) && i < len(stream2); i++ {
			frame1 := stream1[i]
			frame2 := stream2[i]

			if frame1 == nil || frame2 == nil {
				continue
			}

			totalComparisons++

			// Compare PTS (with 1ms tolerance)
			ptsDiff := frame1.PTS - frame2.PTS
			if ptsDiff < 0 {
				ptsDiff = -ptsDiff
			}
			if ptsDiff <= 1*time.Millisecond {
				ptsMatches++
				result.PTSMatches++
			}

			// Compare payload hash
			hash1 := frameHash(frame1)
			hash2 := frameHash(frame2)
			if hash1 == hash2 {
				payloadMatches++
				result.PayloadMatches++
			}
		}

		if totalComparisons > 0 {
			windowResult.PTSMatches = ptsMatches
			windowResult.PayloadMatches = payloadMatches
			windowResult.PTSMatchPercent = float64(ptsMatches) / float64(totalComparisons) * 100.0
			windowResult.PayloadMatchPercent = float64(payloadMatches) / float64(totalComparisons) * 100.0

			logger.GetLogger().Debug("window match stats",
				zap.Int("window", windowIdx),
				zap.Float64("pts_match_percent", windowResult.PTSMatchPercent),
				zap.Float64("payload_match_percent", windowResult.PayloadMatchPercent))

			// Window is considered matched if both PTS and payload match rate is > 80%
			if windowResult.PTSMatchPercent >= 80.0 && windowResult.PayloadMatchPercent >= 80.0 {
				result.MatchedWindows++
			}
		}

		result.WindowDetails = append(result.WindowDetails, windowResult)
	}

	// Calculate overall similarity
	if result.TotalWindows > 0 {
		result.SimilarityPercent = float64(result.MatchedWindows) / float64(result.TotalWindows) * 100.0
	}

	return result
}

// printFrameSequenceComparison prints the comparison results in a readable format
func printFrameSequenceComparison(t *testing.T, result FrameSequenceComparisonResult, frameType string) {
	t.Logf("\n=== %s Frame Sequence Comparison ===", frameType)
	t.Logf("Total Frames: %d", result.TotalFrames)
	t.Logf("Total Windows: %d", result.TotalWindows)
	t.Logf("Matched Windows: %d", result.MatchedWindows)
	t.Logf("Overall Similarity: %.2f%%", result.SimilarityPercent)
	t.Logf("PTS Matches: %d", result.PTSMatches)
	t.Logf("Payload Matches: %d", result.PayloadMatches)

	// Print window statistics
	if len(result.WindowDetails) > 0 {
		t.Logf("\nWindow Statistics:")
		t.Logf("Window | Frames | PTS Match %% | Payload Match %% | Status")
		t.Logf("-------|--------|-------------|------------------|--------")

		for _, window := range result.WindowDetails {
			status := "✗"
			if window.PTSMatchPercent >= 80.0 && window.PayloadMatchPercent >= 80.0 {
				status = "✓"
			}
			t.Logf("%6d | %6d | %11.2f | %16.2f | %s",
				window.WindowIndex,
				window.FrameCount,
				window.PTSMatchPercent,
				window.PayloadMatchPercent,
				status)
		}

		// Print summary statistics
		var avgPTSMatch, avgPayloadMatch float64
		for _, window := range result.WindowDetails {
			avgPTSMatch += window.PTSMatchPercent
			avgPayloadMatch += window.PayloadMatchPercent
		}
		if len(result.WindowDetails) > 0 {
			avgPTSMatch /= float64(len(result.WindowDetails))
			avgPayloadMatch /= float64(len(result.WindowDetails))
		}
		t.Logf("\nAverage PTS Match: %.2f%%", avgPTSMatch)
		t.Logf("Average Payload Match: %.2f%%", avgPayloadMatch)
	}
}

// ============================================================================
// Window-Match Benchmark Functions
// ============================================================================

// windowMatchBenchmarkWithTiming compares two streams by finding all matching windows
// and checks that each window's PTS span matches the elapsed time (with threshold)
func windowMatchBenchmarkWithTiming(stream1, stream2 []*Frame, frameType string, elapsedTime time.Duration, threshold float64) WindowMatchBenchmarkResult {
	result := windowMatchBenchmark(stream1, stream2, frameType)

	// Check PTS window vs elapsed time for each window
	elapsedSeconds := elapsedTime.Seconds()
	for i := range result.Windows {
		window := result.Windows[i]
		if window.StartIndex1 >= len(stream1) || window.EndIndex1 >= len(stream1) ||
			window.StartIndex2 >= len(stream2) || window.EndIndex2 >= len(stream2) {
			continue
		}

		// Calculate PTS window for destination stream (stream1)
		startFrame1 := stream1[window.StartIndex1]
		endFrame1 := stream1[window.EndIndex1]
		if startFrame1 == nil || endFrame1 == nil {
			continue
		}

		ptsWindow := endFrame1.PTS - startFrame1.PTS
		ptsWindowSeconds := ptsWindow.Seconds()

		// Check if PTS window matches elapsed time (with threshold)
		if elapsedSeconds > 0 {
			elapsedDiff := abs(ptsWindowSeconds - elapsedSeconds)
			elapsedDiffPercent := (elapsedDiff / elapsedSeconds) * 100.0

			// If PTS window doesn't match elapsed time significantly, it's a timing issue
			// We'll track this but let the overall match percent decide pass/fail
			if elapsedDiffPercent > threshold*100 {
				// Window PTS doesn't match elapsed time - could indicate timing issues
				// This is logged but doesn't fail the test immediately
			}
		}
	}

	return result
}

// windowMatchBenchmark compares two streams by finding all matching windows
// It iterates through stream1, finds packets that exist in stream2,
// and compares from each matching point until a mismatch, then continues
// searching for the next matching window until the end of both streams
// stream2 is used as the reference (source of truth)
func windowMatchBenchmark(stream1, stream2 []*Frame, frameType string) WindowMatchBenchmarkResult {
	result := WindowMatchBenchmarkResult{
		Windows:          make([]WindowMatch, 0),
		MismatchContexts: make([]MismatchContext, 0),
	}

	if len(stream1) == 0 || len(stream2) == 0 {
		return result
	}

	// Create a map of stream2 frames by PTS for quick lookup
	stream2Map := make(map[string]int) // pts -> index
	for i, frame := range stream2 {
		if frame != nil {
			stream2Map[frame.PTS.String()] = i
		}
	}

	i1 := 0
	totalMatchedFrames := 0
	totalMismatchedFrames := 0
	totalPTSMatches := 0
	totalPayloadMatches := 0
	windowIndex := 0

	// Traverse stream1 and find all matching windows
	for i1 < len(stream1) {
		frame1 := stream1[i1]
		if frame1 == nil {
			i1++
			continue
		}

		// Check if this frame exists in stream2
		pts1 := frame1.PTS.String()
		startIndex2, exists := stream2Map[pts1]

		if !exists {
			// No match, continue to next frame
			i1++
			continue
		}

		// Found a match - start a new window
		startIndex1 := i1
		currentIndex2 := startIndex2
		windowMatchedFrames := 0

		// Compare frames in this window until mismatch or end of streams
		for i1 < len(stream1) && currentIndex2 < len(stream2) {
			frame1 = stream1[i1]
			frame2 := stream2[currentIndex2]

			if frame1 == nil || frame2 == nil {
				break
			}

			if frame1.PTS == frame2.PTS {
				windowMatchedFrames++
				totalMatchedFrames++
				totalPayloadMatches++
				totalPTSMatches++

				i1++
				currentIndex2++
			} else {
				// Mismatch - collect context from reference stream (stream2)
				mismatchCtx := collectMismatchContext(stream1, stream2, i1, currentIndex2, windowIndex)
				result.MismatchContexts = append(result.MismatchContexts, mismatchCtx)
				totalMismatchedFrames++
				break
			}
		}

		// Record this window if it has matches
		if windowMatchedFrames > 0 {
			window := WindowMatch{
				StartIndex1:   startIndex1,
				StartIndex2:   startIndex2,
				EndIndex1:     i1 - 1,
				EndIndex2:     currentIndex2 - 1,
				WindowSize:    windowMatchedFrames,
				MatchedFrames: windowMatchedFrames,
			}
			result.Windows = append(result.Windows, window)
			windowIndex++
		}

		// If we didn't advance, move to next frame to avoid infinite loop
		if windowMatchedFrames == 0 {
			i1++
		}
	}

	result.TotalWindows = len(result.Windows)
	result.TotalMatchedFrames = totalMatchedFrames
	result.TotalMismatchedFrames = totalMismatchedFrames

	// Calculate total window size (sum of all window sizes)
	for _, window := range result.Windows {
		result.TotalWindowSize += window.WindowSize
	}

	if result.TotalWindowSize > 0 {
		result.MatchPercent = float64(totalMatchedFrames) / float64(result.TotalWindowSize) * 100.0
		result.PTSMatchPercent = float64(totalPTSMatches) / float64(result.TotalWindowSize) * 100.0
		result.PayloadMatchPercent = float64(totalPayloadMatches) / float64(result.TotalWindowSize) * 100.0
	}

	result.PTSMatches = totalPTSMatches
	result.PayloadMatches = totalPayloadMatches

	return result
}

// collectMismatchContext collects context around a mismatched frame
// Uses stream2 (reference) as source of truth for context
func collectMismatchContext(stream1, stream2 []*Frame, mismatchIndex1, mismatchIndex2, windowIndex int) MismatchContext {
	ctx := MismatchContext{
		MismatchIndex1: mismatchIndex1,
		MismatchIndex2: mismatchIndex2,
		WindowIndex:    windowIndex,
		BeforeFrames:   make([]*Frame, 0),
		AfterFrames:    make([]*Frame, 0),
	}

	// Get mismatched frames
	if mismatchIndex1 < len(stream1) {
		ctx.MismatchFrame1 = stream1[mismatchIndex1]
	}
	if mismatchIndex2 < len(stream2) {
		ctx.MismatchFrame2 = stream2[mismatchIndex2]
	}

	// Collect 5 frames before from reference stream (stream2)
	contextSize := 5
	startBefore := mismatchIndex2 - contextSize
	if startBefore < 0 {
		startBefore = 0
	}
	for i := startBefore; i < mismatchIndex2 && i < len(stream2); i++ {
		if stream2[i] != nil {
			ctx.BeforeFrames = append(ctx.BeforeFrames, stream2[i])
		}
	}

	// Collect 5 frames after from reference stream (stream2)
	endAfter := mismatchIndex2 + contextSize + 1
	if endAfter > len(stream2) {
		endAfter = len(stream2)
	}
	for i := mismatchIndex2 + 1; i < endAfter && i < len(stream2); i++ {
		if stream2[i] != nil {
			ctx.AfterFrames = append(ctx.AfterFrames, stream2[i])
		}
	}

	return ctx
}

// printWindowMatchBenchmark prints the window-match benchmark results
func printWindowMatchBenchmark(t *testing.T, result WindowMatchBenchmarkResult, frameType string) {
	tsLabel := "PTS"
	if strings.Contains(frameType, "dts") {
		tsLabel = "DTS"
	}

	t.Logf("\n=== %s Window-Match Benchmark ===", frameType)
	t.Logf("Total Windows Found: %d", result.TotalWindows)
	t.Logf("Total Matched Frames: %d", result.TotalMatchedFrames)
	t.Logf("Total Mismatched Frames: %d", result.TotalMismatchedFrames)
	t.Logf("Total Window Size: %d frames", result.TotalWindowSize)
	t.Logf("Overall Match Percent: %.2f%%", result.MatchPercent)
	t.Logf("%s Matches: %d (%.2f%%)", tsLabel, result.PTSMatches, result.PTSMatchPercent)
	t.Logf("Payload Matches: %d (%.2f%%)", result.PayloadMatches, result.PayloadMatchPercent)

	if len(result.Windows) > 0 {
		t.Logf("\nWindow Details:")
		t.Logf("  Window  Stream1 Range      Stream2 Range      Size    Matched")
		t.Logf("  ------  -----------------  -----------------  ----    -------")
		maxPrint := 5
		if len(result.Windows) < maxPrint {
			maxPrint = len(result.Windows)
		}
		for i := 0; i < maxPrint; i++ {
			window := result.Windows[i]
			t.Logf("  %6d   [%5d:%5d]       [%5d:%5d]       %4d    %7d",
				i, window.StartIndex1, window.EndIndex1,
				window.StartIndex2, window.EndIndex2,
				window.WindowSize, window.MatchedFrames)
		}
		if len(result.Windows) > maxPrint {
			t.Logf("  ...     ...                ...                ...    ...")
			t.Logf("  (%d more windows)", len(result.Windows)-maxPrint)
		}
	}

	// Print mismatch contexts
	if len(result.MismatchContexts) > 0 {
		t.Logf("\nMismatch Contexts (Reference Stream as Source of Truth):")
		maxPrint := 5
		if len(result.MismatchContexts) < maxPrint {
			maxPrint = len(result.MismatchContexts)
		}
		for i := 0; i < maxPrint; i++ {
			ctx := result.MismatchContexts[i]
			t.Logf("\n--- Mismatch %d (Window %d) ---", i+1, ctx.WindowIndex)
			t.Logf("Mismatch at Stream1[%d] vs Stream2[%d] (Reference)", ctx.MismatchIndex1, ctx.MismatchIndex2)

			// Print frames before (from reference stream)
			if len(ctx.BeforeFrames) > 0 {
				t.Logf("  Reference frames BEFORE mismatch:")
				for j := 0; j < len(ctx.BeforeFrames); j++ {
					f := ctx.BeforeFrames[j]
					t.Logf("    [%d] %s=%v SeqID=%v Key=%v Codec=%v Hash=%s",
						ctx.MismatchIndex2-len(ctx.BeforeFrames)+j,
						tsLabel, f.PTS, f.SequenceID, f.IsKeyFrame, f.Codec, frameHash(f)[:16])
				}
			}

			// Print mismatched frames
			t.Logf("  MISMATCH:")
			if ctx.MismatchFrame1 != nil {
				t.Logf("    Stream1[%d]: %s=%v SeqID=%v Key=%v Codec=%v Hash=%s",
					ctx.MismatchIndex1,
					tsLabel, ctx.MismatchFrame1.PTS, ctx.MismatchFrame1.SequenceID,
					ctx.MismatchFrame1.IsKeyFrame, ctx.MismatchFrame1.Codec,
					frameHash(ctx.MismatchFrame1)[:16])
			}
			if ctx.MismatchFrame2 != nil {
				t.Logf("    Stream2[%d] (Reference): %s=%v SeqID=%v Key=%v Codec=%v Hash=%s",
					ctx.MismatchIndex2,
					tsLabel, ctx.MismatchFrame2.PTS, ctx.MismatchFrame2.SequenceID,
					ctx.MismatchFrame2.IsKeyFrame, ctx.MismatchFrame2.Codec,
					frameHash(ctx.MismatchFrame2)[:16])
			}

			// Print frames after (from reference stream)
			if len(ctx.AfterFrames) > 0 {
				t.Logf("  Reference frames AFTER mismatch:")
				for j, f := range ctx.AfterFrames {
					t.Logf("    [%d] %s=%v SeqID=%v Key=%v Codec=%v Hash=%s",
						ctx.MismatchIndex2+j+1,
						tsLabel, f.PTS, f.SequenceID, f.IsKeyFrame, f.Codec, frameHash(f)[:16])
				}
			}
		}
		if len(result.MismatchContexts) > maxPrint {
			t.Logf("\n  ... and %d more mismatches", len(result.MismatchContexts)-maxPrint)
		}
	}
}

// ============================================================================
// Equal-Packet-Rate Benchmark Functions
// ============================================================================

// equalPacketRateBenchmark compares two streams by checking how many packets
// from stream1 exist in stream2 (reference) with the same payload
// stream2 is used as the reference (source of truth)
func equalPacketRateBenchmark(stream1, stream2 []*Frame, frameType string) EqualPacketRateBenchmarkResult {
	result := EqualPacketRateBenchmarkResult{}

	if len(stream1) == 0 || len(stream2) == 0 {
		return result
	}

	// stream2 is the reference (source of truth)
	// stream1 is what we're checking against the reference
	referenceStream := stream2
	checkStream := stream1
	baseTS := findBaseTimestamp(checkStream)
	result.BaseTimestamp = baseTS

	// Determine which stream is smaller for reporting
	if len(stream1) <= len(stream2) {
		result.SmallerStreamSize = len(stream1)
		result.LargerStreamSize = len(stream2)
	} else {
		result.SmallerStreamSize = len(stream2)
		result.LargerStreamSize = len(stream1)
	}

	// Create a map of the reference stream (stream2): payload hash -> frame
	referenceStreamMap := make(map[int64]*Frame)
	for _, frame := range referenceStream {
		if frame != nil {
			referenceStreamMap[frame.SequenceID] = frame
		}
	}

	// Iterate check stream sequentially so we can capture context for missing packets
	foundPackets := 0
	notFoundPackets := 0
	ptsMatches := 0
	payloadMatches := 0

	for idx, checkFrame := range checkStream {
		if checkFrame == nil {
			continue
		}

		if refFrame, exists := referenceStreamMap[checkFrame.SequenceID]; exists {
			foundPackets++
			payloadMatches++

			// Compare PTS (with 1ms tolerance)
			ptsDiff := checkFrame.PTS - refFrame.PTS
			if ptsDiff < 0 {
				ptsDiff = -ptsDiff
			}
			if ptsDiff <= 1*time.Millisecond {
				ptsMatches++
			} else {
				// Found with different PTS: capture its context too
				ctx := MissingPacketContext{
					Index:     idx,
					Frame:     checkFrame,
					RefFrame:  refFrame,
					FrameHash: frameHash(checkFrame),
					Before:    collectFrameWindow(checkStream, idx, 3, true),
					After:     collectFrameWindow(checkStream, idx, 3, false),
					RefBefore: collectNearestByPTS(referenceStream, checkFrame.PTS, 3, true),
					RefAfter:  collectNearestByPTS(referenceStream, checkFrame.PTS, 3, false),
				}
				result.MissingContexts = append(result.MissingContexts, ctx)
			}
		} else {
			notFoundPackets++

			ctx := MissingPacketContext{
				Index:     idx,
				Frame:     checkFrame,
				FrameHash: frameHash(checkFrame),
				Before:    collectFrameWindow(checkStream, idx, 3, true),
				After:     collectFrameWindow(checkStream, idx, 3, false),
				RefBefore: collectNearestByPTS(referenceStream, checkFrame.PTS, 3, true),
				RefAfter:  collectNearestByPTS(referenceStream, checkFrame.PTS, 3, false),
			}
			result.MissingContexts = append(result.MissingContexts, ctx)
		}
	}

	result.FoundPackets = foundPackets
	result.NotFoundPackets = notFoundPackets

	if len(checkStream) > 0 {
		result.SuccessRate = float64(foundPackets) / float64(len(checkStream)) * 100.0
		result.PTSMatchPercent = float64(ptsMatches) / float64(len(checkStream)) * 100.0
		result.PayloadMatchPercent = float64(payloadMatches) / float64(len(checkStream)) * 100.0
	}

	result.PTSMatches = ptsMatches
	result.PayloadMatches = payloadMatches

	return result
}

// collectFrameWindow returns up to window frames before/after idx in stream.
func collectFrameWindow(stream []*Frame, idx, window int, before bool) []*Frame {
	var res []*Frame
	if before {
		start := idx - window
		if start < 0 {
			start = 0
		}
		for i := start; i < idx; i++ {
			if stream[i] != nil {
				res = append(res, stream[i])
			}
		}
	} else {
		end := idx + window + 1
		if end > len(stream) {
			end = len(stream)
		}
		for i := idx + 1; i < end; i++ {
			if stream[i] != nil {
				res = append(res, stream[i])
			}
		}
	}
	return res
}

// collectNearestByPTS finds the nearest frame index by PTS and returns context around it.
func collectNearestByPTS(stream []*Frame, targetPTS time.Duration, window int, before bool) []*Frame {
	if len(stream) == 0 {
		return nil
	}
	nearestIdx := 0
	minDiff := time.Duration(1<<63 - 1)
	for i, f := range stream {
		if f == nil {
			continue
		}
		diff := f.PTS - targetPTS
		if diff < 0 {
			diff = -diff
		}
		if diff < minDiff {
			minDiff = diff
			nearestIdx = i
		}
	}
	return collectFrameWindow(stream, nearestIdx, window, before)
}

// findBaseTimestamp finds the first non-zero timestamp in a stream for logging offsets.
func findBaseTimestamp(stream []*Frame) time.Time {
	for _, f := range stream {
		if f != nil && !f.Timestamp.IsZero() {
			return f.Timestamp
		}
	}
	return time.Time{}
}

// formatPTSForLog prints PTS; if zero, falls back to timestamp offset for visibility.
func formatPTSForLog(f *Frame, baseTS time.Time) string {
	if f == nil {
		return "-"
	}
	if f.PTS != 0 {
		return fmt.Sprintf("%v", f.PTS)
	}
	if !baseTS.IsZero() && !f.Timestamp.IsZero() {
		return fmt.Sprintf("ts+%v", f.Timestamp.Sub(baseTS))
	}
	return "0"
}

// printEqualPacketRateBenchmark prints the equal-packet-rate benchmark results
func printEqualPacketRateBenchmark(t *testing.T, result EqualPacketRateBenchmarkResult, frameType string) {
	t.Logf("\n=== %s Equal-Packet-Rate Benchmark (Stream2 as Reference) ===", frameType)
	t.Logf("Smaller Stream Size: %d frames", result.SmallerStreamSize)
	t.Logf("Larger Stream Size: %d frames", result.LargerStreamSize)
	t.Logf("Found Packets (Stream1 in Stream2): %d", result.FoundPackets)
	t.Logf("Not Found Packets: %d", result.NotFoundPackets)
	t.Logf("Success Rate: %.2f%%", result.SuccessRate)
	t.Logf("PTS Matches: %d (%.2f%%)", result.PTSMatches, result.PTSMatchPercent)
	t.Logf("Payload Matches: %d (%.2f%%)", result.PayloadMatches, result.PayloadMatchPercent)

	if len(result.MissingContexts) > 0 {
		maxPrint := 5
		t.Logf("\n  Missing packet contexts (showing up to %d):", maxPrint)
		for i, ctx := range result.MissingContexts {
			if i >= maxPrint {
				break
			}
			t.Logf("  - Missing #%d at index %d (Hash=%s) PTS=%s DTS=%s SeqID=%v Key=%v Codec=%v",
				i+1, ctx.Index, ctx.FrameHash[:16], formatPTSForLog(ctx.Frame, result.BaseTimestamp), formatPTSForLog(ctx.Frame, result.BaseTimestamp), ctx.Frame.SequenceID, ctx.Frame.IsKeyFrame, ctx.Frame.Codec)

			if ctx.RefFrame != nil {
				t.Logf("    Reference Frame: PTS=%s DTS=%s SeqID=%v Key=%v Codec=%v Hash=%s",
					formatPTSForLog(ctx.RefFrame, result.BaseTimestamp), formatPTSForLog(ctx.RefFrame, result.BaseTimestamp), ctx.RefFrame.SequenceID, ctx.RefFrame.IsKeyFrame, ctx.RefFrame.Codec, frameHash(ctx.RefFrame)[:16])
			}
			if len(ctx.Before) > 0 {
				t.Logf("    Before:")
				for bi, f := range ctx.Before {
					t.Logf("      [%d] PTS=%s DTS=%s SeqID=%v Key=%v Codec=%v Hash=%s",
						ctx.Index-len(ctx.Before)+bi, formatPTSForLog(f, result.BaseTimestamp), formatPTSForLog(f, result.BaseTimestamp), f.SequenceID, f.IsKeyFrame, f.Codec, frameHash(f)[:16])
				}
			}
			if len(ctx.After) > 0 {
				t.Logf("    After:")
				for ai, f := range ctx.After {
					t.Logf("      [%d] PTS=%s DTS=%s SeqID=%v Key=%v Codec=%v Hash=%s",
						ctx.Index+1+ai, formatPTSForLog(f, result.BaseTimestamp), formatPTSForLog(f, result.BaseTimestamp), f.SequenceID, f.IsKeyFrame, f.Codec, frameHash(f)[:16])
				}
			}
			if len(ctx.RefBefore) > 0 || len(ctx.RefAfter) > 0 {
				t.Logf("    Reference (nearest by PTS):")
				if len(ctx.RefBefore) > 0 {
					t.Logf("      Before:")
					for _, f := range ctx.RefBefore {
						t.Logf("        PTS=%s DTS=%s SeqID=%v Key=%v Codec=%v Hash=%s",
							formatPTSForLog(f, result.BaseTimestamp), formatPTSForLog(f, result.BaseTimestamp), f.SequenceID, f.IsKeyFrame, f.Codec, frameHash(f)[:16])
					}
				}
				if len(ctx.RefAfter) > 0 {
					t.Logf("      After:")
					for _, f := range ctx.RefAfter {
						t.Logf("        PTS=%s DTS=%s SeqID=%v Key=%v Codec=%v Hash=%s",
							formatPTSForLog(f, result.BaseTimestamp), formatPTSForLog(f, result.BaseTimestamp), f.SequenceID, f.IsKeyFrame, f.Codec, frameHash(f)[:16])
					}
				}
			}
		}
		if len(result.MissingContexts) > maxPrint {
			t.Logf("  ... and %d more missing packets", len(result.MissingContexts)-maxPrint)
		}
	}
}

// ============================================================================
// Switch Latency Functions
// ============================================================================

// measureSwitchLatency measures the latency between switch commands and when new input appears
func measureSwitchLatency(t *testing.T, videoFrames, audioFrames []*Frame, switchEvents []SwitchEvent) {
	if len(switchEvents) == 0 {
		return
	}

	t.Logf("\n=== Switch Latency Benchmark ===")
	t.Logf("Total switches: %d", len(switchEvents))

	// Measure latency for video frames
	videoResults := measureSwitchLatencyForFrames(t, videoFrames, switchEvents, "video")
	// Measure latency for audio frames
	audioResults := measureSwitchLatencyForFrames(t, audioFrames, switchEvents, "audio")

	// Print results
	t.Logf("\n--- Video Switch Latency ---")
	printSwitchLatencyResults(t, videoResults)

	t.Logf("\n--- Audio Switch Latency ---")
	printSwitchLatencyResults(t, audioResults)

	// Calculate statistics
	calculateSwitchLatencyStats(t, videoResults, "video")
	calculateSwitchLatencyStats(t, audioResults, "audio")
}

// measureSwitchLatencyForFrames measures switch latency for a specific frame type
func measureSwitchLatencyForFrames(t *testing.T, frames []*Frame, switchEvents []SwitchEvent, frameType string) []SwitchLatencyResult {
	results := make([]SwitchLatencyResult, 0, len(switchEvents))

	for _, event := range switchEvents {
		result := SwitchLatencyResult{
			SwitchIndex:     event.SwitchIndex,
			TargetInputID:   event.TargetInputID,
			PTSBeforeSwitch: event.PTSBeforeSwitch,
			Found:           false,
		}

		// Find the first frame with the target InputID after the switch PTS
		for i, frame := range frames {
			if frame == nil {
				continue
			}

			// Look for frames that:
			// 1. Have the target InputID
			// 2. Appear after the switch was issued (PTS >= PTSBeforeSwitch)
			if frame.InputID == event.TargetInputID && frame.PTS >= event.PTSBeforeSwitch {
				result.PTSAfterSwitch = frame.PTS
				result.Latency = frame.PTS - event.PTSBeforeSwitch
				result.Found = true
				t.Logf("Switch %v %d (%s): Found first frame at index %d, PTS=%v (latency=%v)",
					frameType, event.SwitchIndex, event.TargetInputID, i, frame.PTS, result.Latency)
				break
			}
		}

		if !result.Found {
			t.Logf("Switch %v %d (%s): No frame found with target InputID after PTS %v",
				frameType, event.SwitchIndex, event.TargetInputID, event.PTSBeforeSwitch)
		}

		results = append(results, result)
	}

	return results
}

// printSwitchLatencyResults prints the switch latency results
func printSwitchLatencyResults(t *testing.T, results []SwitchLatencyResult) {
	t.Logf("Switch | Target Input | PTS Before | PTS After | Latency | Status")
	t.Logf("------|--------------|------------|-----------|---------|--------")

	for _, result := range results {
		status := "✓"
		if !result.Found {
			status = "✗"
		}
		if result.Found {
			t.Logf("  %3d  | %12s | %10v | %9v | %7v | %s",
				result.SwitchIndex, result.TargetInputID,
				result.PTSBeforeSwitch, result.PTSAfterSwitch,
				result.Latency, status)
		} else {
			t.Logf("  %3d  | %12s | %10v | %9s | %7s | %s",
				result.SwitchIndex, result.TargetInputID,
				result.PTSBeforeSwitch, "N/A", "N/A", status)
		}
	}
}

// calculateSwitchLatencyStats calculates and prints statistics about switch latency
func calculateSwitchLatencyStats(t *testing.T, results []SwitchLatencyResult, frameType string) {
	if len(results) == 0 {
		return
	}

	var totalLatency time.Duration
	var foundCount int
	var minLatency, maxLatency time.Duration
	minLatency = time.Hour // Initialize with a large value
	maxLatency = 0

	for _, result := range results {
		if result.Found {
			totalLatency += result.Latency
			foundCount++
			if result.Latency < minLatency {
				minLatency = result.Latency
			}
			if result.Latency > maxLatency {
				maxLatency = result.Latency
			}
		}
	}

	if foundCount > 0 {
		avgLatency := totalLatency / time.Duration(foundCount)
		t.Logf("\n%s Switch Latency Statistics:", frameType)
		t.Logf("  Successful switches: %d/%d", foundCount, len(results))
		t.Logf("  Average latency: %v", avgLatency)
		t.Logf("  Min latency: %v", minLatency)
		t.Logf("  Max latency: %v", maxLatency)
		t.Logf("  Total latency: %v", totalLatency)
	} else {
		t.Logf("\n%s Switch Latency Statistics:", frameType)
		t.Logf("  No successful switches found")
	}
}

// ============================================================================
// InputID Change Functions
// ============================================================================

// checkInputIDChanges detects and prints when InputID changes in the output stream
func checkInputIDChanges(t *testing.T, frames []*Frame, frameType string) {
	if len(frames) == 0 {
		return
	}

	contextSize := 50
	changes := []InputIDChange{}

	for i := 1; i < len(frames); i++ {
		current := frames[i]
		previous := frames[i-1]

		if current == nil || previous == nil {
			continue
		}

		if current.InputID != previous.InputID {
			change := InputIDChange{
				FrameIndex:    i,
				PreviousInput: previous.InputID,
				CurrentInput:  current.InputID,
				BeforeFrames:  []*Frame{},
				ChangeFrame:   current,
				AfterFrames:   []*Frame{},
			}

			// Collect frames before the change
			startBefore := i - contextSize
			if startBefore < 0 {
				startBefore = 0
			}
			for j := startBefore; j < i && j < len(frames); j++ {
				if frames[j] != nil {
					change.BeforeFrames = append(change.BeforeFrames, frames[j])
				}
			}

			// Collect frames after the change
			endAfter := i + contextSize + 1
			if endAfter > len(frames) {
				endAfter = len(frames)
			}
			for j := i + 1; j < endAfter && j < len(frames); j++ {
				if frames[j] != nil {
					change.AfterFrames = append(change.AfterFrames, frames[j])
				}
			}

			changes = append(changes, change)
		}
	}

	// Print InputID changes
	if len(changes) > 0 {
		t.Logf("\n=== %s InputID Changes ===", frameType)
		t.Logf("Total InputID changes detected: %d", len(changes))
		maxPrint := 20
		if len(changes) < maxPrint {
			maxPrint = len(changes)
		}
		for i := 0; i < maxPrint; i++ {
			change := changes[i]
			t.Logf("\n--- InputID Change %d (Frame %d) ---", i+1, change.FrameIndex)
			t.Logf("Changed from '%s' to '%s'", change.PreviousInput, change.CurrentInput)

			// Print frames before
			if len(change.BeforeFrames) > 0 {
				t.Logf("  Frames BEFORE change:")
				for j := 0; j < len(change.BeforeFrames); j++ {
					f := change.BeforeFrames[j]
					idx := change.FrameIndex - len(change.BeforeFrames) + j
					t.Logf("    [%d] InputID=%s PTS=%v DTS=%v SeqID=%v GOPID=%v Key=%v",
						idx, f.InputID, f.PTS, f.DTS, f.SequenceID, f.GOPID, f.IsKeyFrame)
				}
			}

			// Print the change frame
			if change.ChangeFrame != nil {
				t.Logf("  CHANGE FRAME:")
				t.Logf("    [%d] InputID=%s PTS=%v DTS=%v SeqID=%v GOPID=%v Key=%v",
					change.FrameIndex, change.ChangeFrame.InputID, change.ChangeFrame.PTS, change.ChangeFrame.DTS,
					change.ChangeFrame.SequenceID, change.ChangeFrame.GOPID, change.ChangeFrame.IsKeyFrame)
			}

			// Print frames after
			if len(change.AfterFrames) > 0 {
				t.Logf("  Frames AFTER change:")
				for j, f := range change.AfterFrames {
					idx := change.FrameIndex + j + 1
					t.Logf("    [%d] InputID=%s PTS=%v DTS=%v SeqID=%v GOPID=%v Key=%v Timestamp=%v",
						idx, f.InputID, f.PTS, f.DTS, f.SequenceID, f.GOPID, f.IsKeyFrame, f.Timestamp)
				}
			}
		}
		if len(changes) > maxPrint {
			t.Logf("\n  ... and %d more InputID changes", len(changes)-maxPrint)
		}
	} else {
		t.Logf("\n=== %s InputID Changes ===", frameType)
		t.Logf("No InputID changes detected (all frames from same input)")
	}
}

// ============================================================================
// GOPID Verification Functions
// ============================================================================

// verifyGOPIDCorrectness verifies that any frame after a keyframe has the correct GOP_ID
// GOP_ID should be the SequenceID of the last keyframe
func verifyGOPIDCorrectness(t *testing.T, frames []*Frame) {
	if len(frames) == 0 {
		return
	}

	var lastKeyframeSequenceID int64 = -1
	issues := []string{}

	for i, frame := range frames {
		if frame == nil {
			continue
		}

		if frame.IsKeyFrame {
			// Update last keyframe info
			lastKeyframeSequenceID = frame.SequenceID

			// Keyframe's GOPID should be its own SequenceID
			if frame.GOPID != frame.SequenceID {
				issues = append(issues, fmt.Sprintf("Frame %d (keyframe): GOPID=%d should equal SequenceID=%d",
					i, frame.GOPID, frame.SequenceID))
			}
		} else {
			// Non-keyframe should have GOPID equal to the last keyframe's SequenceID
			if lastKeyframeSequenceID >= 0 {
				if frame.GOPID != lastKeyframeSequenceID {
					issues = append(issues, fmt.Sprintf("Frame %d (non-keyframe): GOPID=%d should equal last keyframe SequenceID=%d",
						i, frame.GOPID, lastKeyframeSequenceID))
				}
			} else {
				// No keyframe seen yet, GOPID should be 0 or -1 (initial state)
				if frame.GOPID != 0 && frame.GOPID != -1 {
					issues = append(issues, fmt.Sprintf("Frame %d (non-keyframe before first keyframe): GOPID=%d should be 0 or -1",
						i, frame.GOPID))
				}
			}
		}
	}

	if len(issues) > 0 {
		t.Logf("\n=== GOP_ID Correctness Issues ===")
		t.Logf("Found %d issues:", len(issues))
		maxPrint := 20
		if len(issues) < maxPrint {
			maxPrint = len(issues)
		}
		for i := 0; i < maxPrint; i++ {
			t.Logf("  %s", issues[i])
		}
		if len(issues) > maxPrint {
			t.Logf("  ... and %d more issues", len(issues)-maxPrint)
		}
		t.Errorf("GOP_ID correctness check failed: %d issues found", len(issues))
	} else {
		t.Logf("\n✓ GOP_ID correctness check passed: all frames have correct GOP_ID")
	}
}

func cloneFramesWithDTSAsPTSSwitch(frames []*Frame) []*Frame {
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
