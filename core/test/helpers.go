package test

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/shared"
	"time"
)

type Frame = shared.Frame
type Stream = inputs.Stream
type Event = shared.Event
type State = shared.State

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func frameHash(frame *Frame) string {
	hasher := sha256.New()
	for _, chunk := range frame.Payload {
		hasher.Write(chunk)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func stripAnnexB(nalu []byte) []byte {
	if len(nalu) < 3 {
		return nalu
	}
	if nalu[0] == 0 && nalu[1] == 0 && nalu[2] == 1 {
		return nalu[3:]
	}
	if len(nalu) >= 4 && nalu[0] == 0 && nalu[1] == 0 && nalu[2] == 0 && nalu[3] == 1 {
		return nalu[4:]
	}
	return nalu
}

func cloneFramesWithDTSAsPTS(frames []*Frame) []*Frame {
	cloned := make([]*Frame, len(frames))
	for i, f := range frames {
		clone := *f
		clone.PTS = f.DTS
		cloned[i] = &clone
	}
	return cloned
}

func normalizeFramesForTiming(frames []*Frame, maxGap time.Duration) ([]*Frame, int) {
	if len(frames) == 0 {
		return nil, 0
	}

	type frameRange struct {
		start int
		end   int
		count int
		span  time.Duration
	}

	ranges := make([]frameRange, 0, 4)
	start := -1
	last := -1
	lastPTS := time.Duration(0)

	closeRange := func(end int) {
		if start == -1 || end < start {
			return
		}

		count := 0
		firstPTS := time.Duration(0)
		lastRangePTS := time.Duration(0)
		for i := start; i <= end; i++ {
			if frames[i] == nil {
				continue
			}
			if count == 0 {
				firstPTS = frames[i].PTS
			}
			lastRangePTS = frames[i].PTS
			count++
		}
		if count == 0 {
			return
		}

		ranges = append(ranges, frameRange{
			start: start,
			end:   end,
			count: count,
			span:  lastRangePTS - firstPTS,
		})
	}

	for i, frame := range frames {
		if frame == nil {
			continue
		}

		if start == -1 {
			start = i
			last = i
			lastPTS = frame.PTS
			continue
		}

		gap := frame.PTS - lastPTS
		if gap < -maxGap || gap > maxGap {
			closeRange(last)
			start = i
		}

		last = i
		lastPTS = frame.PTS
	}
	closeRange(last)

	if len(ranges) == 0 {
		return nil, 0
	}

	best := ranges[0]
	for _, candidate := range ranges[1:] {
		if candidate.count > best.count || (candidate.count == best.count && candidate.span > best.span) {
			best = candidate
		}
	}

	first := frames[best.start]
	if first == nil {
		return nil, 0
	}

	normalized := make([]*Frame, 0, best.count)
	basePTS := first.PTS
	baseDTS := first.DTS
	for i := best.start; i <= best.end; i++ {
		if frames[i] == nil {
			continue
		}
		clone := *frames[i]
		clone.PTS -= basePTS
		clone.DTS -= baseDTS
		normalized = append(normalized, &clone)
	}

	return normalized, len(frames) - len(normalized)
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
