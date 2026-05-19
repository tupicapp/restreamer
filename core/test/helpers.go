package test

import (
	"crypto/sha256"
	"encoding/hex"
	"restreamer/core/inputs"
	"restreamer/core/shared"
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
