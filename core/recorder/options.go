package recorder

import (
	ilogger "github.com/tupicapp/restreamer/core/logger"
	"strings"
	"time"

	"go.uber.org/zap"
)

type Option func(*Recorder)

func WithSegmentDuration(d time.Duration) Option {
	return func(r *Recorder) {
		if d > 0 {
			r.segmentDuration = d
		}
	}
}

func WithTargetDuration(target int) Option {
	return func(r *Recorder) {
		if target > 0 {
			r.targetDuration = target
		}
	}
}

func WithPathPrefix(prefix string) Option {
	return func(r *Recorder) {
		r.pathPrefix = strings.TrimSpace(prefix)
	}
}

func withNowFunc(now func() time.Time) Option {
	return func(r *Recorder) {
		if now != nil {
			r.now = now
		}
	}
}

func normalizeBaseID(id string) string {
	id = strings.TrimSpace(id)
	id = strings.TrimSuffix(id, ".m3u8")
	return strings.TrimSpace(id)
}

func durationTo90k(d time.Duration) int64 {
	return int64(d) * 90000 / int64(time.Second)
}

func ticks90kToDuration(v int64) time.Duration {
	return time.Duration(v) * time.Second / 90000
}

func h264ExtractSPSPPS(nalus [][]byte) ([]byte, []byte) {
	var sps []byte
	var pps []byte

	for _, nalu := range nalus {
		switch h264NALTypeFromUnit(nalu) {
		case 7:
			sps = cloneBytes(nalu)
		case 8:
			pps = cloneBytes(nalu)
		}
	}

	return sps, pps
}

func stripAnnexBStartCode(nalu []byte) []byte {
	if len(nalu) >= 4 && nalu[0] == 0x00 && nalu[1] == 0x00 {
		if nalu[2] == 0x01 {
			return nalu[3:]
		}
		if len(nalu) >= 5 && nalu[2] == 0x00 && nalu[3] == 0x01 {
			return nalu[4:]
		}
	}
	return nalu
}

func h264NALTypeFromUnit(nalu []byte) byte {
	nalu = stripAnnexBStartCode(nalu)
	if len(nalu) == 0 {
		return 0
	}
	return nalu[0] & 0x1F
}

func h264SPSPPSPresent(nalus [][]byte) (bool, bool) {
	hasSPS := false
	hasPPS := false
	for _, nalu := range nalus {
		switch h264NALTypeFromUnit(nalu) {
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
		if hasSPS && hasPPS {
			return true, true
		}
	}
	return hasSPS, hasPPS
}

func h264EnsureSPSPPSOnKeyFrame(nalus [][]byte, isKeyFrame bool, cachedSPS, cachedPPS []byte) [][]byte {
	if !isKeyFrame {
		return nalus
	}

	hasSPS, hasPPS := h264SPSPPSPresent(nalus)
	if hasSPS && hasPPS {
		return nalus
	}

	out := make([][]byte, 0, len(nalus)+2)
	if !hasSPS && len(cachedSPS) > 0 {
		out = append(out, cloneBytes(cachedSPS))
	}
	if !hasPPS && len(cachedPPS) > 0 {
		out = append(out, cloneBytes(cachedPPS))
	}
	out = append(out, nalus...)
	return out
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func getLogger() *zap.Logger {
	return ilogger.GetLogger()
}
