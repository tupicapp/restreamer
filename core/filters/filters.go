package filters

import (
	"restreamer/core/logger"
	"restreamer/core/shared"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

func isVideoCodec(codec string) bool {
	return codec == "h264" || codec == "h265"
}

func isAudioCodec(codec string) bool {
	return codec == "aac" || codec == "opus"
}

type genPTSFilter struct {
	// Shared timestamp state.
	generalVideoPTS time.Duration
	generalAudioPTS time.Duration

	lastProcessedVideoPTS time.Duration
	lastProcessedAudioPTS time.Duration

	generalVideoDTS time.Duration
	generalAudioDTS time.Duration

	lastProcessedVideoDTS time.Duration
	lastProcessedAudioDTS time.Duration

	lastVideoDelta time.Duration
	lastAudioDelta time.Duration

	lastVideoDeltaDTS time.Duration
	lastAudioDeltaDTS time.Duration

	lastVideoInputID string
	lastAudioInputID string
}

func (f *genPTSFilter) Name() string {
	return "genPTSFilter"
}

func (f *genPTSFilter) Filter(frame *shared.Frame) *shared.Frame {
	if f == nil || frame == nil {
		return frame
	}

	if frame.Codec == "h264" {
		f.applyGenPTSVideo(frame)
	} else {
		f.applyGenPTSAudio(frame)
	}

	return frame
}

func (g *genPTSFilter) applyGenPTSVideo(f *shared.Frame) {
	if f == nil {
		return
	}

	originalPTS := f.PTS
	originalDTS := f.DTS
	prevDTS := g.generalVideoDTS
	prevPTS := g.generalVideoPTS

	if g.lastProcessedVideoDTS == 0 {
		g.generalVideoDTS = originalDTS
		if g.generalVideoDTS == 0 {
			g.generalVideoDTS = originalPTS
		}
		if g.lastVideoDeltaDTS == 0 {
			g.lastVideoDeltaDTS = 33 * time.Millisecond
		}
	} else {
		delta := originalDTS - g.lastProcessedVideoDTS
		if delta > 0 && delta < 500*time.Millisecond {
			g.generalVideoDTS += delta
			g.lastVideoDeltaDTS = delta
		} else {
			g.generalVideoDTS += g.lastVideoDeltaDTS
			if g.lastVideoDeltaDTS == 0 {
				g.generalVideoDTS += 40 * time.Millisecond
				g.lastVideoDeltaDTS = 40 * time.Millisecond
			}
		}
	}

	if g.generalVideoDTS < prevDTS {
		g.generalVideoDTS = prevDTS + g.lastVideoDeltaDTS
		if g.lastVideoDeltaDTS == 0 {
			g.generalVideoDTS += 40 * time.Millisecond
			g.lastVideoDeltaDTS = 40 * time.Millisecond
		}
	}

	if g.lastProcessedVideoPTS == 0 {
		g.generalVideoPTS = originalPTS
		if g.generalVideoPTS < g.generalVideoDTS {
			if f.IsKeyFrame {
				g.generalVideoPTS = g.generalVideoDTS
				g.lastVideoDelta = 0
			} else {
				g.generalVideoPTS = g.generalVideoDTS + 33*time.Millisecond
				g.lastVideoDelta = 40 * time.Millisecond
			}
		} else {
			g.lastVideoDelta = g.generalVideoPTS - g.generalVideoDTS
		}
	} else {
		dtsAdvance := g.generalVideoDTS - prevDTS
		g.generalVideoPTS = prevPTS + dtsAdvance
		if g.lastVideoDelta == 0 && !f.IsKeyFrame {
			g.lastVideoDelta = 40 * time.Millisecond
		}
		minPTS := g.generalVideoDTS + g.lastVideoDelta
		if g.generalVideoPTS < minPTS {
			g.generalVideoPTS = minPTS
		}
		g.lastVideoDelta = g.generalVideoPTS - g.generalVideoDTS
	}

	if g.generalVideoPTS < prevPTS {
		g.generalVideoPTS = prevPTS + g.lastVideoDeltaDTS
		if g.lastVideoDeltaDTS == 0 {
			g.generalVideoPTS = prevPTS + 40*time.Millisecond
		}
		g.lastVideoDelta = g.generalVideoPTS - g.generalVideoDTS
		if g.lastVideoDelta < 0 {
			if f.IsKeyFrame {
				g.generalVideoPTS = g.generalVideoDTS
				g.lastVideoDelta = 0
			} else {
				g.generalVideoPTS = g.generalVideoDTS + 40*time.Millisecond
				g.lastVideoDelta = 40 * time.Millisecond
			}
		}
	}

	if g.generalVideoPTS <= g.generalVideoDTS {
		if f.IsKeyFrame {
			g.generalVideoPTS = g.generalVideoDTS
			g.lastVideoDelta = 0
		} else {
			g.generalVideoPTS = g.generalVideoDTS + 40*time.Millisecond
			g.lastVideoDelta = 40 * time.Millisecond
		}
	}

	if originalPTS != g.generalVideoPTS {
		logger.GetLogger().Debug("gop buffer: changing video pts",
			zap.Duration("original_pts", originalPTS),
			zap.Duration("new_pts", g.generalVideoPTS),
			zap.Duration("original_dts", originalDTS),
			zap.Duration("new_dts", g.generalVideoDTS))
	}

	f.DTS = g.generalVideoDTS
	f.PTS = g.generalVideoPTS

	g.lastProcessedVideoDTS = originalDTS
	g.lastProcessedVideoPTS = originalPTS
	g.lastVideoInputID = f.InputID
}

func (g *genPTSFilter) applyGenPTSAudio(f *shared.Frame) {
	if f == nil {
		return
	}

	originalPTS := f.PTS
	originalDTS := f.DTS
	prevDTS := g.generalAudioDTS
	prevPTS := g.generalAudioPTS

	if g.lastProcessedAudioDTS == 0 {
		g.generalAudioDTS = originalDTS
		if g.generalAudioDTS == 0 {
			g.generalAudioDTS = originalPTS
		}
		if g.lastAudioDeltaDTS == 0 {
			g.lastAudioDeltaDTS = 23 * time.Millisecond
		}
	} else {
		delta := originalDTS - g.lastProcessedAudioDTS
		if delta > 0 && delta < 500*time.Millisecond {
			g.generalAudioDTS += delta
			g.lastAudioDeltaDTS = delta
		} else {
			g.generalAudioDTS += g.lastAudioDeltaDTS
			if g.lastAudioDeltaDTS == 0 {
				g.generalAudioDTS += 23 * time.Millisecond
				g.lastAudioDeltaDTS = 23 * time.Millisecond
			}
		}
	}

	if g.generalAudioDTS < prevDTS {
		g.generalAudioDTS = prevDTS + g.lastAudioDeltaDTS
		if g.lastAudioDeltaDTS == 0 {
			g.generalAudioDTS += 23 * time.Millisecond
			g.lastAudioDeltaDTS = 23 * time.Millisecond
		}
	}

	if g.lastProcessedAudioPTS == 0 {
		g.generalAudioPTS = originalPTS
		if g.generalAudioPTS < g.generalAudioDTS {
			g.generalAudioPTS = g.generalAudioDTS
			g.lastAudioDelta = 0
		} else {
			g.lastAudioDelta = g.generalAudioPTS - g.generalAudioDTS
		}
	} else {
		dtsAdvance := g.generalAudioDTS - prevDTS
		g.generalAudioPTS = prevPTS + dtsAdvance
		minPTS := g.generalAudioDTS + g.lastAudioDelta
		if g.generalAudioPTS < minPTS {
			g.generalAudioPTS = minPTS
		}
		g.lastAudioDelta = g.generalAudioPTS - g.generalAudioDTS
	}

	if g.generalAudioPTS < prevPTS {
		g.generalAudioPTS = prevPTS + g.lastAudioDeltaDTS
		if g.lastAudioDeltaDTS == 0 {
			g.generalAudioPTS = prevPTS + 23*time.Millisecond
		}
		g.lastAudioDelta = g.generalAudioPTS - g.generalAudioDTS
		if g.lastAudioDelta < 0 {
			g.generalAudioPTS = g.generalAudioDTS
			g.lastAudioDelta = 0
		}
	}

	if g.generalAudioPTS < g.generalAudioDTS {
		g.generalAudioPTS = g.generalAudioDTS
		g.lastAudioDelta = 0
	}

	if originalPTS != g.generalAudioPTS {
		logger.GetLogger().Debug("gop buffer: audio pts changed",
			zap.Duration("original_pts", originalPTS),
			zap.Duration("new_pts", g.generalAudioPTS))
	}

	f.DTS = g.generalAudioDTS
	f.PTS = g.generalAudioPTS

	g.lastProcessedAudioDTS = originalDTS
	g.lastProcessedAudioPTS = originalPTS
	g.lastAudioInputID = f.InputID
}

type DecodingFilter struct {
	lastKeyFrame *shared.Frame
	lastFrame    *shared.Frame
	lastSequence int64
	lastInputID  string
	dropGOPID    int64
	dropUntilKey bool
}

func (f *DecodingFilter) Name() string {
	return "Decoding_Filter"
}

func (f *DecodingFilter) Filter(frame *shared.Frame) *shared.Frame {
	if frame == nil {
		return nil
	}

	if time.Since(frame.Timestamp) > 500*time.Millisecond {
		return nil
	}

	if f.lastInputID == "" {
		f.lastInputID = frame.InputID
	} else if f.lastInputID != frame.InputID {
		// Input switched: wait for a fresh keyframe from the new input.
		f.lastInputID = frame.InputID
		f.dropUntilKey = true
		f.dropGOPID = 0
		f.lastKeyFrame = nil
		f.lastFrame = nil
		f.lastSequence = 0
	}

	if frame.IsKeyFrame {
		f.lastKeyFrame = frame
		f.lastFrame = frame
		f.lastSequence = frame.SequenceID
		f.dropUntilKey = false
		f.dropGOPID = 0
		return frame
	}

	if f.dropUntilKey {
		logger.GetLogger().Error("streamer: drop until key",
			zap.Time("timestamp", frame.Timestamp),
			zap.String("input_id", frame.InputID),
			zap.String("codec", frame.Codec),
		)
		return nil
	}

	if f.lastFrame == nil || f.lastKeyFrame == nil {
		logger.GetLogger().Error("streamer: frame is not a key frame and last frame is nil or last key frame is nil",
			zap.Time("timestamp", frame.Timestamp))

		return nil
	}

	if f.lastSequence != 0 && frame.SequenceID > f.lastSequence+1 {
		// Missing frame in current GOP: drop until next keyframe.
		f.dropUntilKey = true
		f.dropGOPID = frame.GOPID
		f.lastKeyFrame = nil
		f.lastFrame = nil
		f.lastSequence = frame.SequenceID
		return nil
	}

	if frame.GOPID == f.lastFrame.GOPID {
		f.lastFrame = frame
		f.lastSequence = frame.SequenceID
		return frame
	}

	f.dropUntilKey = true
	f.dropGOPID = frame.GOPID
	logger.GetLogger().Error("streamer: GOP mismatch, dropping until next keyframe",
		zap.Time("timestamp", frame.Timestamp),
		zap.Int64("frame_gopid", frame.GOPID),
		zap.Int64("last_gopid", f.lastFrame.GOPID))

	return nil
}

type Surger struct {
	mu sync.RWMutex

	// Current active input for video gating.
	activeInput string

	// Drop video frames until we receive a keyframe for the current input.
	dropVideoUntilKey bool

	// Last accepted video keyframe (per activeInput).
	lastVideoKeyframe *shared.Frame
	lastVideoKeySeq   int64

	// PTS normalization (per codec) to avoid regressions.
	genptsFilterVideo *genPTSFilter
	genptsFilterAudio *genPTSFilter
}

func NewSurger() *Surger {
	return &Surger{
		genptsFilterVideo: &genPTSFilter{},
		genptsFilterAudio: &genPTSFilter{},
	}
}

func (s *Surger) Surge(frames []*shared.Frame) []*shared.Frame {
	if s == nil || len(frames) == 0 {
		return frames
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*shared.Frame, 0, len(frames))
	for i, frame := range frames {
		if frame == nil {
			logger.GetLogger().Warn("surge: ignored frame (nil)", zap.Int("index", i))
			continue
		}

		if isVideoCodec(frame.Codec) {
			// Gate input switches on a *real* keyframe (no synthetic promotion).
			if s.activeInput == "" {
				s.activeInput = frame.InputID
				s.dropVideoUntilKey = true
			} else if frame.InputID != s.activeInput {
				s.activeInput = frame.InputID
				s.dropVideoUntilKey = true
				s.lastVideoKeyframe = nil
				s.lastVideoKeySeq = 0
			}

			if s.dropVideoUntilKey {
				if !frame.IsKeyFrame {
					continue
				}
				s.dropVideoUntilKey = false
			}

			if frame.IsKeyFrame {
				s.lastVideoKeyframe = frame
				s.lastVideoKeySeq = frame.SequenceID
				frame.GOPID = frame.SequenceID
			} else {
				// Non-keyframes must always reference the latest keyframe.
				if s.lastVideoKeySeq == 0 {
					continue
				}
				frame.GOPID = s.lastVideoKeySeq
			}

			out = append(out, frame)
			continue
		}

		// Audio frames: treat as independently decodable.
		if isAudioCodec(frame.Codec) {
			frame.IsKeyFrame = true
			if frame.GOPID == 0 {
				frame.GOPID = frame.SequenceID
			}
			out = append(out, frame)
			continue
		}

		// Unknown codec: pass through as-is.
		out = append(out, frame)
	}

	for i, val := range out {
		if isVideoCodec(val.Codec) {
			out[i] = s.genptsFilterVideo.Filter(val)
		} else if isAudioCodec(val.Codec) {
			out[i] = s.genptsFilterAudio.Filter(val)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].PTS < out[j].PTS
	})

	return out
}

func cloneFrame(frame *shared.Frame) *shared.Frame {
	if frame == nil {
		return nil
	}
	out := *frame
	if len(frame.Payload) > 0 {
		out.Payload = make([][]byte, len(frame.Payload))
		for i, p := range frame.Payload {
			out.Payload[i] = append([]byte(nil), p...)
		}
	}
	return &out
}

// Surge is a stateless convenience wrapper. If you need to keep the last GOP per InputID
// across calls, use a persistent `Surger` instance instead.
func Surge(frames []*shared.Frame) []*shared.Frame {
	return NewSurger().Surge(frames)
}

// ---- Timeline rebasing + switch gating (used by GOPBuffer) ----

type rebasedFrame struct {
	f         *shared.Frame
	orderTime time.Duration
}

type rebasedHeap []rebasedFrame

func (h rebasedHeap) Len() int           { return len(h) }
func (h rebasedHeap) Less(i, j int) bool { return h[i].orderTime < h[j].orderTime }
func (h rebasedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rebasedHeap) Push(x any)        { *h = append(*h, x.(rebasedFrame)) }
func (h *rebasedHeap) Pop() any          { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
func (h rebasedHeap) Peek() (rebasedFrame, bool) {
	if len(h) == 0 {
		return rebasedFrame{}, false
	}
	return h[0], true
}

type TimelineRebaser struct {
	mu sync.Mutex

	activeInput  string
	pendingInput string
	switching    bool

	// We only commit a switch on the first video keyframe of the pending input.
	pendingAudio []*shared.Frame

	// Mapping: outPTS = outBasePTS + (origPTS - origVideoBasePTS)
	origVideoBasePTS time.Duration
	outBasePTS       time.Duration
	haveMapping      bool

	lastVideoKeySeq int64

	// Track monotonicity per track.
	lastOutVideoPTS time.Duration
	lastOutAudioPTS time.Duration
	lastOutVideoDTS time.Duration
	lastOutAudioDTS time.Duration

	// Cross-track continuity base.
	lastOutPTS time.Duration

	lastVideoDur time.Duration
	lastAudioDur time.Duration

	// Track the previous accepted source timestamps per track so continuity
	// decisions are based on source gaps, not total elapsed stream time.
	haveLastSourceVideoPTS bool
	haveLastSourceAudioPTS bool
	lastSourceVideoPTS     time.Duration
	lastSourceAudioPTS     time.Duration

	// Discontinuity handling: only re-anchor when the source itself jumps.
	maxSourceForwardJump time.Duration
	maxSourceBackwardGap time.Duration
}

func NewTimelineRebaser() *TimelineRebaser {
	return &TimelineRebaser{
		lastVideoDur:         33 * time.Millisecond,
		lastAudioDur:         23 * time.Millisecond,
		maxSourceForwardJump: 2 * time.Second,
		maxSourceBackwardGap: 500 * time.Millisecond,
	}
}

func saneDur(f *shared.Frame, fallback time.Duration) time.Duration {
	if f == nil {
		return fallback
	}
	if f.Duration <= 0 || f.Duration > 500*time.Millisecond {
		return fallback
	}
	return f.Duration
}

func (r *TimelineRebaser) Process(in *shared.Frame) []*shared.Frame {
	if r == nil || in == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Work on a copy: multiple outputs can share frame pointers.
	f := cloneFrame(in)
	if f == nil {
		return nil
	}

	isVideo := isVideoCodec(f.Codec)
	isAudio := isAudioCodec(f.Codec)
	if !isVideo && !isAudio {
		return []*shared.Frame{f}
	}

	// Update last durations (used for continuity).
	if isVideo {
		r.lastVideoDur = saneDur(f, r.lastVideoDur)
	} else {
		r.lastAudioDur = saneDur(f, r.lastAudioDur)
		f.IsKeyFrame = true
	}

	origPTS := f.PTS
	origDTS := f.DTS
	if origPTS == 0 && origDTS != 0 {
		origPTS = origDTS
	}

	// Bootstrap: wait for the first video keyframe.
	if r.activeInput == "" {
		r.activeInput = f.InputID
		r.switching = true
		r.pendingInput = f.InputID
		r.pendingAudio = r.pendingAudio[:0]
	}

	// Detect input changes.
	if f.InputID != r.activeInput {
		// Start (or update) a pending switch.
		if !r.switching || r.pendingInput != f.InputID {
			r.switching = true
			r.pendingInput = f.InputID
			r.pendingAudio = r.pendingAudio[:0]
		}
	}

	if r.switching {
		// Buffer audio until the first keyframe of the pending input.
		if isAudio && f.InputID == r.pendingInput {
			if len(r.pendingAudio) < 500 {
				r.pendingAudio = append(r.pendingAudio, f)
			}
			return nil
		}

		// Drop video until keyframe for the pending input.
		if isVideo {
			if f.InputID != r.pendingInput {
				return nil
			}
			if !f.IsKeyFrame {
				return nil
			}

			// Commit switch on this keyframe.
			r.activeInput = r.pendingInput
			r.pendingInput = ""
			r.switching = false

			// Establish mapping anchors.
			r.origVideoBasePTS = origPTS
			if r.origVideoBasePTS == 0 {
				r.origVideoBasePTS = 0
			}

			step := r.lastVideoDur
			if step <= 0 {
				step = 33 * time.Millisecond
			}
			if r.lastOutPTS > 0 {
				r.outBasePTS = r.lastOutPTS + step
			} else {
				r.outBasePTS = 0
			}
			r.haveMapping = true
			r.haveLastSourceVideoPTS = false
			r.haveLastSourceAudioPTS = false

			// Reset GOP tracking for new input.
			r.lastVideoKeySeq = 0

			// Rebase the keyframe and any buffered audio at/after the cut.
			out := make([]*shared.Frame, 0, 1+len(r.pendingAudio))
			out = append(out, r.rebaseOneLocked(f))
			for _, af := range r.pendingAudio {
				// Drop audio that belongs to "before the cut" in source time.
				aOrigPTS := af.PTS
				if aOrigPTS == 0 && af.DTS != 0 {
					aOrigPTS = af.DTS
				}
				if aOrigPTS < r.origVideoBasePTS {
					continue
				}
				out = append(out, r.rebaseOneLocked(af))
			}
			r.pendingAudio = r.pendingAudio[:0]
			return out
		}

		// Any other case while switching: drop.
		return nil
	}

	// Not switching: accept only frames for active input (ignore any stragglers).
	if f.InputID != r.activeInput {
		return nil
	}

	return []*shared.Frame{r.rebaseOneLocked(f)}
}

func (r *TimelineRebaser) rebaseOneLocked(f *shared.Frame) *shared.Frame {
	if r == nil || f == nil || !r.haveMapping {
		return f
	}

	isVideo := isVideoCodec(f.Codec)
	isAudio := isAudioCodec(f.Codec)

	origPTS := f.PTS
	origDTS := f.DTS
	if origPTS == 0 && origDTS != 0 {
		origPTS = origDTS
	}

	if r.shouldReanchorLocked(isVideo, isAudio, origPTS) {
		step := r.lastVideoDur
		if !isVideo {
			step = r.lastAudioDur
		}
		if step <= 0 {
			step = 33 * time.Millisecond
		}

		r.origVideoBasePTS = origPTS
		if r.lastOutPTS > 0 {
			r.outBasePTS = r.lastOutPTS + step
		} else {
			r.outBasePTS = 0
		}
	}

	delta := origPTS - r.origVideoBasePTS
	if delta < 0 {
		delta = 0
	}

	outPTS := r.outBasePTS + delta
	if isVideo {
		r.lastOutVideoPTS = outPTS
	} else if isAudio {
		minStep := saneDur(f, r.lastAudioDur)
		if outPTS < r.lastOutAudioPTS {
			outPTS = r.lastOutAudioPTS + minStep
		}
		r.lastOutAudioPTS = outPTS
	}

	// Preserve PTS-DTS offset when possible.
	ptsDtsDelta := f.PTS - f.DTS
	outDTS := outPTS - ptsDtsDelta
	if outDTS < 0 {
		outDTS = 0
	}
	if outDTS > outPTS {
		outDTS = outPTS
	}
	if isVideo {
		minStep := saneDur(f, r.lastVideoDur)
		if outDTS < r.lastOutVideoDTS {
			outDTS = r.lastOutVideoDTS + minStep
			if outDTS > outPTS {
				outDTS = outPTS
			}
		}
		r.lastOutVideoDTS = outDTS
	} else if isAudio {
		minStep := saneDur(f, r.lastAudioDur)
		if outDTS < r.lastOutAudioDTS {
			outDTS = r.lastOutAudioDTS + minStep
			if outDTS > outPTS {
				outDTS = outPTS
			}
		}
		r.lastOutAudioDTS = outDTS
	}

	f.PTS = outPTS
	f.DTS = outDTS

	// Maintain GOPID correctness for video.
	if isVideo {
		if f.IsKeyFrame {
			r.lastVideoKeySeq = f.SequenceID
			f.GOPID = f.SequenceID
		} else {
			if r.lastVideoKeySeq != 0 {
				f.GOPID = r.lastVideoKeySeq
			}
		}
	} else if isAudio {
		f.IsKeyFrame = true
		if f.GOPID == 0 {
			f.GOPID = f.SequenceID
		}
	}

	if f.PTS > r.lastOutPTS {
		r.lastOutPTS = f.PTS
	}
	r.updateLastSourcePTSLocked(isVideo, isAudio, origPTS)

	return f
}

func (r *TimelineRebaser) shouldReanchorLocked(isVideo, isAudio bool, origPTS time.Duration) bool {
	if isVideo {
		if !r.haveLastSourceVideoPTS {
			return false
		}
		gap := origPTS - r.lastSourceVideoPTS
		return gap < -r.maxSourceBackwardGap || gap > r.maxSourceForwardJump
	}
	if isAudio {
		if !r.haveLastSourceAudioPTS {
			return false
		}
		gap := origPTS - r.lastSourceAudioPTS
		return gap < -r.maxSourceBackwardGap || gap > r.maxSourceForwardJump
	}
	return false
}

func (r *TimelineRebaser) updateLastSourcePTSLocked(isVideo, isAudio bool, origPTS time.Duration) {
	if isVideo {
		r.lastSourceVideoPTS = origPTS
		r.haveLastSourceVideoPTS = true
		return
	}
	if isAudio {
		r.lastSourceAudioPTS = origPTS
		r.haveLastSourceAudioPTS = true
	}
}
