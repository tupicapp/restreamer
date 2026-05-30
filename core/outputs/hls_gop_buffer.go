package outputs

import (
	"container/heap"
	"strings"
	"sync"
	"time"

	"github.com/tupicapp/restreamer/core/shared"
)

const (
	defaultHLSGOPInputBuffer  = 4096
	defaultHLSGOPReadyBuffer  = 1024
	maxHLSBufferedSwitchAudio = 500
	maxHLSPendingAudioWindow  = 400 * time.Millisecond
	maxHLSResumeBacklogWindow = 400 * time.Millisecond
	maxHLSOverdueEmitDrop     = 800 * time.Millisecond
	maxHLSPacingIdleReset     = 1200 * time.Millisecond
)

type hlsBufferedOrderFrame struct {
	frame      *shared.Frame
	orderTime  time.Duration
	arrivedAt  int64
}

type hlsBufferedOrderHeap []hlsBufferedOrderFrame

func (h hlsBufferedOrderHeap) Len() int { return len(h) }

func (h hlsBufferedOrderHeap) Less(i, j int) bool {
	if h[i].orderTime == h[j].orderTime {
		return h[i].arrivedAt < h[j].arrivedAt
	}
	return h[i].orderTime < h[j].orderTime
}

func (h hlsBufferedOrderHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *hlsBufferedOrderHeap) Push(x any) {
	*h = append(*h, x.(hlsBufferedOrderFrame))
}

func (h *hlsBufferedOrderHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h hlsBufferedOrderHeap) Peek() (hlsBufferedOrderFrame, bool) {
	if len(h) == 0 {
		return hlsBufferedOrderFrame{}, false
	}
	return h[0], true
}

type hlsGOPBuffer struct {
	VideoFrameChan chan *shared.Frame
	AudioFrameChan chan *shared.Frame
	ReadyChan      chan *shared.Frame

	incoming chan *shared.Frame
	rebaser  *hlsTimelineRebaser

	outMu   sync.Mutex
	outHeap hlsBufferedOrderHeap

	pacingInit bool
	pacingPTS  time.Duration
	pacingWall time.Time
	lastEmit   time.Time

	arrivalSerial int64

	done      chan struct{}
	closeOnce sync.Once
}

func newHLSGOPBuffer() *hlsGOPBuffer {
	return &hlsGOPBuffer{
		VideoFrameChan: make(chan *shared.Frame, defaultHLSGOPReadyBuffer),
		AudioFrameChan: make(chan *shared.Frame, defaultHLSGOPReadyBuffer),
		ReadyChan:      make(chan *shared.Frame, defaultHLSGOPReadyBuffer),
		incoming:       make(chan *shared.Frame, defaultHLSGOPInputBuffer),
		rebaser:        newHLSTimelineRebaser(),
		done:           make(chan struct{}),
	}
}

func (g *hlsGOPBuffer) Close() {
	g.closeOnce.Do(func() {
		close(g.done)
	})
}

func (g *hlsGOPBuffer) IsRebase() bool {
	return true
}

func (g *hlsGOPBuffer) GetReadyChan() chan *shared.Frame {
	return g.ReadyChan
}

func (g *hlsGOPBuffer) OnSwitch() {}

func (g *hlsGOPBuffer) Run() {
	defer close(g.ReadyChan)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		g.runCollector()
	}()
	go func() {
		defer wg.Done()
		g.runScheduler()
	}()

	wg.Wait()
}

func (g *hlsGOPBuffer) runCollector() {
	videoCh := g.VideoFrameChan
	audioCh := g.AudioFrameChan

	for {
		select {
		case <-g.done:
			return
		case f, ok := <-videoCh:
			if !ok {
				videoCh = nil
				if audioCh == nil {
					return
				}
				continue
			}
			if f == nil || shouldDropStaleHLSFrame(f) {
				continue
			}
			select {
			case g.incoming <- f:
			case <-g.done:
				return
			}
		case f, ok := <-audioCh:
			if !ok {
				audioCh = nil
				if videoCh == nil {
					return
				}
				continue
			}
			if f == nil || shouldDropStaleHLSFrame(f) {
				continue
			}
			select {
			case g.incoming <- f:
			case <-g.done:
				return
			}
		}
	}
}

func (g *hlsGOPBuffer) runScheduler() {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		for {
			g.outMu.Lock()
			top, ok := g.outHeap.Peek()
			if !ok {
				g.outMu.Unlock()
				break
			}

			if !g.pacingInit {
				g.pacingInit = true
				g.pacingPTS = top.orderTime
				g.pacingWall = time.Now()
				g.lastEmit = time.Now()
			}

			dueAt := g.pacingWall.Add(top.orderTime - g.pacingPTS)
			wait := time.Until(dueAt)
			if wait > 0 {
				g.outMu.Unlock()
				break
			}

			// Live input can recover from dead sources with a burst of overdue
			// frames. Drop very late packets instead of fast-forwarding them.
			if !top.frame.IsFile && wait < -maxHLSOverdueEmitDrop {
				heap.Pop(&g.outHeap)
				g.outMu.Unlock()
				continue
			}

			popped := heap.Pop(&g.outHeap).(hlsBufferedOrderFrame)
			g.outMu.Unlock()

			if !g.emit(popped.frame) {
				return
			}
		}

		var timerC <-chan time.Time
		g.outMu.Lock()
		next, ok := g.outHeap.Peek()
		if ok && g.pacingInit {
			dueAt := g.pacingWall.Add(next.orderTime - g.pacingPTS)
			wait := time.Until(dueAt)
			if wait < 0 {
				wait = 0
			}
			if wait > time.Second && !g.lastEmit.IsZero() && time.Since(g.lastEmit) > time.Second {
				g.pacingWall = time.Now().Add(-(next.orderTime - g.pacingPTS))
				wait = 0
			}

			if timer == nil {
				timer = time.NewTimer(wait)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(wait)
			}
			timerC = timer.C
		} else {
			if timer != nil {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			timerC = nil
		}
		g.outMu.Unlock()

		select {
		case <-g.done:
			return
		case <-timerC:
			continue
		case in := <-g.incoming:
			if in == nil {
				continue
			}

			outs := g.rebaser.Process(in)
			if len(outs) == 0 {
				continue
			}

			g.outMu.Lock()
			if containsHLSDiscontinuity(outs) {
				g.outHeap = g.outHeap[:0]
				g.pacingInit = false
			}
			g.resetPacingAfterIdleLocked(outs)
			for _, f := range outs {
				if f == nil {
					continue
				}
				orderTime := f.PTS
				if isHLSVideoCodec(f.Codec) {
					orderTime = f.DTS
					if orderTime == 0 {
						orderTime = f.PTS
					}
				}
				g.arrivalSerial++
				heap.Push(&g.outHeap, hlsBufferedOrderFrame{
					frame:     f,
					orderTime: orderTime,
					arrivedAt: g.arrivalSerial,
				})
			}
			g.outMu.Unlock()
		}
	}
}

func (g *hlsGOPBuffer) resetPacingAfterIdleLocked(outs []*shared.Frame) {
	if len(outs) == 0 || g.lastEmit.IsZero() || time.Since(g.lastEmit) < maxHLSPacingIdleReset {
		return
	}

	live := false
	latestOrder := time.Duration(0)
	for _, f := range outs {
		if f == nil {
			continue
		}
		if !f.IsFile {
			live = true
		}
		orderTime := f.PTS
		if isHLSVideoCodec(f.Codec) {
			orderTime = f.DTS
			if orderTime == 0 {
				orderTime = f.PTS
			}
		}
		if orderTime > latestOrder {
			latestOrder = orderTime
		}
	}
	if !live {
		return
	}

	minKeep := latestOrder - maxHLSResumeBacklogWindow
	if minKeep < 0 {
		minKeep = 0
	}
	if len(g.outHeap) > 0 {
		trimmed := make(hlsBufferedOrderHeap, 0, len(g.outHeap))
		for _, item := range g.outHeap {
			if item.frame != nil && !item.frame.IsFile && item.orderTime < minKeep {
				continue
			}
			trimmed = append(trimmed, item)
		}
		g.outHeap = trimmed
		heap.Init(&g.outHeap)
	}
	g.pacingInit = false
}

func containsHLSDiscontinuity(frames []*shared.Frame) bool {
	for _, frame := range frames {
		if frame != nil && frame.Discontinuity {
			return true
		}
	}
	return false
}

func (g *hlsGOPBuffer) emit(f *shared.Frame) bool {
	if f == nil {
		return true
	}

	select {
	case g.ReadyChan <- f:
		g.lastEmit = time.Now()
		return true
	case <-g.done:
		return false
	}
}

type hlsTimelineRebaser struct {
	mu sync.Mutex

	activeInput  string
	pendingInput string
	switching    bool

	pendingAudio []*shared.Frame

	origVideoBasePTS time.Duration
	outVideoBasePTS  time.Duration
	haveVideoMapping bool

	origAudioBasePTS time.Duration
	outAudioBasePTS  time.Duration
	haveAudioMapping bool

	lastVideoKeySeq int64

	lastOutVideoPTS time.Duration
	lastOutAudioPTS time.Duration
	lastOutVideoDTS time.Duration
	lastOutAudioDTS time.Duration
	lastOutPTS      time.Duration

	lastVideoDur time.Duration
	lastAudioDur time.Duration

	haveLastSourceVideoPTS bool
	haveLastSourceAudioPTS bool
	lastSourceVideoPTS     time.Duration
	lastSourceAudioPTS     time.Duration

	maxSourceForwardJump time.Duration
	maxSourceBackwardGap time.Duration

	cachedH264SPSByInput map[string][]byte
	cachedH264PPSByInput map[string][]byte
}

func newHLSTimelineRebaser() *hlsTimelineRebaser {
	return &hlsTimelineRebaser{
		lastVideoDur:         33 * time.Millisecond,
		lastAudioDur:         23 * time.Millisecond,
		maxSourceForwardJump: 2 * time.Second,
		maxSourceBackwardGap: 500 * time.Millisecond,
	}
}

func (r *hlsTimelineRebaser) Process(in *shared.Frame) []*shared.Frame {
	if r == nil || in == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	f := cloneSharedFrame(in)
	if f == nil {
		return nil
	}

	isVideo := isHLSVideoCodec(f.Codec)
	isAudio := isHLSAudioCodec(f.Codec)
	if !isVideo && !isAudio {
		return []*shared.Frame{f}
	}

	if isVideo {
		r.lastVideoDur = saneHLSDuration(f, r.lastVideoDur)
		r.cacheH264ParameterSetsLocked(f.InputID, f.Codec, f.Payload, f.VideoSPS, f.VideoPPS)
	} else {
		r.lastAudioDur = saneHLSDuration(f, r.lastAudioDur)
		f.IsKeyFrame = true
	}

	origPTS := f.PTS
	origDTS := f.DTS
	if origPTS == 0 && origDTS != 0 {
		origPTS = origDTS
	}

	if r.activeInput == "" {
		r.activeInput = f.InputID
		r.switching = true
		r.pendingInput = f.InputID
		r.pendingAudio = r.pendingAudio[:0]
	}

	if f.InputID != r.activeInput {
		if !r.switching || r.pendingInput != f.InputID {
			r.switching = true
			r.pendingInput = f.InputID
			r.pendingAudio = r.pendingAudio[:0]
		}
	}

	if r.switching {
		if isAudio && f.InputID == r.pendingInput {
			if len(r.pendingAudio) < maxHLSBufferedSwitchAudio {
				r.pendingAudio = append(r.pendingAudio, f)
				r.trimPendingAudioByPTSLocked()
			}
			return nil
		}

		if isVideo {
			if f.InputID != r.pendingInput {
				return nil
			}
			if !r.canCommitSwitchLocked(f) {
				return nil
			}

			r.activeInput = r.pendingInput
			r.pendingInput = ""
			r.switching = false
			r.origVideoBasePTS = origPTS

			step := r.lastVideoDur
			if step <= 0 {
				step = 33 * time.Millisecond
			}
			if r.lastOutVideoPTS > 0 {
				r.outVideoBasePTS = r.lastOutVideoPTS + step
			} else {
				r.outVideoBasePTS = 0
			}
			r.haveVideoMapping = true
			r.haveAudioMapping = false
			r.haveLastSourceVideoPTS = false
			r.haveLastSourceAudioPTS = false
			r.lastVideoKeySeq = 0

			out := make([]*shared.Frame, 0, 1+len(r.pendingAudio))
			keyframe := r.rebaseOneLocked(f)
			if keyframe != nil && keyframe.InputID != "" && r.lastOutPTS > 0 {
				keyframe.Discontinuity = true
			}
			out = append(out, keyframe)
			for _, af := range r.pendingAudio {
				if af == nil {
					continue
				}
				audioOrigPTS := af.PTS
				if audioOrigPTS == 0 && af.DTS != 0 {
					audioOrigPTS = af.DTS
				}
				// Keep only audio at/after the committed cut point.
				if audioOrigPTS < r.origVideoBasePTS {
					continue
				}
				out = append(out, r.rebaseOneLocked(af))
			}
			r.pendingAudio = r.pendingAudio[:0]
			return out
		}

		return nil
	}

	if f.InputID != r.activeInput {
		return nil
	}

	return []*shared.Frame{r.rebaseOneLocked(f)}
}

func (r *hlsTimelineRebaser) canCommitSwitchLocked(f *shared.Frame) bool {
	if f == nil || !f.IsKeyFrame || !isHLSVideoCodec(f.Codec) {
		return false
	}

	if f.Codec != "h264" {
		return true
	}

	sps, pps := r.h264ParameterSetsForInputLocked(f.InputID)
	f.Payload = h264EnsureSPSPPSOnKeyFrame(f.Payload, true, sps, pps)
	hasSPS, hasPPS := h264SPSPPSPresent(f.Payload)
	return hasSPS && hasPPS
}

func (r *hlsTimelineRebaser) cacheH264ParameterSetsLocked(inputID, codec string, nalus [][]byte, frameSPS, framePPS []byte) {
	if codec != "h264" {
		return
	}
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return
	}
	if r.cachedH264SPSByInput == nil {
		r.cachedH264SPSByInput = make(map[string][]byte)
	}
	if r.cachedH264PPSByInput == nil {
		r.cachedH264PPSByInput = make(map[string][]byte)
	}
	sps, pps := h264ExtractSPSPPS(nalus)
	if len(frameSPS) > 0 {
		sps = cloneBytes(frameSPS)
	}
	if len(framePPS) > 0 {
		pps = cloneBytes(framePPS)
	}
	if len(sps) > 0 {
		r.cachedH264SPSByInput[inputID] = sps
	}
	if len(pps) > 0 {
		r.cachedH264PPSByInput[inputID] = pps
	}
}

func (r *hlsTimelineRebaser) h264ParameterSetsForInputLocked(inputID string) ([]byte, []byte) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return nil, nil
	}
	return cloneBytes(r.cachedH264SPSByInput[inputID]), cloneBytes(r.cachedH264PPSByInput[inputID])
}

func (r *hlsTimelineRebaser) rebaseOneLocked(f *shared.Frame) *shared.Frame {
	if r == nil || f == nil || !r.haveVideoMapping {
		return f
	}

	isVideo := isHLSVideoCodec(f.Codec)
	isAudio := isHLSAudioCodec(f.Codec)

	origPTS := f.PTS
	origDTS := f.DTS
	if origPTS == 0 && origDTS != 0 {
		origPTS = origDTS
	}

	var outPTS time.Duration
	if isVideo {
		if r.shouldReanchorLocked(true, false, origPTS) {
			step := r.lastVideoDur
			if step <= 0 {
				step = 33 * time.Millisecond
			}

			r.origVideoBasePTS = origPTS
			if r.lastOutVideoPTS > 0 {
				r.outVideoBasePTS = r.lastOutVideoPTS + step
			} else {
				r.outVideoBasePTS = 0
			}
			// The next audio frame should establish a fresh audio mapping relative
			// to the new video anchor instead of inheriting the previous source base.
			r.haveAudioMapping = false
		}

		delta := origPTS - r.origVideoBasePTS
		if delta < 0 {
			delta = 0
		}
		outPTS = r.outVideoBasePTS + delta
		r.lastOutVideoPTS = outPTS
	} else if isAudio {
		if !r.haveAudioMapping {
			r.establishAudioMappingLocked(origPTS)
		} else if r.shouldReanchorLocked(false, true, origPTS) {
			step := r.lastAudioDur
			if step <= 0 {
				step = 23 * time.Millisecond
			}
			r.origAudioBasePTS = origPTS
			if r.lastOutAudioPTS > 0 {
				r.outAudioBasePTS = r.lastOutAudioPTS + step
			} else {
				r.establishAudioMappingLocked(origPTS)
			}
		}

		delta := origPTS - r.origAudioBasePTS
		if delta < 0 {
			delta = 0
		}
		outPTS = r.outAudioBasePTS + delta
		minStep := saneHLSDuration(f, r.lastAudioDur)
		if outPTS < r.lastOutAudioPTS {
			outPTS = r.lastOutAudioPTS + minStep
		}
		r.lastOutAudioPTS = outPTS
	}

	ptsDtsDelta := f.PTS - f.DTS
	outDTS := outPTS - ptsDtsDelta
	if outDTS < 0 {
		outDTS = 0
	}
	if outDTS > outPTS {
		outDTS = outPTS
	}

	if isVideo {
		minStep := saneHLSDuration(f, r.lastVideoDur)
		if outDTS < r.lastOutVideoDTS {
			outDTS = r.lastOutVideoDTS + minStep
			if outDTS > outPTS {
				outDTS = outPTS
			}
		}
		r.lastOutVideoDTS = outDTS
	} else if isAudio {
		minStep := saneHLSDuration(f, r.lastAudioDur)
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

	if isVideo {
		if f.IsKeyFrame {
			r.lastVideoKeySeq = f.SequenceID
			f.GOPID = f.SequenceID
		} else if r.lastVideoKeySeq != 0 {
			f.GOPID = r.lastVideoKeySeq
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

func (r *hlsTimelineRebaser) establishAudioMappingLocked(origPTS time.Duration) {
	r.origAudioBasePTS = origPTS

	step := r.lastAudioDur
	if step <= 0 {
		step = 23 * time.Millisecond
	}

	desired := time.Duration(0)
	if r.haveVideoMapping {
		offset := origPTS - r.origVideoBasePTS
		if offset < 0 {
			offset = 0
		}
		desired = r.outVideoBasePTS + offset
	} else if r.lastOutAudioPTS > 0 {
		desired = r.lastOutAudioPTS + step
	} else {
		desired = 0
	}

	if r.lastOutAudioPTS > 0 {
		min := r.lastOutAudioPTS + step
		if desired < min {
			desired = min
		}

		// Smooth switch and re-anchor corrections so audio continuity is kept
		// within roughly one video frame budget instead of jumping by the
		// full source-side A/V offset in a single packet.
		max := r.lastOutAudioPTS + 4*step
		if desired > max {
			desired = max
		}
	}

	r.outAudioBasePTS = desired
	r.haveAudioMapping = true
}

func (r *hlsTimelineRebaser) shouldReanchorLocked(isVideo, isAudio bool, origPTS time.Duration) bool {
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

func (r *hlsTimelineRebaser) updateLastSourcePTSLocked(isVideo, isAudio bool, origPTS time.Duration) {
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

func (r *hlsTimelineRebaser) trimPendingAudioByPTSLocked() {
	if len(r.pendingAudio) < 2 {
		return
	}

	latest := sourcePTSOrDTS(r.pendingAudio[len(r.pendingAudio)-1])
	if latest <= 0 {
		return
	}
	minPTS := latest - maxHLSPendingAudioWindow

	keepFrom := 0
	for i, af := range r.pendingAudio {
		pts := sourcePTSOrDTS(af)
		if pts <= 0 || pts >= minPTS {
			keepFrom = i
			break
		}
	}
	if keepFrom > 0 {
		r.pendingAudio = append(r.pendingAudio[:0], r.pendingAudio[keepFrom:]...)
	}
}

func sourcePTSOrDTS(f *shared.Frame) time.Duration {
	if f == nil {
		return 0
	}
	if f.PTS != 0 {
		return f.PTS
	}
	return f.DTS
}

func saneHLSDuration(f *shared.Frame, fallback time.Duration) time.Duration {
	if f == nil {
		return fallback
	}
	if f.Duration <= 0 || f.Duration > 500*time.Millisecond {
		return fallback
	}
	return f.Duration
}

func isHLSVideoCodec(codec string) bool {
	return codec == "h264" || codec == "h265"
}

func isHLSAudioCodec(codec string) bool {
	return codec == "aac" || codec == "opus"
}

func shouldDropStaleHLSFrame(f *shared.Frame) bool {
	if f == nil || f.Timestamp.IsZero() || f.IsFile {
		return false
	}
	return time.Since(f.Timestamp) > 1500*time.Millisecond
}
