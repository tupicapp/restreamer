package filters

import (
	"container/heap"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"restreamer/core/shared"
	"strings"
	"sync"
	"time"
)

type GOPBuffer struct {
	// Input channels
	VideoFrameChan chan *shared.Frame
	AudioFrameChan chan *shared.Frame

	// Output channels
	VideoReadyChan chan *shared.Frame // Channel to read ready video frames
	AudioReadyChan chan *shared.Frame // Channel to read ready audio frames

	RateControl bool
	GenPTS      bool
	PTSFilter   bool

	rebase    bool
	dropStale bool

	incoming chan *shared.Frame
	rebaser  *TimelineRebaser

	outMu   sync.Mutex
	outHeap rebasedHeap

	// pacing state (shared across audio+video)
	pacingInit bool
	pacingPTS  time.Duration
	pacingWall time.Time
	lastEmit   time.Time

	done      chan struct{}
	closeOnce sync.Once
}

func NewGOPBuffer(RateControl bool, GenPTS bool, ptsFilter bool) *GOPBuffer {
	return NewGOPBufferWithOptions(RateControl, GenPTS, ptsFilter, true, true)
}

func NewGOPBufferWithOptions(RateControl bool, GenPTS bool, ptsFilter bool, rebase bool, dropStale bool) *GOPBuffer {
	g := &GOPBuffer{
		VideoFrameChan: make(chan *shared.Frame, 1024),
		AudioFrameChan: make(chan *shared.Frame, 1024),
		VideoReadyChan: make(chan *shared.Frame, 1024),
		AudioReadyChan: make(chan *shared.Frame, 1024),
		incoming:       make(chan *shared.Frame, 4096),
		done:           make(chan struct{}),
		RateControl:    RateControl,
		GenPTS:         GenPTS,
		PTSFilter:      ptsFilter,
		rebase:         rebase,
		dropStale:      dropStale,
		rebaser:        NewTimelineRebaser(),
	}

	return g
}

func (g *GOPBuffer) Close() {
	g.closeOnce.Do(func() {
		close(g.done)
	})
}

// GetVideoReadyChan returns the channel to read ready video frames from
func (g *GOPBuffer) GetVideoReadyChan() chan *shared.Frame {
	return g.VideoReadyChan
}

// GetAudioReadyChan returns the channel to read ready audio frames from
func (g *GOPBuffer) GetAudioReadyChan() chan *shared.Frame {
	return g.AudioReadyChan
}

// GetAudioReadyChan returns the channel to read ready audio frames from
func (g *GOPBuffer) IsRebase() bool {
	return g.rebase
}

func (g *GOPBuffer) Run() {
	defer close(g.VideoReadyChan)
	defer close(g.AudioReadyChan)

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

func (g *GOPBuffer) runCollector() {
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
			if f == nil {
				continue
			}

			if g.dropStale && !f.Timestamp.IsZero() && !f.IsFile && time.Since(f.Timestamp) > 1500*time.Millisecond {
				// fmt.Println("gop : reciving video :  ", f.InputID, f.PTS, g.dropStale, f.Timestamp.UnixMilli(), time.Since(f.Timestamp))

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
			if f == nil {
				continue
			}

			if g.dropStale && !f.Timestamp.IsZero() && !f.IsFile && time.Since(f.Timestamp) > 1500*time.Millisecond {
				continue
			}

			// fmt.Println("gop : reciving audio :  ", f.InputID, f.PTS)

			select {
			case g.incoming <- f:
			case <-g.done:
				return
			}
		}
	}
}

func (g *GOPBuffer) runScheduler() {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		// Emit everything that's due right now.
		for {
			g.outMu.Lock()
			top, ok := g.outHeap.Peek()
			if !ok {
				g.outMu.Unlock()
				break
			}

			// Initialize pacing on the first output frame.
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

			popped := heap.Pop(&g.outHeap).(rebasedFrame)
			g.outMu.Unlock()

			if !g.emit(popped.f) {
				return
			}
		}

		// Compute the next wake-up (or block on input if nothing queued).
		var timerC <-chan time.Time
		g.outMu.Lock()
		next, ok := g.outHeap.Peek()
		if ok && g.pacingInit {
			dueAt := g.pacingWall.Add(next.orderTime - g.pacingPTS)
			wait := time.Until(dueAt)
			if wait < 0 {
				wait = 0
			}

			// Safety: if pacing would sleep too long (usually caused by timestamp jumps),
			// resync pacing to "now" so the stream doesn't appear stuck.
			if wait > 1*time.Second && !g.lastEmit.IsZero() && time.Since(g.lastEmit) > 1*time.Second {
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
			// No queued frames (or not initialized): stop timer and block on input.
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
			// Time to try emitting due frames.
			continue
		case in := <-g.incoming:
			if in == nil {
				continue
			}

			var outs []*shared.Frame
			if g.rebase {
				outs = g.rebaser.Process(in)
			} else {
				outs = []*shared.Frame{cloneFrame(in)}
			}
			if len(outs) == 0 {
				continue
			}

			g.outMu.Lock()
			for _, f := range outs {
				if f == nil {
					continue
				}
				orderTime := f.PTS
				if isVideoCodec(f.Codec) {
					orderTime = f.DTS
					if orderTime == 0 {
						orderTime = f.PTS
					}
				}
				heap.Push(&g.outHeap, rebasedFrame{f: f, orderTime: orderTime})
			}
			g.outMu.Unlock()
		}
	}
}

func (g *GOPBuffer) emit(f *shared.Frame) bool {
	if f == nil {
		return true
	}

	if isVideoCodec(f.Codec) {
		select {
		case g.VideoReadyChan <- f:
			g.lastEmit = time.Now()
			return true
		case <-g.done:
			return false
		}
	}

	if isAudioCodec(f.Codec) {
		select {
		case g.AudioReadyChan <- f:
			g.lastEmit = time.Now()
			return true
		case <-g.done:
			return false
		}
	}

	return true
}

func classifyH265PacketType(au [][]byte) string {
	for _, nalu := range au {
		if len(nalu) < 2 {
			continue
		}
		nalType := (nalu[0] >> 1) & 0x3F
		if nalType >= 16 && nalType <= 21 {
			return "I"
		}
		if nalType <= 31 {
			return "P"
		}
	}
	return "unknown"
}

func parseH264SliceType(nalu []byte) (int, error) {
	if len(nalu) <= 1 {
		return 0, io.EOF
	}

	rbsp := removeEmulationPreventionBytes(nalu[1:])
	if len(rbsp) == 0 {
		return 0, io.EOF
	}

	br := newBitReader(rbsp)
	if _, err := br.readUE(); err != nil {
		return 0, err
	}

	sliceType, err := br.readUE()
	if err != nil {
		return 0, err
	}

	return int(sliceType), nil
}

func mapH264SliceType(sliceType int) string {
	switch sliceType {
	case 0, 5:
		return "P"
	case 1, 6:
		return "B"
	case 2, 7:
		return "I"
	case 3, 8:
		return "SP"
	case 4, 9:
		return "SI"
	default:
		return "unknown"
	}
}

func removeEmulationPreventionBytes(data []byte) []byte {
	if len(data) < 3 {
		return append([]byte(nil), data...)
	}

	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if i+2 < len(data) && data[i] == 0x00 && data[i+1] == 0x00 && data[i+2] == 0x03 {
			out = append(out, 0x00, 0x00)
			i += 2
			continue
		}
		out = append(out, data[i])
	}

	return out
}

type bitReader struct {
	data    []byte
	bytePos int
	bitPos  uint8
}

func newBitReader(data []byte) *bitReader {
	return &bitReader{data: data}
}

func (b *bitReader) readBits(n int) (uint32, error) {
	var value uint32
	for i := 0; i < n; i++ {
		if b.bytePos >= len(b.data) {
			return 0, io.EOF
		}
		bit := (b.data[b.bytePos] >> (7 - b.bitPos)) & 0x1
		value = (value << 1) | uint32(bit)
		b.bitPos++
		if b.bitPos == 8 {
			b.bitPos = 0
			b.bytePos++
		}
	}
	return value, nil
}

func (b *bitReader) readUE() (uint32, error) {
	leadingZeroBits := 0
	for {
		bit, err := b.readBits(1)
		if err != nil {
			return 0, err
		}
		if bit == 0 {
			leadingZeroBits++
			continue
		}
		break
	}

	if leadingZeroBits == 0 {
		return 0, nil
	}

	suffix, err := b.readBits(leadingZeroBits)
	if err != nil {
		return 0, err
	}

	return ((1 << leadingZeroBits) - 1) + suffix, nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func normalizeHLSURI(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("empty uri")
	}

	u, err := url.Parse(trimmed)
	if err == nil && u.Scheme != "" {
		return trimmed, nil
	}

	absPath, err := filepath.Abs(trimmed)
	if err != nil {
		return "", err
	}

	fileURL := &url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(absPath),
	}

	return fileURL.String(), nil
}

func AddSPStoKeyFrame(frame *shared.Frame) {
	if frame == nil {
		return
	}

	spsPps := []byte{}

	payload := frame.Payload
	if frame.IsKeyFrame && len(spsPps) > 0 {
		payload = append([][]byte{spsPps}, payload...)
	} else if frame.IsKeyFrame {
		// Extract SPS/PPS from the keyframe if we haven't yet
		for _, nalu := range frame.Payload {
			if len(nalu) == 0 {
				continue
			}
			typ := nalu[0] & 0x1F
			if typ == 7 || typ == 8 { // SPS=7, PPS=8
				spsPps = append(spsPps, prependStartCode(nalu)...)
			}
		}
	}

	frame.Payload = payload
}

func IsTsKeyFrame(frame *shared.Frame) bool {
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

// NormalizeHLSURI exposes HLS URI normalization for moved-layout packages.
func NormalizeHLSURI(raw string) (string, error) {
	return normalizeHLSURI(raw)
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

func H264SPSPPSPresent(nalus [][]byte) (bool, bool) {
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

func prependStartCode(nalu []byte) []byte {
	startCode := []byte{0x00, 0x00, 0x00, 0x01}
	return append(startCode, nalu...)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
