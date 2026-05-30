package inputs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
	pkgrtmp "github.com/tupicapp/restreamer/pkg/rtmp"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/gortsplib/v5/pkg/ringbuffer"
	"go.uber.org/zap"
)

type MediaPacketValidator interface {
	ValidateTracks([]*gortmplib.Track) error
	ObserveVideoPacket(pts, dts time.Duration, au [][]byte) error
	ObserveAudioPacket(pts time.Duration, au []byte) error
}

// RTMPInput receives an incoming RTMP publish connection and converts it to streamer frames.
type RTMPInput struct {
	id   string
	url  string
	conn *gortmplib.ServerConn

	watchersMu sync.RWMutex
	watchers   []Stream

	onCloseMu sync.RWMutex
	onClose   []func()

	videoChan chan *Frame
	audioChan chan *Frame
	videoRing *ringbuffer.RingBuffer
	audioRing *ringbuffer.RingBuffer
	audioMu   sync.RWMutex
	videoMu   sync.RWMutex

	IsStarted   bool
	IsInitiated bool

	TotalVideoFrames   int64
	TotalAudioFrames   int64
	DroppedVideoFrames int64
	DroppedAudioFrames int64
	LastIO             time.Time
	RunnerDetails      string

	AudioFps float64
	VideoFps float64

	lastPTS time.Duration

	lastVideoPacket *Frame
	lastAudioPacket *Frame

	VideoSequenceID int64
	AudioSequenceID int64

	lastVideoGOPID int64
	lastAudioGOPID int64

	sequenceIDMu sync.Mutex
	gopMu        sync.RWMutex
	h264ParamMu  sync.RWMutex
	cachedSPS    []byte
	cachedPPS    []byte
	audioRateMu  sync.RWMutex
	audioRateHz  int
	closeInfoMu  sync.RWMutex
	closeInfo    pkgrtmp.SessionCloseInfo
	validatorMu  sync.RWMutex
	validator    MediaPacketValidator

	done    chan struct{}
	started chan struct{}
	closeCh chan struct{}

	closeOnce sync.Once
	events    *shared.EventEmitter
}

func NewRTMPInput(id, rawurl string, conn *gortmplib.ServerConn, watchers []Stream) *RTMPInput {
	s := &RTMPInput{
		id:        id,
		url:       rawurl,
		conn:      conn,
		watchers:  watchers,
		videoChan: make(chan *Frame, 100),
		audioChan: make(chan *Frame, 100),
		done:      make(chan struct{}),
		started:   make(chan struct{}),
		closeCh:   make(chan struct{}),
		events:    shared.NewEventEmitter(128),
	}

	videoRing, err := ringbuffer.New(128)
	if err != nil {
		panic(fmt.Errorf("cannot create video ring buffer: %w", err))
	}
	audioRing, err := ringbuffer.New(128)
	if err != nil {
		panic(fmt.Errorf("cannot create audio ring buffer: %w", err))
	}

	s.videoRing = videoRing
	s.audioRing = audioRing

	go s.forwardVideo()
	go s.forwardAudio()

	return s
}

func (s *RTMPInput) Clone() (Stream, error) {
	return nil, errors.New("rtmp input cannot be cloned")
}

func (s *RTMPInput) SetOnClose(fn func()) {
	if fn == nil {
		return
	}

	s.onCloseMu.Lock()
	defer s.onCloseMu.Unlock()
	s.onClose = append(s.onClose, fn)
}

func (s *RTMPInput) SetMediaPacketValidator(validator pkgrtmp.MediaPacketValidator) {
	s.validatorMu.Lock()
	defer s.validatorMu.Unlock()
	s.validator = validator
}

func (s *RTMPInput) CloseInfo() pkgrtmp.SessionCloseInfo {
	s.closeInfoMu.RLock()
	defer s.closeInfoMu.RUnlock()
	return s.closeInfo
}

func (s *RTMPInput) Start() {
	if s.IsInitiated {
		s.IsStarted = true
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: s.id, StreamType: s.Type(), Message: "rtmp publish input resumed", Meta: shared.StreamLifecycleMeta{URL: s.url}})
		return
	}

	s.IsInitiated = true
	s.IsStarted = true
	s.RunnerDetails = "rtmp input loop"

	getLogger().Info("rtmp input: started",
		zap.String("stream_id", s.id),
		zap.String("url", s.url))
	s.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: s.id, StreamType: s.Type(), Message: "rtmp publish input started", Meta: shared.StreamLifecycleMeta{URL: s.url}})

	s.watchersMu.RLock()
	for _, w := range s.watchers {
		if w == nil {
			continue
		}
		w.Start()
	}
	s.watchersMu.RUnlock()

	go s.run()
}

func (s *RTMPInput) WaitForStart(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")
		case <-s.done:
			return errors.New("stream is closed")
		case <-s.started:
			return nil
		}
	}
}

func (s *RTMPInput) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *RTMPInput) run() {
	defer close(s.closeCh)
	defer s.Close()

	r := &gortmplib.Reader{Conn: s.conn}
	if err := r.Initialize(); err != nil {
		getLogger().Error("rtmp input: reader init failed",
			zap.String("stream_id", s.id),
			zap.String("url", s.url),
			zap.Error(err))
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: s.id, StreamType: s.Type(), Message: "rtmp publish input reader init failed", Error: err})
		s.setCloseInfo("", err)
		return
	}

	if err := s.initTracks(r); err != nil {
		var rejectErr *pkgrtmp.RejectError
		if errors.As(err, &rejectErr) {
			_ = pkgrtmp.WriteStatusError(s.conn, rejectErr.Code, rejectErr.Description)
			s.setCloseInfo(rejectErr.Reason, rejectErr)
		} else {
			s.setCloseInfo("track_init_failed", err)
		}
		return
	}
	close(s.started)

	for {
		select {
		case <-s.done:
			return
		default:
			if !s.IsStarted {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			if err := r.Read(); err != nil {
				var rejectErr *pkgrtmp.RejectError
				if errors.As(err, &rejectErr) {
					_ = pkgrtmp.WriteStatusError(s.conn, rejectErr.Code, rejectErr.Description)
					s.setCloseInfo(rejectErr.Reason, rejectErr)
					s.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: s.id, StreamType: s.Type(), Message: "rtmp publish input rejected", Error: rejectErr})
				} else if isExpectedRTMPConnClose(err) {
					s.setCloseInfo("publisher_closed", nil)
					s.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: s.id, StreamType: s.Type(), Message: "publisher connection closed"})
				} else {
					s.setCloseInfo("publisher_read_failed", err)
					s.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: s.id, StreamType: s.Type(), Message: "rtmp publish input read failed", Error: err})
				}
				getLogger().Info("rtmp input: connection closed",
					zap.String("stream_id", s.id),
					zap.String("url", s.url),
					zap.Error(err))
				return
			}
		}
	}
}

func isExpectedRTMPConnClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "use of closed network connection")
}

func (s *RTMPInput) bufferVideoPacket(pts time.Duration, dts time.Duration, au [][]byte) {
	if err := s.observeVideoPacket(pts, dts, au); err != nil {
		s.rejectConnection(err)
		return
	}

	s.updateH264ParameterSets(au)

	keyFrame := false
	for _, nalu := range au {
		if h264NALTypeFromUnit(nalu) == 5 {
			keyFrame = true
			break
		}
	}
	if keyFrame {
		sps, pps := s.getH264ParameterSets()
		au = h264EnsureSPSPPSOnKeyFrame(au, true, sps, pps)
		s.updateH264ParameterSets(au)
	}

	s.sequenceIDMu.Lock()
	s.VideoSequenceID++
	sequenceID := s.VideoSequenceID
	s.sequenceIDMu.Unlock()

	prevPTS := time.Duration(0)
	if s.lastVideoPacket != nil {
		prevPTS = s.lastVideoPacket.PTS
	}

	duration := time.Duration(0)
	if prevPTS != 0 && pts >= prevPTS {
		duration = pts - prevPTS
	}

	f := &Frame{
		PTS:        pts,
		DTS:        dts,
		Duration:   duration,
		Payload:    au,
		Codec:      "h264",
		PacketType: classifyVideoPacketType(au, "h264"),
		Timestamp:  time.Now(),
		InputID:    s.id,
		SequenceID: sequenceID,
	}
	f.VideoSPS, f.VideoPPS = s.getH264ParameterSets()

	f.IsKeyFrame = keyFrame

	s.gopMu.Lock()
	if f.IsKeyFrame {
		s.lastVideoGOPID = sequenceID
	}
	f.GOPID = s.lastVideoGOPID
	s.gopMu.Unlock()

	s.LastIO = time.Now()
	s.lastVideoPacket = f
	s.lastPTS = pts
	s.TotalVideoFrames++

	if !s.videoRing.Push(f) {
		s.DroppedVideoFrames++
	}
}

func (s *RTMPInput) bufferAudioPacket(pts time.Duration, au []byte) {
	if err := s.observeAudioPacket(pts, au); err != nil {
		s.rejectConnection(err)
		return
	}

	s.sequenceIDMu.Lock()
	s.AudioSequenceID++
	sequenceID := s.AudioSequenceID
	s.sequenceIDMu.Unlock()

	prevPTS := time.Duration(0)
	if s.lastAudioPacket != nil {
		prevPTS = s.lastAudioPacket.PTS
	}

	duration := time.Duration(0)
	if prevPTS != 0 && pts >= prevPTS {
		duration = pts - prevPTS
	}

	f := &Frame{
		PTS:        pts,
		DTS:        pts,
		Duration:   duration,
		Payload:    [][]byte{au},
		Codec:      "aac",
		Timestamp:  time.Now(),
		InputID:    s.id,
		IsKeyFrame: true,
		SequenceID: sequenceID,
		SampleRate: s.audioSampleRate(),
	}

	s.gopMu.Lock()
	s.lastAudioGOPID = sequenceID
	f.GOPID = s.lastAudioGOPID
	s.gopMu.Unlock()

	s.lastAudioPacket = f
	s.lastPTS = pts
	s.LastIO = time.Now()
	s.TotalAudioFrames++

	if !s.audioRing.Push(f) {
		s.DroppedAudioFrames++
	}
}

func (s *RTMPInput) initTracks(r *gortmplib.Reader) error {
	validator := s.getMediaPacketValidator()
	if validator != nil {
		if err := validator.ValidateTracks(r.Tracks()); err != nil {
			return err
		}
	}

	for _, track := range r.Tracks() {
		if track == nil || track.Codec == nil {
			continue
		}

		switch codec := track.Codec.(type) {
		case *codecs.H264:
			s.setH264ParameterSets(codec.SPS, codec.PPS)
			r.OnDataH264(track, func(pts, dts time.Duration, au [][]byte) {
				s.bufferVideoPacket(pts, dts, au)
			})
		case *codecs.MPEG4Audio:
			s.setAudioSampleRate(codec.Config.SampleRate)
			r.OnDataMPEG4Audio(track, func(pts time.Duration, au []byte) {
				s.bufferAudioPacket(pts, au)
			})
		}
	}

	return nil
}

func (s *RTMPInput) forwardVideo() {
	defer close(s.videoChan)
	for {
		data, ok := s.videoRing.Pull()
		if !ok {
			return
		}

		frame := data.(*Frame)
		select {
		case s.videoChan <- frame:
		case <-time.After(frame.Duration):
		case <-s.done:
			return
		}

		s.multicastVideoToWatchers(frame)
	}
}

func (s *RTMPInput) forwardAudio() {
	defer close(s.audioChan)
	for {
		data, ok := s.audioRing.Pull()
		if !ok {
			return
		}

		frame := data.(*Frame)
		select {
		case s.audioChan <- frame:
		case <-time.After(frame.Duration):
		case <-s.done:
			return
		}

		s.multicastAudioToWatchers(frame)
	}
}

func (s *RTMPInput) Stop() {
	s.IsStarted = false
	s.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: s.id, StreamType: s.Type(), Message: "rtmp publish input stopped"})
}

func (s *RTMPInput) Close() {
	s.Stop()
	var onClose []func()
	s.closeOnce.Do(func() {
		close(s.done)
		s.closeUnderlyingConn()
		if s.videoRing != nil {
			s.videoRing.Close()
		}
		if s.audioRing != nil {
			s.audioRing.Close()
		}

		s.onCloseMu.RLock()
		onClose = append([]func(){}, s.onClose...)
		s.onCloseMu.RUnlock()

		s.watchersMu.RLock()
		watchers := append([]Stream(nil), s.watchers...)
		s.watchersMu.RUnlock()
		for _, w := range watchers {
			if w == nil {
				continue
			}
			w.Close()
		}
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: s.id, StreamType: s.Type(), Message: "rtmp publish input closed"})
		s.events.Close()
	})
	for _, fn := range onClose {
		if fn != nil {
			fn()
		}
	}
}

func (s *RTMPInput) Done() <-chan struct{} { return s.closeCh }

func (s *RTMPInput) IsKeyFrame(frame *Frame) bool {
	if frame == nil {
		return false
	}

	for _, nalu := range frame.Payload {
		if len(nalu) == 0 {
			continue
		}
		nal := nalu[0] & 0x1F
		if nal == 5 {
			return true
		}
	}

	return false
}

func (s *RTMPInput) setH264ParameterSets(sps, pps []byte) {
	s.h264ParamMu.Lock()
	defer s.h264ParamMu.Unlock()

	if len(sps) > 0 {
		s.cachedSPS = cloneBytes(sps)
	}
	if len(pps) > 0 {
		s.cachedPPS = cloneBytes(pps)
	}
}

func (s *RTMPInput) setAudioSampleRate(rate int) {
	if rate <= 0 {
		return
	}
	s.audioRateMu.Lock()
	s.audioRateHz = rate
	s.audioRateMu.Unlock()
}

func (s *RTMPInput) audioSampleRate() int {
	s.audioRateMu.RLock()
	defer s.audioRateMu.RUnlock()
	if s.audioRateHz > 0 {
		return s.audioRateHz
	}
	return DefaultAudioRate
}

func (s *RTMPInput) updateH264ParameterSets(nalus [][]byte) {
	sps, pps := h264ExtractSPSPPS(nalus)
	s.h264ParamMu.Lock()
	defer s.h264ParamMu.Unlock()

	if len(sps) > 0 {
		s.cachedSPS = sps
	}
	if len(pps) > 0 {
		s.cachedPPS = pps
	}
}

func (s *RTMPInput) getH264ParameterSets() ([]byte, []byte) {
	s.h264ParamMu.RLock()
	defer s.h264ParamMu.RUnlock()
	return cloneBytes(s.cachedSPS), cloneBytes(s.cachedPPS)
}

func (s *RTMPInput) GetVideoChan() chan *Frame { return s.videoChan }
func (s *RTMPInput) GetAudioChan() chan *Frame { return s.audioChan }
func (s *RTMPInput) GetID() string             { return s.id }
func (s *RTMPInput) Type() string              { return "rtmp_input" }
func (s *RTMPInput) IsRestartable() bool       { return false }
func (s *RTMPInput) RestartInterval() time.Duration {
	return 0
}

func (s *RTMPInput) State() *State {
	return &State{
		LastIO:             s.LastIO,
		IsStarted:          s.IsStarted,
		StreamID:           s.id,
		Url:                s.url,
		Type:               s.Type(),
		DroppedAudioFrames: float64(s.DroppedAudioFrames),
		DroppedVideoFrames: float64(s.DroppedVideoFrames),
		TotalVideoFrames:   s.TotalVideoFrames,
		TotalAudioFrames:   s.TotalAudioFrames,
		AudioFps:           s.AudioFps,
		VideoFps:           s.VideoFps,
	}
}

func (s *RTMPInput) multicastVideoToWatchers(frame *Frame) {
	if frame == nil {
		return
	}

	logger := getLogger()
	wg := sync.WaitGroup{}

	s.watchersMu.RLock()
	watchers := append([]Stream(nil), s.watchers...)
	s.watchersMu.RUnlock()
	if len(watchers) == 0 {
		return
	}

	for _, out := range watchers {
		if out == nil {
			continue
		}

		wg.Add(1)
		go func(w Stream, f *Frame) {
			defer wg.Done()
			cloned := cloneFrameForWatcher(f)
			select {
			case w.GetVideoChan() <- cloned:
			case <-time.After(1000 * time.Millisecond):
				logger.Warn("rtmp input: watcher dropped video frame",
					zap.String("watcher_id", w.GetID()),
					zap.Int64("sequence_id", f.SequenceID),
					zap.Duration("pts", f.PTS),
					zap.String("input_id", f.InputID),
					zap.Bool("is_keyframe", f.IsKeyFrame))
			case <-s.done:
			}
		}(out, frame)
	}

	wg.Wait()
}

func (s *RTMPInput) getMediaPacketValidator() pkgrtmp.MediaPacketValidator {
	s.validatorMu.RLock()
	defer s.validatorMu.RUnlock()
	return s.validator
}

func (s *RTMPInput) observeVideoPacket(pts, dts time.Duration, au [][]byte) error {
	validator := s.getMediaPacketValidator()
	if validator == nil {
		return nil
	}
	return validator.ObserveVideoPacket(pts, dts, au)
}

func (s *RTMPInput) observeAudioPacket(pts time.Duration, au []byte) error {
	validator := s.getMediaPacketValidator()
	if validator == nil {
		return nil
	}
	return validator.ObserveAudioPacket(pts, au)
}

func (s *RTMPInput) rejectConnection(err error) {
	var rejectErr *pkgrtmp.RejectError
	if errors.As(err, &rejectErr) {
		_ = pkgrtmp.WriteStatusError(s.conn, rejectErr.Code, rejectErr.Description)
		s.setCloseInfo(rejectErr.Reason, rejectErr)
	} else {
		s.setCloseInfo("publisher_read_failed", err)
	}
	s.Close()
}

func (s *RTMPInput) setCloseInfo(reason string, err error) {
	s.closeInfoMu.Lock()
	defer s.closeInfoMu.Unlock()
	if s.closeInfo.Err != nil {
		return
	}
	s.closeInfo = pkgrtmp.SessionCloseInfo{
		Reason: reason,
		Err:    err,
	}
}

func (s *RTMPInput) closeUnderlyingConn() {
	if s.conn == nil || s.conn.RW == nil {
		return
	}

	if closer, ok := s.conn.RW.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

func (s *RTMPInput) multicastAudioToWatchers(frame *Frame) {
	if frame == nil {
		return
	}

	logger := getLogger()
	wg := sync.WaitGroup{}

	s.watchersMu.RLock()
	watchers := append([]Stream(nil), s.watchers...)
	s.watchersMu.RUnlock()
	if len(watchers) == 0 {
		return
	}

	for _, out := range watchers {
		if out == nil {
			continue
		}

		wg.Add(1)
		go func(w Stream, f *Frame) {
			defer wg.Done()
			cloned := cloneFrameForWatcher(f)
			select {
			case w.GetAudioChan() <- cloned:
			case <-time.After(1000 * time.Millisecond):
				logger.Warn("rtmp input: watcher dropped audio frame",
					zap.String("watcher_id", w.GetID()),
					zap.Int64("sequence_id", f.SequenceID),
					zap.Duration("pts", f.PTS),
					zap.String("input_id", f.InputID))
			case <-s.done:
			}
		}(out, frame)
	}

	wg.Wait()
}

func cloneFrameForWatcher(in *Frame) *Frame {
	if in == nil {
		return nil
	}

	out := *in
	out.Payload = make([][]byte, len(in.Payload))
	for i := range in.Payload {
		out.Payload[i] = cloneBytes(in.Payload[i])
	}
	out.VideoSPS = cloneBytes(in.VideoSPS)
	out.VideoPPS = cloneBytes(in.VideoPPS)
	return &out
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

func h264NALTypeFromUnit(nalu []byte) byte {
	nalu = stripAnnexBStartCode(nalu)
	if len(nalu) == 0 {
		return 0
	}
	return nalu[0] & 0x1F
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
