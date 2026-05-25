package inputs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/gortsplib/v5/pkg/ringbuffer"
	"go.uber.org/zap"
)

type rtmpInputStream struct {
	id        string
	url       string
	videoChan chan *Frame
	audioChan chan *Frame
	videoRing *ringbuffer.RingBuffer
	audioRing *ringbuffer.RingBuffer
	audioMu   sync.RWMutex
	videoMu   sync.RWMutex

	audioConfigMu sync.RWMutex
	audioConfig   []byte
	audioRate     int

	trackInfoMu sync.RWMutex
	trackInfo   InputTrackInfo
	trackInfoCh chan InputTrackInfo

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

	lastRealVideoFrameTime time.Time // Track when real video frames are received (not fake ones)
	lastRealVideoFrameMu   sync.RWMutex

	lastRealAudioFrameTime time.Time // Track when real audio frames are received (not fake ones)
	lastRealAudioFrameMu   sync.RWMutex

	lastVideoPacket *Frame
	lastAudioPacket *Frame

	VideoSequenceID int64
	AudioSequenceID int64

	start time.Time

	done    chan struct{}
	started chan struct{}

	// Drop flags: when a frame is dropped, don't push anything until next keyframe
	videoDropFlag bool
	audioDropFlag bool
	dropFlagMu    sync.Mutex

	// GOP tracking: track last keyframe sequence ID for GOP ID
	lastVideoGOPID int64
	lastAudioGOPID int64
	gopMu          sync.RWMutex

	sequenceIDMu sync.Mutex // Protects VideoSequenceID and AudioSequenceID

	closeOnce sync.Once
	events    *shared.EventEmitter
}

func NewRTMP(id string, rawurl string) Stream {
	s := &rtmpInputStream{
		id:          id,
		url:         rawurl,
		videoChan:   make(chan *Frame, 100),
		audioChan:   make(chan *Frame, 100),
		done:        make(chan struct{}),
		started:     make(chan struct{}),
		events:      shared.NewEventEmitter(128),
		trackInfoCh: make(chan InputTrackInfo, 8),
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

func (s *rtmpInputStream) Clone() (Stream, error) {
	return NewRTMP(s.id, s.url), nil
}

func (s *rtmpInputStream) AudioSpecificConfig() []byte {
	s.audioConfigMu.RLock()
	defer s.audioConfigMu.RUnlock()

	if len(s.audioConfig) == 0 {
		return nil
	}
	return append([]byte(nil), s.audioConfig...)
}

func (s *rtmpInputStream) TrackInfoSnapshot() InputTrackInfo {
	s.trackInfoMu.RLock()
	defer s.trackInfoMu.RUnlock()

	info := s.trackInfo
	if len(info.AudioConfig) > 0 {
		info.AudioConfig = append([]byte(nil), info.AudioConfig...)
	}
	return info
}

func (s *rtmpInputStream) TrackInfoChan() <-chan InputTrackInfo {
	return s.trackInfoCh
}

func (s *rtmpInputStream) Start() {
	if s.IsInitiated {
		s.IsStarted = true
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: s.id, StreamType: s.Type(), Message: "rtmp reader resumed", Meta: shared.StreamLifecycleMeta{URL: s.url, Restartable: s.IsRestartable()}})
		return
	}

	s.IsInitiated = true
	s.IsStarted = true
	s.RunnerDetails = "rtmp reader loop"

	getLogger().Debug("rtmp source: started", zap.String("stream_id", s.id), zap.String("url", s.url))
	s.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: s.id, StreamType: s.Type(), Message: "rtmp reader started", Meta: shared.StreamLifecycleMeta{URL: s.url, Restartable: s.IsRestartable()}})

	go s.run()
}

func (s *rtmpInputStream) WaitForStart(ctx context.Context) error {
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

func (r *rtmpInputStream) RestartInterval() time.Duration { return 5 * time.Second }

func (s *rtmpInputStream) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *rtmpInputStream) run() {
	logger := getLogger()
	u, err := url.Parse(addDefaultRTMPPort(s.url))
	if err != nil {
		logger.Error("rtmp source: invalid RTMP URL", zap.String("stream_id", s.id), zap.String("url", s.url), zap.Error(err))
		return
	}

	c := &gortmplib.Client{
		URL:     u,
		Publish: false,
	}

	if err := c.Initialize(context.Background()); err != nil {
		logger.Error("rtmp source: init failed", zap.String("stream_id", s.id), zap.String("url", s.url), zap.Error(err))
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: s.id, StreamType: s.Type(), Message: "rtmp source init failed", Error: err})
		return
	}

	r := &gortmplib.Reader{Conn: c}
	if err := r.Initialize(); err != nil {
		logger.Error("rtmp source: reader init failed", zap.String("stream_id", s.id), zap.String("url", s.url), zap.Error(err))
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: s.id, StreamType: s.Type(), Message: "rtmp reader init failed", Error: err})
		return
	}

	s.initTracks(r)

	close(s.started)

	frameInterval := 10 * time.Millisecond
	minInterval := 2 * time.Millisecond
	maxInterval := 80 * time.Millisecond
	diff := time.Duration(0)
	s.start = time.Now()
	counter := 0

	for {
		select {
		case <-s.done:
			return

		default:
			if !s.IsStarted {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			loopStart := time.Now()

			c.NetConn().SetReadDeadline(time.Now().Add(2 * time.Second))
			err := r.Read()
			if err != nil {
				logger.Debug("rtmp source: read error", zap.String("stream_id", s.id), zap.Error(err))
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// adaptive pacing
			elapsed := time.Since(s.start)
			streamPTS := s.lastPTS

			newDiff := streamPTS - elapsed

			// control loop on fps to always follow the right fps
			counter++
			if counter == 10 {
				if newDiff > diff+10*time.Millisecond {
					frameInterval += 200 * time.Microsecond
					if frameInterval > maxInterval {
						frameInterval = maxInterval
					}
				} else if newDiff < diff+10*time.Millisecond {
					frameInterval -= 200 * time.Microsecond
					if frameInterval < minInterval {
						frameInterval = minInterval
					}
				}

				diff = streamPTS - elapsed
				counter = 0
			}

			remaining := frameInterval - time.Since(loopStart)
			if remaining > 0 {
				// time.Sleep(remaining)

				// fmt.Println("sleep for: ", remaining)
			}
		}
	}
}

func (s *rtmpInputStream) forwardVideo() {
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
	}
}

func (s *rtmpInputStream) forwardAudio() {
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
	}
}

func (s *rtmpInputStream) bufferVideoPacket(pts time.Duration, dts time.Duration, au [][]byte) {
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

	f.IsKeyFrame = s.IsKeyFrame(f)

	s.gopMu.Lock()
	if f.IsKeyFrame {
		s.lastVideoGOPID = sequenceID
	}
	f.GOPID = s.lastVideoGOPID
	s.gopMu.Unlock()

	s.LastIO = time.Now()
	s.lastRealVideoFrameTime = time.Now()
	s.lastVideoPacket = f
	s.lastPTS = pts
	s.TotalVideoFrames++

	// fmt.Println("forwarding video frame", f.SequenceID, f.PTS, f.Duration)

	if !s.videoRing.Push(f) {
		// fmt.Printf("rtmp source: dropped h264 frame stream_id=%s sequence_id=%d pts=%v input_id=%s is_keyframe=%v (ring full)\n",
		// s.id, sequenceID, pts, f.InputID, f.IsKeyFrame)
		s.DroppedVideoFrames++
	}
}

func (s *rtmpInputStream) bufferAudioPacket(pts time.Duration, au []byte) {
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
		SampleRate: s.audioSampleRate(),
		Timestamp:  time.Now(),
		InputID:    s.id,
		IsKeyFrame: true,
		SequenceID: sequenceID,
	}

	// Update GOP ID: audio frames are always keyframes, so update GOP ID
	s.gopMu.Lock()
	s.lastAudioGOPID = sequenceID
	f.GOPID = s.lastAudioGOPID
	s.gopMu.Unlock()

	s.lastAudioPacket = f
	s.lastPTS = pts
	s.LastIO = time.Now()
	s.lastRealAudioFrameTime = time.Now()
	s.TotalAudioFrames++

	// fmt.Println("forwarding audio frame", f.SequenceID, f.PTS, f.Duration)

	if !s.audioRing.Push(f) {
		s.DroppedAudioFrames++

		// fmt.Printf("rtmp source: dropped mpeg4 audio frame stream_id=%s sequence_id=%d pts=%v input_id=%s (ring full)\n",
		// s.id, sequenceID, pts, f.InputID)
	}
}

func (s *rtmpInputStream) initTracks(r *gortmplib.Reader) {
	info := InputTrackInfo{Initialized: true}

	for _, track := range r.Tracks() {
		if track == nil || track.Codec == nil {
			continue
		}

		switch codec := track.Codec.(type) {
		case *codecs.H264:
			info.HasVideo = true
			r.OnDataH264(track, func(pts, dts time.Duration, au [][]byte) {
				s.bufferVideoPacket(pts, dts, au)
			})
		case *codecs.MPEG4Audio:
			info.HasAudio = true
			if codec.Config != nil {
				if config, err := codec.Config.Marshal(); err == nil && len(config) > 0 {
					s.audioConfigMu.Lock()
					s.audioConfig = append(s.audioConfig[:0], config...)
					s.audioRate = codec.Config.SampleRate
					s.audioConfigMu.Unlock()
					info.AudioConfig = append(info.AudioConfig[:0], config...)
					info.AudioSampleRate = codec.Config.SampleRate
				}
			}
			r.OnDataMPEG4Audio(track, func(pts time.Duration, au []byte) {
				s.bufferAudioPacket(pts, au)
			})

		case *codecs.H265:
			r.OnDataH265(track, func(pts, dts time.Duration, au [][]byte) {
				f := &Frame{
					PTS:       pts,
					DTS:       dts,
					Payload:   au,
					Codec:     "h265",
					Timestamp: time.Now(),
				}

				s.lastVideoPacket = f

				getLogger().Debug("rtmp source: h265 frame received",
					zap.Int64("sequence_id", f.SequenceID),
					zap.Duration("pts", f.PTS),
					zap.String("input_id", f.InputID),
					zap.Bool("is_keyframe", f.IsKeyFrame))
			})

		case *codecs.MPEG1Audio:
			r.OnDataMPEG1Audio(track, func(pts time.Duration, au []byte) {
				f := &Frame{
					PTS:        pts,
					DTS:        pts,
					Payload:    [][]byte{au},
					Codec:      "mpeg1",
					Timestamp:  time.Now(),
					InputID:    s.id,
					IsKeyFrame: true,
				}

				s.lastAudioPacket = f
			})

		case *codecs.Opus:
			r.OnDataOpus(track, func(pts time.Duration, packet []byte) {
				f := &Frame{
					PTS:       pts,
					DTS:       pts,
					Payload:   [][]byte{packet},
					Codec:     "opus",
					Timestamp: time.Now(),
				}

				s.lastAudioPacket = f
			})
		}
	}

	if info.HasAudio && info.AudioSampleRate == 0 {
		info.AudioSampleRate = s.audioSampleRate()
	}
	s.publishTrackInfo(info)
}

func (s *rtmpInputStream) Stop() {
	s.IsStarted = false
	s.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: s.id, StreamType: s.Type(), Message: "rtmp reader stopped"})
}

func (s *rtmpInputStream) Close() {
	s.Stop()

	s.closeOnce.Do(func() {
		close(s.done)
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: s.id, StreamType: s.Type(), Message: "rtmp reader closed"})
		s.events.Close()
	})
}

func (s *rtmpInputStream) IsKeyFrame(frame *Frame) bool {
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

func (s *rtmpInputStream) OnSwitch() {}

func (s *rtmpInputStream) GetVideoChan() chan *Frame { return s.videoChan }
func (s *rtmpInputStream) GetAudioChan() chan *Frame { return s.audioChan }
func (s *rtmpInputStream) GetID() string             { return s.id }
func (s *rtmpInputStream) Type() string              { return "reader" }
func (s *rtmpInputStream) AudioLock() *sync.RWMutex  { return &s.audioMu }
func (s *rtmpInputStream) VideoLock() *sync.RWMutex  { return &s.videoMu }
func (s *rtmpInputStream) IsRestartable() bool       { return true }
func (s *rtmpInputStream) State() *State {
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

func (s *rtmpInputStream) publishTrackInfo(info InputTrackInfo) {
	if len(info.AudioConfig) > 0 {
		info.AudioConfig = append([]byte(nil), info.AudioConfig...)
	}

	s.trackInfoMu.Lock()
	s.trackInfo = info
	s.trackInfoMu.Unlock()

	select {
	case s.trackInfoCh <- info:
	default:
		select {
		case <-s.trackInfoCh:
		default:
		}
		select {
		case s.trackInfoCh <- info:
		default:
		}
	}
}

func (s *rtmpInputStream) audioSampleRate() int {
	s.audioConfigMu.RLock()
	defer s.audioConfigMu.RUnlock()
	if s.audioRate > 0 {
		return s.audioRate
	}
	return DefaultAudioRate
}
