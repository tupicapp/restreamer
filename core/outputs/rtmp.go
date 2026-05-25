package outputs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	filters "github.com/tupicapp/restreamer/core/filters"
	"github.com/tupicapp/restreamer/core/shared"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"go.uber.org/zap"
)

type RtmpWriter interface {
	Write(f *shared.Frame) error
}

type rtmpLibWriter struct {
	libWriter  *gortmplib.Writer
	videoTrack *gortmplib.Track
	audioTrack *gortmplib.Track
}

func (r *rtmpLibWriter) Write(f *shared.Frame) error {
	switch f.Codec {
	case "h264":
		return r.libWriter.WriteH264(r.videoTrack, f.PTS, f.DTS, f.Payload)
	case "h265":
	case "aac":
		return r.libWriter.WriteMPEG4Audio(r.audioTrack, f.PTS, f.Payload[0])
	case "opus":
	default:
		return fmt.Errorf("unsupported codec: %s", f.Codec)
	}

	return nil
}

type rtmpWriter struct {
	id     string
	url    string
	writer RtmpWriter
	initFn func(sps, pps []byte) (RtmpWriter, error)

	isStarted   bool
	isInitiated bool
	lastIO      time.Time

	lastAudioPTSDuration time.Duration
	lastVideoPTSDuration time.Duration

	AudioFps float64
	VideoFps float64

	DroppedAudioFrames int64
	DroppedVideoFrames int64
	TotalAudioFrames   int64
	TotalVideoFrames   int64

	gopBuffer *filters.GOPBuffer

	done      chan struct{}
	Started   chan struct{}
	audioMu   sync.RWMutex
	videoMu   sync.RWMutex
	writerMu  sync.Mutex
	closeOnce sync.Once

	pendingAudio []*shared.Frame
	events       *shared.EventEmitter
}

func NewRtmpWriter(id, url string) (shared.Stream, error) {
	ps := &rtmpWriter{
		id:        id,
		url:       addDefaultRTMPPort(url),
		done:      make(chan struct{}),
		Started:   make(chan struct{}),
		gopBuffer: filters.NewGOPBuffer(true, true, true),
		events:    shared.NewEventEmitter(128),
	}
	ps.initFn = ps.newLibWriter

	// initialize the RTMP writer
	return ps, nil
}

func (s *rtmpWriter) newLibWriter(sps, pps []byte) (RtmpWriter, error) {
	u, err := url.Parse(s.url)
	if err != nil {
		return nil, err
	}

	c := &gortmplib.Client{
		URL:     u,
		Publish: true,
	}

	err = c.Initialize(context.Background())
	if err != nil {
		getLogger().Error("rtmp destination: error initializing RTMP writer", zap.Error(err))
		return nil, err
	}

	writer := &rtmpLibWriter{}
	writer.videoTrack = &gortmplib.Track{
		Codec: &codecs.H264{
			SPS: sps,
			PPS: pps,
		},
	}
	writer.audioTrack = &gortmplib.Track{
		Codec: &codecs.MPEG4Audio{
			Config: &mpeg4audio.AudioSpecificConfig{
				Type:         mpeg4audio.ObjectTypeAACLC,
				SampleRate:   DefaultAudioRate,
				ChannelCount: DefaultAudioChannels,
			},
		},
	}

	writer.libWriter = &gortmplib.Writer{
		Conn:   c,
		Tracks: []*gortmplib.Track{writer.videoTrack, writer.audioTrack},
	}

	if err := writer.libWriter.Initialize(); err != nil {
		return nil, err
	}

	return writer, nil
}

func (s *rtmpWriter) GetID() string { return s.id }
func (s *rtmpWriter) GetVideoChan() chan *shared.Frame {
	return s.gopBuffer.VideoFrameChan
}

func (s *rtmpWriter) GetAudioChan() chan *shared.Frame {
	return s.gopBuffer.AudioFrameChan
}

func (s *rtmpWriter) Type() string             { return "writer" }
func (s *rtmpWriter) AudioLock() *sync.RWMutex { return &s.audioMu }
func (s *rtmpWriter) VideoLock() *sync.RWMutex { return &s.videoMu }
func (s *rtmpWriter) IsRestartable() bool      { return true }
func (s *rtmpWriter) State() *shared.State {
	return &shared.State{
		LastIO:             s.lastIO,
		IsStarted:          s.isStarted,
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
func (o *rtmpWriter) IsKeyFrame(*shared.Frame) bool { return true }

func (s *rtmpWriter) Clone() (shared.Stream, error) {
	return NewRtmpWriter(s.id, s.url)
}

func (s *rtmpWriter) WaitForStart(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.Started:
			return nil
		case <-s.done:
			return errors.New("stream is closed")
		}
	}
}

func (s *rtmpWriter) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *rtmpWriter) Start() {
	s.isStarted = true
	s.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: s.id, StreamType: s.Type(), Message: "rtmp destination started", Meta: shared.StreamLifecycleMeta{URL: s.url, Restartable: s.IsRestartable()}})

	if s.isInitiated {
		return
	}

	go s.run()
	go s.gopBuffer.Run()

	close(s.Started)

	s.isInitiated = true
}

func (s *rtmpWriter) Stop() {
	s.isStarted = false
	s.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: s.id, StreamType: s.Type(), Message: "rtmp destination stopped"})
}

func (s *rtmpWriter) Close() {
	s.Stop()
	s.closeOnce.Do(func() {
		close(s.done)
		if s.gopBuffer != nil {
			s.gopBuffer.Close()
		}
		s.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: s.id, StreamType: s.Type(), Message: "rtmp destination closed"})
		s.events.Close()
	})
}

func (s *rtmpWriter) run() {
	<-s.Started

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		s.consumeVideoReady()
	}()
	go func() {
		defer wg.Done()
		s.consumeAudioReady()
	}()

	wg.Wait()

	<-s.done
}

func (s *rtmpWriter) consumeVideoReady() {
	for {
		select {
		case f, ok := <-s.gopBuffer.GetVideoReadyChan():
			if !ok {
				return
			}
			if f == nil {
				continue
			}

			if err := s.WriteH264(f); err != nil {
				s.DroppedVideoFrames++
				getLogger().Error("rtmp destination: dropped video frame (ring full)",
					zap.Int64("sequence_id", f.SequenceID),
					zap.Duration("pts", f.PTS),
					zap.String("input_id", f.InputID))
			}

		case <-s.done:
			return
		}
	}
}

func (s *rtmpWriter) consumeAudioReady() {

	for {
		select {
		case f, ok := <-s.gopBuffer.GetAudioReadyChan():
			if !ok {
				return
			}
			if f == nil {
				continue
			}

			// fmt.Println("getting audio ", f.PTS)

			if err := s.WriteMpeg4Audio(f); err != nil {
				s.DroppedAudioFrames++
				getLogger().Error("rtmp destination: dropped audio frame (ring full)",
					zap.Int64("sequence_id", f.SequenceID),
					zap.Duration("pts", f.PTS),
					zap.String("input_id", f.InputID))
			}
		case <-s.done:
			return
		}
	}
}

func (s *rtmpWriter) WriteH264(f *shared.Frame) error {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	getLogger().Debug("rtmp destination: writing video frame", zap.Duration("pts", f.PTS))

	if err := s.ensureWriterForVideoLocked(f); err != nil {
		return err
	}
	if s.writer == nil {
		getLogger().Debug("rtmp destination: waiting for H264 SPS/PPS before initializing writer",
			zap.String("stream_id", s.id),
			zap.Int64("sequence_id", f.SequenceID),
			zap.Bool("is_keyframe", f.IsKeyFrame))
		return nil
	}

	// write H264 frame
	if err := s.writer.Write(f); err != nil {
		getLogger().Warn("rtmp destination: dropped video frame (write error)",
			zap.String("stream_id", s.id),
			zap.Int64("sequence_id", f.SequenceID),
			zap.Duration("pts", f.PTS),
			zap.String("input_id", f.InputID),
			zap.Bool("is_keyframe", f.IsKeyFrame),
			zap.Error(err))
		s.DroppedVideoFrames++
	} else {
		getLogger().Debug("rtmp destination: write video success",
			zap.Int64("sequence_id", f.SequenceID),
			zap.Duration("pts", f.PTS),
			zap.String("input_id", f.InputID),
			zap.Bool("is_keyframe", f.IsKeyFrame))
		s.lastIO = time.Now()
		s.TotalVideoFrames++
		s.lastVideoPTSDuration = f.PTS
		s.flushPendingAudioLocked()
	}

	return nil
}

func (r *rtmpWriter) RestartInterval() time.Duration { return 5 * time.Second }

func (s *rtmpWriter) WriteMpeg4Audio(f *shared.Frame) error {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	getLogger().Debug("rtmp destination: writing audio frame", zap.Duration("pts", f.PTS))
	if s.writer == nil {
		s.queuePendingAudioLocked(f)
		return nil
	}
	// write AAC frame (au is single []byte)
	if len(f.Payload) > 0 && len(f.Payload[0]) > 0 {
		err := s.writer.Write(f)
		if err != nil {
			getLogger().Warn("rtmp destination: dropped audio frame (write error)",
				zap.String("stream_id", s.id),
				zap.Int64("sequence_id", f.SequenceID),
				zap.Duration("pts", f.PTS),
				zap.String("input_id", f.InputID),
				zap.Error(err))
			s.DroppedAudioFrames++
		} else {
			getLogger().Debug("rtmp destination: write audio success",
				zap.Int64("sequence_id", f.SequenceID),
				zap.Duration("pts", f.PTS),
				zap.String("input_id", f.InputID),
				zap.Bool("is_keyframe", f.IsKeyFrame))
			s.lastIO = time.Now()
			s.TotalAudioFrames++
			s.lastAudioPTSDuration = f.PTS
		}
	}

	return nil
}

func (s *rtmpWriter) ensureWriterForVideoLocked(f *shared.Frame) error {
	if s.writer != nil || s.initFn == nil || f == nil {
		return nil
	}

	sps, pps := extractH264ParamsFromAccessUnit(f.Payload)
	if len(sps) == 0 || len(pps) == 0 {
		return nil
	}

	writer, err := s.initFn(sps, pps)
	if err != nil {
		return err
	}
	s.writer = writer
	return nil
}

func (s *rtmpWriter) queuePendingAudioLocked(f *shared.Frame) {
	if f == nil || len(f.Payload) == 0 {
		return
	}

	const maxPendingAudioFrames = 256
	if len(s.pendingAudio) >= maxPendingAudioFrames {
		s.pendingAudio = s.pendingAudio[1:]
		s.DroppedAudioFrames++
	}
	s.pendingAudio = append(s.pendingAudio, cloneFrame(f))
}

func (s *rtmpWriter) flushPendingAudioLocked() {
	if s.writer == nil || len(s.pendingAudio) == 0 {
		return
	}

	for _, f := range s.pendingAudio {
		if f == nil || len(f.Payload) == 0 || len(f.Payload[0]) == 0 {
			continue
		}
		if err := s.writer.Write(f); err != nil {
			getLogger().Warn("rtmp destination: dropped buffered audio frame (write error)",
				zap.String("stream_id", s.id),
				zap.Int64("sequence_id", f.SequenceID),
				zap.Duration("pts", f.PTS),
				zap.String("input_id", f.InputID),
				zap.Error(err))
			s.DroppedAudioFrames++
			continue
		}
		s.lastIO = time.Now()
		s.TotalAudioFrames++
		s.lastAudioPTSDuration = f.PTS
	}

	s.pendingAudio = nil
}

func extractH264ParamsFromAccessUnit(au [][]byte) ([]byte, []byte) {
	var sps []byte
	var pps []byte

	for _, nalu := range au {
		trimmed := trimAnnexBStartCode(nalu)
		if len(trimmed) == 0 {
			continue
		}

		switch trimmed[0] & 0x1F {
		case 7:
			sps = append([]byte(nil), trimmed...)
		case 8:
			pps = append([]byte(nil), trimmed...)
		}
	}

	return sps, pps
}

func trimAnnexBStartCode(nalu []byte) []byte {
	if len(nalu) >= 4 && nalu[0] == 0x00 && nalu[1] == 0x00 {
		if nalu[2] == 0x01 {
			return nalu[3:]
		}
		if nalu[2] == 0x00 && nalu[3] == 0x01 {
			return nalu[4:]
		}
	}
	return nalu
}

func cloneFrame(f *shared.Frame) *shared.Frame {
	if f == nil {
		return nil
	}

	out := *f
	if len(f.Payload) > 0 {
		out.Payload = make([][]byte, 0, len(f.Payload))
		for _, payload := range f.Payload {
			out.Payload = append(out.Payload, append([]byte(nil), payload...))
		}
	}

	return &out
}
