package inputs

import (
	"context"
	"fmt"
	"sync"
	"time"

	gohlslib "github.com/bluenviron/gohlslib/v2"
	ghlcodecs "github.com/bluenviron/gohlslib/v2/pkg/codecs"
	shared "github.com/tupicapp/restreamer/core/shared"

	"go.uber.org/zap"
)

type hlsInputLive struct {
	id  string
	uri string

	videoChan chan *Frame
	audioChan chan *Frame

	done       chan struct{}
	started    chan struct{}
	closeOnce  sync.Once
	startOnce  sync.Once
	signalOnce sync.Once

	mu             sync.Mutex
	isStarted      bool
	isInitiated    bool
	lastIO         time.Time
	videoSeqID     int64
	audioSeqID     int64
	lastVideoGOPID int64
	lastVideoPTS   time.Duration
	lastAudioPTS   time.Duration

	h264ParamMu sync.RWMutex
	cachedSPS   []byte
	cachedPPS   []byte

	events *shared.EventEmitter
}

// NewHLSLive returns a Stream that reads a live HLS playlist using gohlslib.Client.
// No FFmpeg or external RTMP server is required.
func NewHLSLive(id, uri string) Stream {
	return &hlsInputLive{
		id:        id,
		uri:       uri,
		videoChan: make(chan *Frame, 300),
		audioChan: make(chan *Frame, 300),
		done:      make(chan struct{}),
		started:   make(chan struct{}),
		events:    shared.NewEventEmitter(128),
	}
}

func (r *hlsInputLive) GetVideoChan() chan *Frame      { return r.videoChan }
func (r *hlsInputLive) GetAudioChan() chan *Frame      { return r.audioChan }
func (r *hlsInputLive) GetID() string                  { return r.id }
func (r *hlsInputLive) Type() string                   { return "hlslive" }
func (r *hlsInputLive) IsRestartable() bool            { return false }
func (r *hlsInputLive) RestartInterval() time.Duration { return 30 * time.Second }

func (r *hlsInputLive) EventChan() chan shared.Event {
	if r.events == nil {
		return nil
	}
	return r.events.Chan()
}

func (r *hlsInputLive) State() *State {
	return &State{
		LastIO:      r.lastIO,
		IsResumable: r.isInitiated,
		IsStarted:   r.isStarted,
		StreamID:    r.id,
		Url:         r.uri,
		Type:        r.Type(),
	}
}

func (r *hlsInputLive) Clone() (Stream, error) {
	return NewHLSLive(r.id, r.uri), nil
}

func (r *hlsInputLive) WaitForStart(ctx context.Context) error {
	select {
	case <-r.started:
		return nil
	case <-r.done:
		return fmt.Errorf("stream is closed")
	case <-ctx.Done():
		return fmt.Errorf("context cancelled")
	}
}

func (r *hlsInputLive) Stop() {
	r.mu.Lock()
	r.isStarted = false
	r.mu.Unlock()
	if r.events != nil {
		r.events.Emit(shared.Event{
			Type:       shared.EventTypeStreamStopped,
			StreamID:   r.id,
			StreamType: r.Type(),
			Message:    "hls live reader stopped",
		})
	}
}

func (r *hlsInputLive) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		if r.events != nil {
			r.events.Emit(shared.Event{
				Type:       shared.EventTypeStreamClosed,
				StreamID:   r.id,
				StreamType: r.Type(),
				Message:    "hls live reader closed",
			})
			r.events.Close()
		}
	})
}

func (r *hlsInputLive) Start() {
	r.startOnce.Do(func() {
		r.mu.Lock()
		r.isStarted = true
		r.isInitiated = true
		r.mu.Unlock()

		if r.events != nil {
			r.events.Emit(shared.Event{
				Type:       shared.EventTypeStreamStarted,
				StreamID:   r.id,
				StreamType: r.Type(),
				Message:    "hls live reader starting",
				Meta:       shared.StreamLifecycleMeta{URL: r.uri, Restartable: false},
			})
		}

		go r.run()
	})
}

func (r *hlsInputLive) run() {
	for {
		select {
		case <-r.done:
			return
		default:
		}

		if err := r.runClient(); err != nil {
			getLogger().Error("hls live: client error, retrying",
				zap.String("stream_id", r.id),
				zap.Error(err))
		}

		select {
		case <-r.done:
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (r *hlsInputLive) runClient() error {
	var c *gohlslib.Client
	c = &gohlslib.Client{
		URI: r.uri,
		OnTracks: func(tracks []*gohlslib.Track) error {
			for _, track := range tracks {
				track := track
				switch track.Codec.(type) {
				case *ghlcodecs.H264:
					h264Codec := track.Codec.(*ghlcodecs.H264)
					r.setH264ParameterSets(h264Codec.SPS, h264Codec.PPS)
					c.OnDataH26x(track, func(pts, dts int64, au [][]byte) {
						r.onVideoFrame(pts, dts, au, "h264", track.ClockRate)
					})
					getLogger().Info("hls live: found H264 track",
						zap.String("stream_id", r.id),
						zap.Int("clock_rate", track.ClockRate),
						zap.Bool("has_sps", len(h264Codec.SPS) > 0),
						zap.Bool("has_pps", len(h264Codec.PPS) > 0))

				case *ghlcodecs.H265:
					c.OnDataH26x(track, func(pts, dts int64, au [][]byte) {
						r.onVideoFrame(pts, dts, au, "h265", track.ClockRate)
					})
					getLogger().Info("hls live: found H265 track",
						zap.String("stream_id", r.id),
						zap.Int("clock_rate", track.ClockRate))

				case *ghlcodecs.MPEG4Audio:
					aacCodec := track.Codec.(*ghlcodecs.MPEG4Audio)
					actualSampleRate := aacCodec.Config.SampleRate
					if actualSampleRate <= 0 {
						actualSampleRate = 44100
					}
					sr := actualSampleRate
					c.OnDataMPEG4Audio(track, func(pts int64, aus [][]byte) {
						r.onAudioFrames(pts, aus, "aac", track.ClockRate, sr)
					})
					getLogger().Info("hls live: found MPEG4Audio track",
						zap.String("stream_id", r.id),
						zap.Int("clock_rate", track.ClockRate),
						zap.Int("sample_rate", sr))

				case *ghlcodecs.Opus:
					c.OnDataOpus(track, func(pts int64, packets [][]byte) {
						r.onAudioFrames(pts, packets, "opus", track.ClockRate, track.ClockRate)
					})
					getLogger().Info("hls live: found Opus track",
						zap.String("stream_id", r.id),
						zap.Int("clock_rate", track.ClockRate))
				}
			}
			return nil
		},
		OnDownloadPrimaryPlaylist: func(u string) {
			getLogger().Debug("hls live: downloading primary playlist",
				zap.String("stream_id", r.id), zap.String("url", u))
		},
		OnDownloadStreamPlaylist: func(u string) {
			getLogger().Debug("hls live: downloading stream playlist",
				zap.String("stream_id", r.id), zap.String("url", u))
		},
		OnDownloadSegment: func(u string) {
			getLogger().Debug("hls live: downloading segment",
				zap.String("stream_id", r.id), zap.String("url", u))
		},
		OnDecodeError: func(err error) {
			getLogger().Warn("hls live: decode error",
				zap.String("stream_id", r.id), zap.Error(err))
		},
	}

	if err := c.Start(); err != nil {
		return fmt.Errorf("start gohlslib client: %w", err)
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- c.Wait2()
	}()

	select {
	case err := <-waitErr:
		c.Close()
		return err
	case <-r.done:
		c.Close()
		<-waitErr
		return nil
	}
}

func (r *hlsInputLive) onVideoFrame(pts, dts int64, au [][]byte, codec string, clockRate int) {
	ptsD := time.Duration(pts) * time.Second / time.Duration(clockRate)
	dtsD := time.Duration(dts) * time.Second / time.Duration(clockRate)
	payload := cloneNALUs(au)

	r.mu.Lock()
	r.videoSeqID++
	seqID := r.videoSeqID
	prevPTS := r.lastVideoPTS
	r.lastVideoPTS = ptsD
	r.lastIO = time.Now()
	r.mu.Unlock()

	duration := time.Duration(0)
	if prevPTS != 0 && ptsD >= prevPTS {
		duration = ptsD - prevPTS
	}

	frame := &Frame{
		PTS:        ptsD,
		DTS:        dtsD,
		Payload:    payload,
		Codec:      codec,
		PacketType: classifyVideoPacketType(au, codec),
		Timestamp:  time.Now(),
		InputID:    r.id,
		SequenceID: seqID,
		Duration:   duration,
	}
	frame.IsKeyFrame = IsTsKeyFrame(frame)
	if codec == "h264" {
		r.updateH264ParameterSets(frame.Payload)
		if frame.IsKeyFrame {
			sps, pps := r.getH264ParameterSets()
			frame.Payload = h264EnsureSPSPPSOnKeyFrame(frame.Payload, true, sps, pps)
		}
		frame.VideoSPS, frame.VideoPPS = r.getH264ParameterSets()
	}

	if frame.IsKeyFrame {
		r.mu.Lock()
		r.lastVideoGOPID = seqID
		r.mu.Unlock()
	}

	r.mu.Lock()
	frame.GOPID = r.lastVideoGOPID
	r.mu.Unlock()

	r.signalStarted()

	select {
	case r.videoChan <- frame:
	case <-r.done:
	default:
		getLogger().Warn("hls live: dropped video frame (channel full)",
			zap.String("stream_id", r.id),
			zap.Int64("seq", seqID))
	}
}

func (r *hlsInputLive) onAudioFrames(pts int64, payloads [][]byte, codec string, clockRate int, sampleRate int) {
	ptsBase := time.Duration(pts) * time.Second / time.Duration(clockRate)
	frameDur := time.Second * TsAACBytePerSample / time.Duration(sampleRate)

	for i, payload := range payloads {
		framePTS := ptsBase + time.Duration(i)*frameDur

		r.mu.Lock()
		r.audioSeqID++
		seqID := r.audioSeqID
		prevPTS := r.lastAudioPTS
		r.lastAudioPTS = framePTS
		r.lastIO = time.Now()
		r.mu.Unlock()

		duration := time.Duration(0)
		if prevPTS != 0 && framePTS >= prevPTS {
			duration = framePTS - prevPTS
		}

		frame := &Frame{
			PTS:        framePTS,
			DTS:        framePTS,
			Payload:    [][]byte{cloneBytes(payload)},
			Codec:      codec,
			Timestamp:  time.Now(),
			InputID:    r.id,
			IsKeyFrame: true,
			SequenceID: seqID,
			Duration:   duration,
			SampleRate: sampleRate,
		}

		r.mu.Lock()
		frame.GOPID = r.lastVideoGOPID
		r.mu.Unlock()

		r.signalStarted()

		select {
		case r.audioChan <- frame:
		case <-r.done:
			return
		default:
			getLogger().Warn("hls live: dropped audio frame (channel full)",
				zap.String("stream_id", r.id),
				zap.Int64("seq", seqID))
		}
	}
}

func (r *hlsInputLive) setH264ParameterSets(sps, pps []byte) {
	r.h264ParamMu.Lock()
	defer r.h264ParamMu.Unlock()

	if len(sps) > 0 {
		r.cachedSPS = cloneBytes(sps)
	}
	if len(pps) > 0 {
		r.cachedPPS = cloneBytes(pps)
	}
}

func (r *hlsInputLive) updateH264ParameterSets(nalus [][]byte) {
	sps, pps := h264ExtractSPSPPS(nalus)

	r.h264ParamMu.Lock()
	defer r.h264ParamMu.Unlock()

	if len(sps) > 0 {
		r.cachedSPS = sps
	}
	if len(pps) > 0 {
		r.cachedPPS = pps
	}
}

func (r *hlsInputLive) getH264ParameterSets() ([]byte, []byte) {
	r.h264ParamMu.RLock()
	defer r.h264ParamMu.RUnlock()

	return cloneBytes(r.cachedSPS), cloneBytes(r.cachedPPS)
}

func (r *hlsInputLive) signalStarted() {
	r.signalOnce.Do(func() {
		close(r.started)
	})
}
