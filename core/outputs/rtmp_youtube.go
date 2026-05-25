package outputs

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/tupicapp/restreamer/core/shared"

	"go.uber.org/zap"
)

type RtmpYouTubeOutput struct {
	// output files
	videoFile  io.WriteCloser // stdin  (pipe:0)
	audioFile  *os.File       // fd 3   (pipe:3)
	stdErrFile io.ReadCloser  // fd 2   (pipe:2)
	cmd        *exec.Cmd

	// input channels
	videoChan chan *shared.Frame
	audioChan chan *shared.Frame

	// locks
	streamsMu *sync.RWMutex

	// options
	id            string
	url           string
	width         int
	height        int
	fps           float64
	audioChannels int
	audioRate     int
	videoBitrate  string
	maxrate       string
	bufsize       string
	audioBitrate  string

	profile    int
	sampleRate int
	channels   int

	IsStarted      bool
	IsInitited     bool
	lastAudioWrite time.Time
	lastVideoWrite time.Time
	startTime      time.Time
	lastVideoPTS   time.Time
	lastAudioPTS   time.Time

	videoFpsTimer time.Time
	audioFpsTimer time.Time

	TotalAudioFrames   int64
	TotalVideoFrames   int64
	currentVideoFps    float64
	currentAudioFps    float64
	DroppedAudioFrames float64
	DroppedVideoFrames float64

	closeOnce sync.Once
	Started   chan struct{}
	done      chan struct{}
	events    *shared.EventEmitter
}

func NewRtmpYouTubeOutput(outputId, rtmpURL string, opts ...YouTubeOutputOption) (*RtmpYouTubeOutput, error) {
	o := &RtmpYouTubeOutput{
		width:         DefaultWidth,
		height:        DefaultHeight,
		fps:           30.0, // YouTube standard
		audioChannels: 2,    // Stereo
		audioRate:     44100,
		videoBitrate:  "3000k",
		maxrate:       "3000k",
		bufsize:       "6000k",
		audioBitrate:  "128k",
		profile:       DefaultAudioProfile,
		sampleRate:    DefaultAudioRate,
		channels:      DefaultAudioChannels,
		url:           rtmpURL,
		id:            outputId,
		streamsMu:     &sync.RWMutex{},
		done:          make(chan struct{}),
		audioChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		videoChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		Started:       make(chan struct{}),
		events:        shared.NewEventEmitter(128),
	}

	// apply user-provided options
	for _, opt := range opts {
		opt(o)
	}

	o.cmd = o.generateFFmpegCommand()

	// ----------------- create audio pipe for fd 3 -----------------
	audR, audW, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	o.cmd.ExtraFiles = []*os.File{audR} // fd 3 in child process

	// stdin for video frames
	vidStdin, err := o.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	vidStderr, err := o.cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	o.videoFile = vidStdin
	o.stdErrFile = vidStderr
	o.audioFile = audW

	return o, nil
}

func (o *RtmpYouTubeOutput) generateFFmpegCommand() *exec.Cmd {
	// Build FFmpeg command optimized for YouTube RTMP streaming
	// Input: H264 video and MPEG4Audio (AAC) - matching user's command
	args := []string{
		"-hide_banner",
		"-re",
		// VIDEO INPUT
		// "-f", "h264",
		"-i", "pipe:0",

		// AUDIO INPUT
		// "-f", "aac",
		"-i", "pipe:3",

		"-c:v", "libx264",
		"-preset", "veryfast",
		"-r", "30",
		"-c:a", "aac",
		"-ar", "44100",
		"-b:a", "128k",

		// OUTPUT
		"-f", "flv",
		o.url,
	}

	cmd := exec.Command("ffmpeg", args...)

	getLogger().Debug("ffmpeg command", zap.String("command", cmd.String()))

	return cmd
}

func (o *RtmpYouTubeOutput) GetVideoChan() chan *shared.Frame {
	select {
	case <-o.done:
		return make(chan *shared.Frame)
	default:
		return o.videoChan
	}
}

func (o *RtmpYouTubeOutput) GetAudioChan() chan *shared.Frame {
	select {
	case <-o.done:
		return make(chan *shared.Frame)
	default:
		return o.audioChan
	}
}

func (o *RtmpYouTubeOutput) GetID() string       { return o.id }
func (o *RtmpYouTubeOutput) Type() string        { return "writer" }
func (o *RtmpYouTubeOutput) IsRestartable() bool { return true }

func (o *RtmpYouTubeOutput) IsKeyFrame(frame *shared.Frame) bool {
	if frame == nil || len(frame.Payload) == 0 {
		return false
	}

	// Check if any NAL unit is an IDR frame (type 5) or contains SPS/PPS
	for _, nalu := range frame.Payload {
		if len(nalu) == 0 {
			continue
		}
		nalType := nalu[0] & 0x1F
		// IDR frame (type 5) indicates a keyframe
		if nalType == 5 {
			return true
		}
		// Also check if frame.IsKeyFrame is set
		if frame.IsKeyFrame {
			return true
		}
	}
	return false
}

func (o *RtmpYouTubeOutput) OnSwitch() {}

func (o *RtmpYouTubeOutput) Clone() (shared.Stream, error) {
	newO := &RtmpYouTubeOutput{
		width:         o.width,
		height:        o.height,
		fps:           o.fps,
		audioChannels: o.audioChannels,
		audioRate:     o.audioRate,
		videoBitrate:  o.videoBitrate,
		maxrate:       o.maxrate,
		bufsize:       o.bufsize,
		audioBitrate:  o.audioBitrate,
		id:            o.id,
		url:           o.url,
		streamsMu:     &sync.RWMutex{},
		done:          make(chan struct{}),
		audioChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		videoChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		Started:       make(chan struct{}),
		events:        shared.NewEventEmitter(128),
	}

	newO.cmd = newO.generateFFmpegCommand()

	// ----------------- create audio pipe for fd 3 -----------------
	audR, audW, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	newO.cmd.ExtraFiles = []*os.File{audR}

	// stdin for video frames
	vidStdin, err := newO.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	vidStderr, err := newO.cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	newO.videoFile = vidStdin
	newO.stdErrFile = vidStderr
	newO.audioFile = audW

	return newO, nil
}

func (r *RtmpYouTubeOutput) RestartInterval() time.Duration { return 10 * time.Second }
func (o *RtmpYouTubeOutput) EventChan() chan shared.Event {
	if o.events == nil {
		return nil
	}
	return o.events.Chan()
}
func (o *RtmpYouTubeOutput) WaitForStart(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-o.done:
			return errors.New("stream is closed")
		case <-o.Started:
			return nil
		}
	}
}

func (o *RtmpYouTubeOutput) Start() {
	if o.IsInitited {
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "youtube destination resumed", Meta: shared.StreamLifecycleMeta{URL: o.url, Restartable: o.IsRestartable()}})
		return
	}

	o.IsInitited = true
	logger := getLogger()

	// Start FFmpeg process
	if err := o.cmd.Start(); err != nil {
		logger.Error("failed to start FFmpeg for YouTube RTMP", zap.String("stream_id", o.id), zap.Error(err))
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: o.id, StreamType: o.Type(), Message: "youtube destination failed to start", Error: err})
		return
	}

	o.startTime = time.Now()
	o.lastVideoPTS = o.startTime
	o.lastAudioPTS = o.startTime

	logger.Info("FFmpeg started for YouTube RTMP", zap.String("stream_id", o.id), zap.String("url", o.url))

	// Start goroutines for reading stderr and writing frames
	go o.readStderr()
	go o.writeVideo()
	go o.writeAudio()

	o.IsStarted = true
	close(o.Started)
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "youtube destination started", Meta: shared.StreamLifecycleMeta{URL: o.url, Restartable: o.IsRestartable()}})
}

func (o *RtmpYouTubeOutput) Stop() {
	o.IsStarted = false
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: o.id, StreamType: o.Type(), Message: "youtube destination stopped"})
}

func (o *RtmpYouTubeOutput) Close() {
	o.closeOnce.Do(func() {
		o.Stop()
		close(o.done)

		// Close pipes
		if o.videoFile != nil {
			o.videoFile.Close()
		}
		if o.audioFile != nil {
			o.audioFile.Close()
		}
		if o.stdErrFile != nil {
			o.stdErrFile.Close()
		}
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: o.id, StreamType: o.Type(), Message: "youtube destination closed"})
		o.events.Close()

		// Wait for FFmpeg to finish
		if o.cmd != nil && o.cmd.Process != nil {
			o.cmd.Process.Kill()
			o.cmd.Wait()
		}
	})
}

func (o *RtmpYouTubeOutput) State() *shared.State {
	return &shared.State{
		IsStarted:          o.IsStarted,
		StreamID:           o.id,
		Url:                o.url,
		Type:               o.Type(),
		TotalVideoFrames:   o.TotalVideoFrames,
		TotalAudioFrames:   o.TotalAudioFrames,
		DroppedVideoFrames: o.DroppedVideoFrames,
		DroppedAudioFrames: o.DroppedAudioFrames,
		VideoFps:           o.currentVideoFps,
		AudioFps:           o.currentAudioFps,
		LastIO:             o.lastVideoWrite,
	}
}

func (o *RtmpYouTubeOutput) readStderr() {
	logger := getLogger()
	buf := make([]byte, DefaultErrorLoggerBuffer)

	for {
		select {
		case <-o.done:
			return
		default:

			n, err := o.stdErrFile.Read(buf)
			if n > 0 {
				logger.Error("ffmpeg stderr output", zap.String("output_id", o.id), zap.String("output", string(buf[:n])))
			}

			if err != nil {
				if err == io.EOF {
				}
			}
		}
	}
}

func (o *RtmpYouTubeOutput) writeVideo() {
	logger := getLogger()
	spsPps := []byte{}

	defer func() {
		close(o.videoChan)
		close(o.audioChan)
	}()

	for {
		select {
		case <-o.done:
			return
		case frame := <-o.videoChan:
			if frame == nil {
				continue
			}

			if !o.IsStarted {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			if len(frame.Payload) == 0 {
				continue
			}

			for {
				if o.lastVideoPTS.Sub(o.lastAudioPTS) < time.Millisecond*30 {
					break
				}

				getLogger().Warn("video pts is behind audio pts", zap.Duration("lag", o.lastVideoPTS.Sub(o.lastAudioPTS)))

				time.Sleep(10 * time.Millisecond)
			}

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

			if err := o.writeAnnexBFrame(o.videoFile.(*os.File), payload); err != nil {
				logger.Error("file output write h264 failed", zap.String("stream_id", o.id), zap.Error(err))
				o.DroppedVideoFrames++
				continue
			}

			o.lastVideoWrite = time.Now()
			o.TotalVideoFrames++
			o.updateVideoFps()
		}
	}
}

func (o *RtmpYouTubeOutput) writeAudio() {
	logger := getLogger()

	for {
		select {
		case <-o.done:
			return
		case frame := <-o.audioChan:
			if frame == nil || o.audioFile == nil {
				continue
			}

			if !o.IsStarted || len(frame.Payload) == 0 || len(frame.Payload[0]) == 0 {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// Build ADTS header for this frame
			aacFrame := frame.Payload[0]
			adtsHeader := buildADTSHeader(len(aacFrame), o.profile, o.sampleRate, o.channels)

			// Write ADTS header + AAC frame
			if _, err := o.audioFile.Write(adtsHeader); err != nil {
				logger.Error("file output write ADTS header failed", zap.String("stream_id", o.id), zap.Error(err))
				o.DroppedAudioFrames++
				continue
			}

			if _, err := o.audioFile.Write(aacFrame); err != nil {
				logger.Error("file output write AAC frame failed", zap.String("stream_id", o.id), zap.Error(err))
				o.DroppedAudioFrames++
				continue
			}

			o.lastAudioWrite = time.Now()
			o.TotalAudioFrames++
			o.updateAudioFps()
		}
	}
}

// writeAnnexBFrame writes NALUs to a file in Annex-B format
func (o *RtmpYouTubeOutput) writeAnnexBFrame(file *os.File, nalus [][]byte) error {
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		if _, err := file.Write(prependStartCode(nalu)); err != nil {
			return err
		}
	}
	return nil
}

func (o *RtmpYouTubeOutput) updateVideoFps() {
	if o.TotalVideoFrames%30 == 0 {
		dur := time.Since(o.videoFpsTimer)
		if dur > 0 {
			o.currentVideoFps = 30 / dur.Seconds()
		}
		o.videoFpsTimer = time.Now()
	}
}

func (o *RtmpYouTubeOutput) updateAudioFps() {
	if o.TotalAudioFrames%100 == 0 {
		dur := time.Since(o.audioFpsTimer)
		if dur > 0 {
			o.currentAudioFps = 100 / dur.Seconds()
		}
		o.audioFpsTimer = time.Now()
	}
}

// YouTubeOutputOption is a function type for configuring RtmpYouTubeOutput
type YouTubeOutputOption func(*RtmpYouTubeOutput)

// WithYouTubeWidth sets the video width
func WithYouTubeWidth(width int) YouTubeOutputOption {
	return func(o *RtmpYouTubeOutput) {
		o.width = width
	}
}

// WithYouTubeHeight sets the video height
func WithYouTubeHeight(height int) YouTubeOutputOption {
	return func(o *RtmpYouTubeOutput) {
		o.height = height
	}
}

// WithYouTubeFPS sets the video frame rate
func WithYouTubeFPS(fps float64) YouTubeOutputOption {
	return func(o *RtmpYouTubeOutput) {
		o.fps = fps
	}
}

// WithYouTubeVideoBitrate sets the video bitrate
func WithYouTubeVideoBitrate(bitrate string) YouTubeOutputOption {
	return func(o *RtmpYouTubeOutput) {
		o.videoBitrate = bitrate
		o.maxrate = bitrate
		o.bufsize = bitrate + "k" // Set bufsize to 2x bitrate
	}
}

// WithYouTubeAudioBitrate sets the audio bitrate
func WithYouTubeAudioBitrate(bitrate string) YouTubeOutputOption {
	return func(o *RtmpYouTubeOutput) {
		o.audioBitrate = bitrate
	}
}
