package inputs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"

	"go.uber.org/zap"
)

type FFmpegInput struct {
	streamID string
	file     string

	command *exec.Cmd

	video         chan *Frame
	audio         chan *Frame
	lastAudioRead time.Time
	lastVideoRead time.Time

	w, h                           int // resolution
	fps                            float64
	ar, ac                         int // audio sample-rate / channels
	vBuf, aBuf                     int // channel buffer sizes
	videoFrameSize, audioFrameSize int
	audioEnabled                   bool
	videoEnabled                   bool
	logEnabled                     bool
	streamType                     StreamType

	TotalAudioFrames   int64
	TotalVideoFrames   int64
	currentVideoFps    float64
	currentAudioFps    float64
	DroppedAudioFrames float64
	DroppedVideoFrames float64

	startSignal chan struct{}
	done        chan struct{}
	audioMu     sync.RWMutex
	videoMu     sync.RWMutex
	events      *shared.EventEmitter
}

/*
Constructor
*/
func NewFFmpegInput(file, streamID string, streamType StreamType, opts ...Option) (*FFmpegInput, error) {
	c := &FFmpegInput{
		file:         file,
		w:            DefaultWidth,
		h:            DefaultHeight,
		streamID:     streamID,
		fps:          DefaultFPS,
		ar:           DefaultAudioRate,
		videoEnabled: true,
		audioEnabled: true,
		logEnabled:   true,
		ac:           DefaultAudioChannels,
		vBuf:         DefaultChannelBufferSize,
		aBuf:         DefaultChannelBufferSize,
		streamType:   streamType,
		startSignal:  make(chan struct{}, 3),
		done:         make(chan struct{}),
		events:       shared.NewEventEmitter(128),
	}

	for _, f := range opts {
		f(c)
	}

	err := c.initInputs()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (is *FFmpegInput) Clone() (Stream, error) {
	c := &FFmpegInput{
		file:         is.file,
		w:            is.w,
		h:            is.h,
		streamID:     is.streamID,
		fps:          is.fps,
		ar:           is.ar,
		videoEnabled: is.videoEnabled,
		audioEnabled: is.audioEnabled,
		ac:           is.ac,
		vBuf:         is.vBuf,
		aBuf:         is.aBuf,
		startSignal:  make(chan struct{}, 3),
		done:         make(chan struct{}),
		audioMu:      sync.RWMutex{},
		videoMu:      sync.RWMutex{},
		events:       shared.NewEventEmitter(128),
	}

	err := c.initInputs()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (is *FFmpegInput) initInputs() error {
	is.setVideoFrameSize()
	is.setAudioFrameSize()

	is.command = is.generateFFmpegCommand()

	if is.audioEnabled {
		err := is.startAudio()
		if err != nil {
			return err
		}
	}

	if is.videoEnabled {
		err := is.startVideo()
		if err != nil {
			return err
		}
	}

	if is.logEnabled {
		err := is.startLogger()
		if err != nil {
			return err
		}
	}

	return nil
}

func (is *FFmpegInput) startLogger() error {
	stderr, err := is.command.StderrPipe()
	if err != nil {
		return err
	}

	go func() {
		<-is.startSignal
		logger := getLogger()
		logger.Debug("input logger started", zap.String("stream_id", is.streamID))
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			select {
			case <-is.done:
				return
			default:
			}

			logger.Debug("input stream ffmpeg output", zap.String("stream_id", is.streamID), zap.String("output", sc.Text()))
		}
	}()

	return nil
}

func (is *FFmpegInput) startAudio() error {
	if is.ac > 0 {
		is.audio = make(chan *Frame, is.aBuf)

		/* -------- pipe for fd 3 (audio) -------- */
		audioR, audioW, err := os.Pipe()
		if err != nil {
			return err
		}

		is.command.ExtraFiles = []*os.File{audioW}

		/* ---------------- audio goroutine ---------------- */
		go func() {
			<-is.startSignal
			logger := getLogger()
			logger.Info("audio stream started", zap.String("stream_id", is.streamID))

			t1 := time.Now()
			count := 0

			defer close(is.audio)
			for {

				select {
				case <-is.done:
					return
				default:
				}

				buf := make([]byte, is.audioFrameSize)

				err := audioR.SetReadDeadline(time.Now().Add(DefaultReadDeadline))
				if err != nil {
					logger.Error("error setting audio deadline", zap.String("stream_id", is.streamID), zap.Error(err))
				}

				_, err = io.ReadFull(audioR, buf)
				if err != nil {
					continue
				} else {
					is.lastAudioRead = time.Now()
					is.TotalAudioFrames++

					count++
				}

				frame := Frame{
					Payload:   [][]byte{buf},
					Timestamp: time.Now(),
				}

				select {
				case is.audio <- &frame:
				case <-time.After(20 * time.Millisecond):
					logger.Debug("reader dropped audio frame", zap.String("stream_id", is.streamID))
					is.DroppedAudioFrames++
				}

				if count >= 300 {
					dur := time.Since(t1)
					fps := 1000 * float64(count) / float64(dur.Milliseconds())
					is.currentAudioFps = fps
					logger.Debug("stream reader audio fps", zap.String("stream_id", is.streamID), zap.Float64("fps", fps))
					count = 0
					t1 = time.Now()
				}
			}
		}()
	}

	return nil
}

func (o *FFmpegInput) OnSwitch()              {}
func (o *FFmpegInput) IsKeyFrame(*Frame) bool { return true }
func (is *FFmpegInput) startVideo() error {
	is.video = make(chan *Frame, is.vBuf)

	videoStdout, err := is.command.StdoutPipe()
	if err != nil {
		return err
	}

	go func() {
		<-is.startSignal
		logger := getLogger()
		logger.Info("video stream started", zap.String("stream_id", is.streamID))

		defer close(is.video)

		t1 := time.Now()
		count := 0

		for {
			select {
			case <-is.done:
				return
			default:
			}

			buf := make([]byte, is.videoFrameSize)

			err := videoStdout.(*os.File).SetReadDeadline(time.Now().Add(DefaultReadDeadline))
			if err != nil {
				logger.Error("error setting video deadline", zap.String("stream_id", is.streamID), zap.String("file", is.file), zap.Error(err))
			}

			if _, err := io.ReadFull(videoStdout, buf); err != nil {
				continue
			} else {
				is.lastVideoRead = time.Now()
				is.TotalVideoFrames++
				count++
			}

			frame := Frame{
				Payload:   [][]byte{buf},
				Timestamp: time.Now(),
			}

			select {
			case is.video <- &frame:
			case <-time.After(20 * time.Millisecond):
				logger.Debug("reader dropped video frame", zap.String("stream_id", is.streamID))
				is.DroppedVideoFrames++
			}

			if count >= 300 {
				dur := time.Since(t1)
				fps := 1000 * float64(count) / float64(dur.Milliseconds())
				is.currentVideoFps = fps
				logger.Debug("stream reader video fps", zap.String("stream_id", is.streamID), zap.Float64("fps", fps))
				count = 0
				t1 = time.Now()
			}
		}
	}()

	return nil
}

/***********************************************************************
 *  Options                                                          *
 ***********************************************************************/

type Option func(*FFmpegInput)

func InputWithResolution(w, h int) Option { return func(c *FFmpegInput) { c.w, c.h = w, h } }
func InputWithFPS(f float64) Option       { return func(c *FFmpegInput) { c.fps = f } }
func InputWithLogger() Option             { return func(c *FFmpegInput) { c.logEnabled = true } }
func InputWithAudioRate(rate, ch int) Option {
	return func(c *FFmpegInput) { c.ar, c.ac = rate, ch }
}

func InputWithBuffers(vBuf, aBuf int) Option {
	return func(c *FFmpegInput) { c.vBuf, c.aBuf = vBuf, aBuf }
}

/***********************************************************************
 *  Accessors                                                          *
 ***********************************************************************/
func (is *FFmpegInput) Type() string              { return "reader" }
func (is *FFmpegInput) GetVideoChan() chan *Frame { return is.video }
func (is *FFmpegInput) GetAudioChan() chan *Frame { return is.audio }
func (is *FFmpegInput) GetID() string             { return is.streamID }
func (is *FFmpegInput) AudioLock() *sync.RWMutex  { return &is.audioMu }
func (is *FFmpegInput) VideoLock() *sync.RWMutex  { return &is.videoMu }
func (is *FFmpegInput) EventChan() chan shared.Event {
	if is.events == nil {
		return nil
	}
	return is.events.Chan()
}
func (is *FFmpegInput) Start() {
	is.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: is.streamID, StreamType: is.Type(), Message: "ffmpeg input started", Meta: shared.StreamLifecycleMeta{URL: is.file, Restartable: is.IsRestartable()}})
	go func() {
		logger := getLogger()
		for {
			_, err := ProbeStream(is.file)
			if err == nil {
				logger.Info("input started",
					zap.String("stream_id", is.streamID),
					zap.String("file", is.file),
					zap.Int("width", is.w),
					zap.Int("height", is.h),
					zap.Float64("fps", is.fps),
					zap.Int("audio_rate", is.ar),
					zap.Int("audio_channels", is.ac))
				break
			}

			select {
			case <-is.done:
				return
			default:
			}

			logger.Warn("input check error", zap.String("stream_id", is.streamID), zap.String("file", is.file), zap.Error(err))

			time.Sleep(10 * time.Millisecond)
		}

		if err := is.command.Start(); err != nil {
			logger.Error("input command could not start", zap.String("stream_id", is.streamID), zap.String("file", is.file), zap.Error(err))
			is.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: is.streamID, StreamType: is.Type(), Message: "ffmpeg input command could not start", Error: err})
			return
		}

		for i := 0; i < 3; i++ {
			go func() {
				is.startSignal <- struct{}{}
			}()
		}
	}()

}

func (is *FFmpegInput) Stop() {
	is.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: is.streamID, StreamType: is.Type(), Message: "ffmpeg input stopped"})
}

func (is *FFmpegInput) WaitForStart(ctx context.Context) error {
	return nil
}

func (is *FFmpegInput) Close() {
	close(is.done)
	is.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: is.streamID, StreamType: is.Type(), Message: "ffmpeg input closed"})
	is.events.Close()
}

func (is *FFmpegInput) setVideoFrameSize() {
	is.videoFrameSize = is.w*is.h + (is.w*is.h)/2 // yuv420p
}

func (is *FFmpegInput) setAudioFrameSize() {
	is.audioFrameSize = int((float64(is.ar) / is.fps) * float64(is.ac*2))
}

func (is *FFmpegInput) IsRestartable() bool {
	switch is.streamType {
	case InputTypeFILE:
		return false
	default:
		return true
	}
}

func (is *FFmpegInput) generateFFmpegCommand() *exec.Cmd {
	scaleFilter := fmt.Sprintf("scale=%d:%d,format=yuv420p", is.w, is.h)

	typeSpecificArgs := make([]string, 0)
	if is.streamType == InputTypeFILE {
		typeSpecificArgs = append(typeSpecificArgs, []string{"-re"}...)
	}

	args := []string{
		"-hide_banner", "-loglevel", "error",
		// "-r", fmt.Sprintf("%v", is.fps),
		"-re",
		// "-threads", "1", // Limit FFmpeg to one CPU thread
		// "-fflags", "nobuffer", // Minimize buffering, low-latency mode
		// "-flags", "low_delay", // Further reduce internal buffering
		// "-strict", "experimental", // Allow experimental codecs if needed
	}

	args = append(args, typeSpecificArgs...)

	args = append(args, []string{"-i", is.file}...)

	// Always enforce output format for video
	videoArgs := []string{
		"-map", "0:v:0", // first video stream
		"-vf", scaleFilter, // resize + pixel format
		// "-vf", fmt.Sprintf("fps=%v", is.fps), // Force output FPS to maintain smooth playback and timing sync
		"-f", "rawvideo", // enforce pixel format
		"-r", fmt.Sprintf("%v", is.fps), // force FPS
		"pipe:1", // raw video out
	}

	// Always enforce output format for audio
	audioArgs := []string{
		"-map", "0:a:0", // first audio stream
		"-ac", fmt.Sprintf("%d", is.ac), // force channel count
		"-ar", fmt.Sprintf("%d", is.ar), // force sample rate
		"-f", DefaultAudioFormat, // raw PCM (16-bit little endian)
		"-af", fmt.Sprintf("aresample=%v,asetnsamples=n=%v:p=0", is.ar, int(float64(is.ar)/is.fps)),
		"pipe:3",
	}

	if is.videoEnabled {
		args = append(args, videoArgs...)
	}

	if is.audioEnabled && is.ac > 0 {
		args = append(args, audioArgs...)
	}

	return exec.Command("ffmpeg", args...)
}

func (b *FFmpegInput) RestartInterval() time.Duration { return 10 * time.Second }

func (is *FFmpegInput) State() *State {
	lastRead := is.lastAudioRead
	if is.lastVideoRead.Sub(is.lastAudioRead) > 0 {
		lastRead = is.lastVideoRead
	}

	return &State{
		RunnerDetails:      is.command.String() + "\n" + is.command.ProcessState.String(),
		LastIO:             lastRead,
		StreamID:           is.streamID,
		Type:               string(is.streamType),
		Url:                is.file,
		AudioFps:           is.currentAudioFps,
		VideoFps:           is.currentVideoFps,
		DroppedAudioFrames: is.DroppedAudioFrames,
		DroppedVideoFrames: is.DroppedVideoFrames,
		TotalAudioFrames:   is.TotalAudioFrames,
		TotalVideoFrames:   is.TotalVideoFrames,
	}
}
