package outputs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/tupicapp/restreamer/core/shared"

	"go.uber.org/zap"
)

type FFmpegOutput struct {
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
	streamType       shared.StreamType
	id               string
	url              string
	videoCodec       string
	audioCodec       string
	audioFormat      string
	blackVideoOnstop bool
	IsStarted        bool
	IsInitited       bool
	width            int
	height           int
	fps              float64
	audioChannels    int
	audioRate        int

	lastAudioWrite time.Time
	lastVideoWrite time.Time

	TotalAudioFrames   int64
	TotalVideoFrames   int64
	currentVideoFps    float64
	currentAudioFps    float64
	DroppedAudioFrames float64
	DroppedVideoFrames float64

	closeOnce sync.Once
	Started   chan struct{}
	done      chan struct{}
	audioMu   sync.RWMutex
	videoMu   sync.RWMutex
	events    *shared.EventEmitter
}

func NewFFmpegOutput(outFile, outputId string, streamType shared.StreamType, opts ...OutputOption) (*FFmpegOutput, error) {
	o := &FFmpegOutput{
		width:         DefaultWidth,
		height:        DefaultHeight,
		fps:           DefaultFPS2,
		audioChannels: DefaultAudioChannels,
		audioRate:     DefaultAudioRate,
		videoCodec:    DefaultVideoFormat,
		audioCodec:    DefaultAudioCodec,
		audioFormat:   DefaultAudioFormat,
		url:           outFile,
		id:            outputId,
		streamType:    streamType,
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

	o.videoFile = vidStdin.(*os.File)
	o.stdErrFile = vidStderr
	o.audioFile = audW

	return o, nil
}

func (o *FFmpegOutput) Clone() (shared.Stream, error) {
	newO := &FFmpegOutput{
		width:         o.width,
		height:        o.height,
		fps:           DefaultFPS2,
		audioChannels: o.audioChannels,
		audioRate:     o.audioRate,
		videoCodec:    o.videoCodec,
		audioFormat:   o.audioFormat,
		audioCodec:    o.audioCodec,
		id:            o.id,
		url:           o.url,
		streamType:    o.streamType,
		done:          make(chan struct{}),
		streamsMu:     &sync.RWMutex{},
		audioChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		videoChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		Started:       make(chan struct{}),
		audioMu:       sync.RWMutex{},
		videoMu:       sync.RWMutex{},
		events:        shared.NewEventEmitter(128),
	}

	newO.cmd = o.generateFFmpegCommand()

	// ----------------- create audio pipe for fd 3 -----------------
	audR, audW, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	newO.cmd.ExtraFiles = []*os.File{audR} // fd 3 in child process

	// stdin for video frames
	vidStdin, err := newO.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	vidStderr, err := newO.cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	newO.videoFile = vidStdin.(*os.File)
	newO.stdErrFile = vidStderr
	newO.audioFile = audW

	return newO, nil
}

func (o *FFmpegOutput) OnSwitch()                      {}
func (o *FFmpegOutput) IsKeyFrame(*shared.Frame) bool  { return true }
func (b *FFmpegOutput) RestartInterval() time.Duration { return 10 * time.Second }

func (o *FFmpegOutput) generateFFmpegCommand() *exec.Cmd {
	// ----------------- build ffmpeg args -----------------
	args := []string{
		"-hide_banner", "-loglevel", "error",

		// --- Global Flags ---
		// "-fflags", "+genpts", // Force FFmpeg to generate presentation timestamps (PTS) for frames that lack them,
		// "-fflags", "+nobuffer", // Disable input buffering to minimize latency (process frames ASAP)
		"-fflags", "+genpts+nobuffer",
		// ensuring proper DTS/PTD ordering (fixes "DTS > PTS" issues).

		// --- Video Input (stdin pipe) ---
		"-thread_queue_size", "512",
		"-f", "rawvideo", // Input format: raw uncompressed video stream
		"-pixel_format", "yuv420p", // Pixel format expected by encoder
		"-video_size", fmt.Sprintf("%dx%d", o.width, o.height), // Input frame size
		"-framerate", fmt.Sprint(25), // Input frame rate (forces consistent timing)
		// "-r", fmt.Sprintf("%v", o.fps),
		"-i", "pipe:0", // Read video data from stdin (pipe:0)

		// --- Audio Input (pipe fd 3) ---
		"-thread_queue_size", "512",
		"-f", o.audioFormat, // Raw PCM 16-bit little-endian audio format
		"-ac", fmt.Sprintf("%d", o.audioChannels), // Number of audio channels
		"-ar", fmt.Sprintf("%d", o.audioRate), // Audio sample rate — input must match this
		"-i", "pipe:3", // Read audio data from pipe file descriptor 3

		// --- Stream Mapping ---
		"-map", "0:v:0", // Map first input's video stream
		"-map", "1:a:0", // Map second input's audio stream

		// --- Filters ---
		// "-vf", fmt.Sprintf("fps=%d", 20), // Force output FPS to maintain smooth playback and timing sync
		// "-af", fmt.Sprintf("aresample=%v,asetnsamples=n=%v:p=0", o.audioRate, int(float64(o.audioRate)/(20))),
		// "-af", fmt.Sprintf("asetnsamples=n=%d:p=0", 1000),
		// "-af", "aresample=async=1:min_hard_comp=0.100:first_pts=0",
		// "-af", "aresample=async=1:min_hard_comp=0.100000:first_pts=0",
		"-af", fmt.Sprintf("aresample=%v,asetnsamples=n=%v:p=0", o.audioRate, o.audioRate/25),
		// "-vsync", "vfr",

		// --- Encoding / Muxing ---
		"-c:v", o.videoCodec, // Set video codec (e.g., libx264)
		"-preset", DefaultPreset, // Use faster encoding preset for real-time streaming

		// fixes problem of DTS bigger than PTS
		// doesnt allow encoding before display
		// Disable frame reordering (so P- and B-frames are not delayed),
		// Reduce internal buffering,
		// Emit frames as soon as encoded, without waiting for future reference frames.
		"-tune", "zerolatency", // Optimize for low-latency streaming
		// "-pix_fmt", "yuv420p", // Set pixel format for output compatibility
		// "-maxrate", "10000k", // Limit maximum video bitrate
		"-bufsize", "0k", // Encoder buffer size (controls bitrate smoothing)
		// "-c:a", o.audioCodec, // Audio codec (e.g., aac)
		// "-b:a", "128k", // Audio bitrate (quality control)
		"-bf", "0",

		"-vsync", "0", // <--- ADD: Pass all video frames
		"-async", "1", // <--- ADD: Slave audio to video
		// "-r", fmt.Sprintf("%d", 20),
		"-flush_packets", "1",
		"-f", "flv", // Output format: FLV (for RTMP streaming)
	}

	args = append(args, "-y", o.url)

	return exec.Command("ffmpeg", args...)
}

func (o *FFmpegOutput) GetVideoChan() chan *shared.Frame {
	return o.videoChan
}

func (o *FFmpegOutput) GetAudioChan() chan *shared.Frame {
	return o.audioChan
}

func (o *FFmpegOutput) GetID() string {
	return o.id
}

func (is *FFmpegOutput) Type() string            { return "writer" }
func (o *FFmpegOutput) AudioLock() *sync.RWMutex { return &o.audioMu }
func (o *FFmpegOutput) VideoLock() *sync.RWMutex { return &o.videoMu }
func (o *FFmpegOutput) EventChan() chan shared.Event {
	if o.events == nil {
		return nil
	}
	return o.events.Chan()
}

func (o *FFmpegOutput) Close() {
	o.closeOnce.Do(func() {
		close(o.done)
		_ = o.videoFile.Close()
		_ = o.audioFile.Close()
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: o.id, StreamType: o.Type(), Message: "ffmpeg destination closed"})
		o.events.Close()
	})
}

// if its already started it will resume it
func (o *FFmpegOutput) Start() {
	if o.IsInitited {
		o.IsStarted = true
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "ffmpeg destination resumed", Meta: shared.StreamLifecycleMeta{URL: o.url, Restartable: o.IsRestartable()}})
		return
	}

	go func() {
		logger := getLogger()
		t := time.NewTicker(time.Second)

		for {
			err := FFmpegWritable(o.url)
			if err == nil {
				break
			}

			select {
			case <-t.C:
				logger.Warn("output is not writable", zap.String("output_id", o.id), zap.String("url", o.url), zap.Error(err))
			case <-o.done:
				return
			default:
			}
		}

		if err := o.cmd.Start(); err != nil {
			logger.Error("error starting output", zap.String("output_id", o.id), zap.String("url", o.url), zap.Error(err))
			o.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: o.id, StreamType: o.Type(), Message: "ffmpeg destination failed to start", Error: err})
			return
		}

		go o.videoWriter()
		go o.audioWriter()
		go o.errorLogger()

		o.IsStarted = true
		o.IsInitited = true
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "ffmpeg destination started", Meta: shared.StreamLifecycleMeta{URL: o.url, Restartable: o.IsRestartable()}})

		go func() {
			o.Started <- struct{}{}
		}()

		logger.Info("output started", zap.String("output_id", o.id), zap.String("url", o.url))

	}()
}

func FFmpegWritable(url string) error {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=black:s=1280x720:r=1",
		"-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
		"-t", "0.1",
		"-c:v", "libx264",
		"-pix_fmt", "yuv420p",
		"-c:a", "aac",
		"-frames:v", "1",
		"-f", "flv",
		url,
	}

	cmd := exec.Command("ffmpeg", args...)

	ffmpegDone := make(chan error)

	go func() {
		ffmpegDone <- cmd.Run()
	}()

	select {
	case err := <-ffmpegDone:
		if err != nil {
			return err
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("ffmpeg output [%v] is writable timeout ", url)
	}

	return nil
}

func (o *FFmpegOutput) WaitForStart(ctx context.Context) error {
	select {
	case <-o.Started:
		return nil
	case <-ctx.Done():
		return errors.New("context deadline exceeded")
	}
}

func (o *FFmpegOutput) Stop() {
	o.streamsMu.Lock()
	o.IsStarted = false
	o.streamsMu.Unlock()
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: o.id, StreamType: o.Type(), Message: "ffmpeg destination stopped"})
}

func (o *FFmpegOutput) errorLogger() {
	logger := getLogger()
	buf := make([]byte, DefaultErrorLoggerBuffer)

	for {
		select {
		case <-o.done:
			return
		default:
			n, err := o.stdErrFile.Read(buf)
			if n > 0 {
				logger.Debug("ffmpeg stderr output", zap.String("output_id", o.id), zap.String("output", string(buf[:n])))
			}

			if err != nil {
				if err == io.EOF {
					return
				}

				logger.Error("error reading ffmpeg stderr", zap.String("output_id", o.id), zap.Error(err))
				return
			}
		}
	}
}

func (is *FFmpegOutput) IsRestartable() bool {
	return true
}

func (o *FFmpegOutput) videoWriter() {
	logger := getLogger()
	t1 := time.Now()
	count := 0

	remainingFrameBytes := 0

	fps := float64(0)

	for {
		select {
		case f := <-o.videoChan:
			if !o.IsStarted {
				time.Sleep(1 * time.Millisecond)

				continue
			}

			// if time.Since(f.timeStamp) > DefaultMaxAcceptedLatancy {
			// 	fmt.Println("video : dropped outdated packet")

			// 	continue
			// }

			if remainingFrameBytes > 0 {
				logger.Debug("writing remaining bytes", zap.String("output_id", o.id))
				err := o.videoFile.(*os.File).SetWriteDeadline(time.Now().Add(DefaultWriteDeadline))
				if err != nil {
					logger.Error("error setting video pipe deadline", zap.String("output_id", o.id), zap.Error(err))
				}

				frame := make([]byte, remainingFrameBytes)
				copy(frame, f.Payload[0])

				n, err := o.videoFile.Write(frame)
				if err != nil {
					logger.Error("error writing remaining video frame", zap.String("output_id", o.id), zap.Error(err))
				}

				remainingFrameBytes -= n
			}

			err := o.videoFile.(*os.File).SetWriteDeadline(time.Now().Add(DefaultWriteDeadline))
			if err != nil {
				logger.Error("error setting video pipe deadline", zap.String("output_id", o.id), zap.Error(err))
			}

			n, err := o.videoFile.Write(f.Payload[0])
			if err != nil {
				logger.Error("error writing video frame", zap.String("output_id", o.id), zap.Error(err))
				o.DroppedVideoFrames++
			} else {
				count++
				o.TotalVideoFrames++
				o.lastVideoWrite = time.Now()
			}

			remainingFrameBytes += len(f.Payload) - n

		case <-time.After(50 * time.Millisecond):
		}

		if count >= 300 {
			dur := time.Since(t1)
			fps = 1000 * float64(count) / float64(dur.Milliseconds())
			o.currentVideoFps = fps
			logger.Debug("stream writer video fps", zap.String("output_id", o.id), zap.String("url", o.url), zap.Float64("fps", fps))
			count = 0
			t1 = time.Now()
		}

		select {
		case <-o.done:
			return
		default:
		}
	}
}

func (o *FFmpegOutput) audioWriter() {
	defer o.audioFile.Close()
	logger := getLogger()
	t1 := time.Now()
	count := 0

	fps := float64(0)

	for {
		select {
		case f := <-o.audioChan:
			if !o.IsStarted {
				continue
			}

			// if time.Since(f.timeStamp) > DefaultMaxAcceptedLatancy {
			// 	fmt.Println("audio : dropped outdated packet")

			// 	continue
			// }

			total := 0
			for total < len(f.Payload[0]) {
				err := o.audioFile.SetWriteDeadline(time.Now().Add(DefaultWriteDeadline))
				if err != nil {
					logger.Error("error setting audio pipe deadline", zap.String("output_id", o.id), zap.Error(err))
				}

				n, err := o.audioFile.Write(f.Payload[0][total:])
				if err != nil {
					logger.Error("error writing audio frame", zap.String("output_id", o.id), zap.Error(err))
					if n <= 0 {
						break
					}
					o.DroppedAudioFrames++
				} else {
					count++
					o.TotalAudioFrames++
					o.lastAudioWrite = time.Now()
				}

				total += n
			}

		case <-time.After(50 * time.Millisecond):
		}

		if count >= 300 {
			dur := time.Since(t1)
			fps = 1000 * float64(count) / float64(dur.Milliseconds())
			o.currentAudioFps = fps
			logger.Debug("stream writer audio fps", zap.String("output_id", o.id), zap.String("url", o.url), zap.Float64("fps", fps))
			count = 0
			t1 = time.Now()
		}

		select {
		case <-o.done:
			return
		default:
		}
	}
}

func (o *FFmpegOutput) State() *shared.State {
	lastRead := o.lastAudioWrite
	if o.lastVideoWrite.Sub(o.lastAudioWrite) > 0 {
		lastRead = o.lastVideoWrite
	}

	return &shared.State{
		IsStarted:          o.IsStarted,
		LastIO:             lastRead,
		StreamID:           o.id,
		Type:               string(o.streamType),
		Url:                o.url,
		DroppedAudioFrames: o.DroppedAudioFrames,
		DroppedVideoFrames: o.DroppedVideoFrames,
		TotalVideoFrames:   o.TotalVideoFrames,
		TotalAudioFrames:   o.TotalAudioFrames,
		AudioFps:           o.currentAudioFps,
		VideoFps:           o.currentVideoFps,
	}
}

// ------------------------ options ------------------------
type OutputOption func(*FFmpegOutput)

func WithVideoCodec(codec string) OutputOption {
	return func(o *FFmpegOutput) {
		o.videoCodec = codec
	}
}

func WithBlackVideoOnStop() OutputOption {
	return func(o *FFmpegOutput) {
		o.blackVideoOnstop = true
	}
}

func WithAudioCodec(codec string) OutputOption {
	return func(o *FFmpegOutput) {
		o.audioCodec = codec
	}
}

func WithResolution(width, height int) OutputOption {
	return func(o *FFmpegOutput) {
		o.width = width
		o.height = height
	}
}

func WithFPS(fps float64) OutputOption {
	return func(o *FFmpegOutput) {
		o.fps = fps
	}
}

func WithAudioParams(channels, rate int) OutputOption {
	return func(o *FFmpegOutput) {
		o.audioChannels = channels
		o.audioRate = rate
	}
}

// curl harbor-origin-a:9997/v3/paths/get/live/01k6csmjm0r0z217b340bqxhxd/01k6qrwenmzepegtjt6w1n8xpj
