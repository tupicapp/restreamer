package inputs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	shared "restreamer/irajstreamer/core/shared"

	"go.uber.org/zap"
)

const (
	defaultFFmpegRTMPURL = "rtmp://127.0.0.1:1938/live/"
	defaultFpsThreshold  = int64(15)
)

type hlsInputLive struct {
	id              string
	uri             string
	baseURL         *url.URL
	IsInitiated     bool
	closeOnce       sync.Once
	startedOnce     sync.Once
	started         chan struct{}
	done            chan struct{}
	isStarted       bool
	ffmpegCmd       *exec.Cmd
	ffmpegURL       string
	rtmpInputStream Stream
	lastIO          time.Time
	ffmpegErr       io.ReadCloser
	fpsThreshold    int64
	events          *shared.EventEmitter
}

// NewHLS returns a Stream implementation that reads from an HLS playlist.
func NewHLSLive(id, uri string) Stream {
	ffmpegRTMPURL := strings.TrimSpace(os.Getenv("HLS_READER_LIVE_FFMPEG_RTMP_URL"))
	if ffmpegRTMPURL == "" {
		ffmpegRTMPURL = defaultFFmpegRTMPURL
	}

	fpsThreshold := defaultFpsThreshold
	if raw := strings.TrimSpace(os.Getenv("HLS_READER_LIVE_FPS_HEALTH_THRESHOLD")); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			fpsThreshold = parsed
		}
	}

	rtmpInputStream := Manage(NewRTMP(id, ffmpegRTMPURL+id))
	h := &hlsInputLive{
		id:              id,
		uri:             uri,
		done:            make(chan struct{}),
		started:         make(chan struct{}),
		ffmpegURL:       ffmpegRTMPURL + id,
		rtmpInputStream: rtmpInputStream,
		fpsThreshold:    fpsThreshold,
		events:          shared.NewEventEmitter(128),
	}

	return h
}

func (r *hlsInputLive) GetVideoChan() chan *Frame      { return r.rtmpInputStream.GetVideoChan() }
func (r *hlsInputLive) GetAudioChan() chan *Frame      { return r.rtmpInputStream.GetAudioChan() }
func (r *hlsInputLive) GetID() string                  { return r.rtmpInputStream.GetID() }
func (r *hlsInputLive) Type() string                   { return "hlslive" }
func (r *hlsInputLive) IsRestartable() bool            { return r.rtmpInputStream.IsRestartable() }
func (b *hlsInputLive) RestartInterval() time.Duration { return 30 * time.Second }
func (r *hlsInputLive) EventChan() chan shared.Event {
	if r.events == nil {
		return nil
	}
	return r.events.Chan()
}

func (r *hlsInputLive) Stop() {
	select {
	case <-r.done:
		return
	default:
	}
	r.isStarted = false
	r.rtmpInputStream.Stop()
	if r.events == nil {
		return
	}
	r.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: r.id, StreamType: r.Type(), Message: "hls live reader stopped"})
}

func (r *hlsInputLive) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: r.id, StreamType: r.Type(), Message: "hls live reader closed"})
		r.events.Close()
	})

	r.rtmpInputStream.Close()
	r.Stop()
	r.stopFFmpegPipeline()
}

func (r *hlsInputLive) State() *State {
	return &State{
		LastIO:      r.lastIO,
		IsResumable: r.IsInitiated,
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
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")
		case <-r.done:
			return fmt.Errorf("hls reader live is closed")
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := r.rtmpInputStream.WaitForStart(ctx)
			if err == nil {
				r.lastIO = time.Now()
				cancel()
				return nil
			}

			cancel()
		}
	}
}

func (r *hlsInputLive) Start() {
	r.rtmpInputStream.Start()
	r.isStarted = true
	if r.IsInitiated {
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: r.id, StreamType: r.Type(), Message: "hls live reader resumed", Meta: shared.StreamLifecycleMeta{URL: r.uri, Restartable: r.IsRestartable()}})
		return
	}

	r.lastIO = time.Now()
	if err := r.startFFmpegPipeline(); err != nil {
		r.isStarted = false
		getLogger().Error("hls reader live: failed to start ffmpeg pipeline", zap.String("stream_id", r.id), zap.Error(err))
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: r.id, StreamType: r.Type(), Message: "failed to start hls live ffmpeg pipeline", Error: err})
		return
	}

	err := r.startLogger()
	if err != nil {
		r.isStarted = false
		getLogger().Error("hls reader live: failed to start stderr logger", zap.String("stream_id", r.id), zap.Error(err))
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamError, StreamID: r.id, StreamType: r.Type(), Message: "failed to start hls live stderr logger", Error: err})
		return
	}

	getLogger().Debug("hls reader: started", zap.String("stream_id", r.id), zap.String("uri", r.uri))
	r.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: r.id, StreamType: r.Type(), Message: "hls live reader started", Meta: shared.StreamLifecycleMeta{URL: r.uri, Restartable: r.IsRestartable()}})

	r.IsInitiated = true
}

func (r *hlsInputLive) startFFmpegPipeline() error {
	cmd := exec.Command("ffmpeg",
		"-nostdin",

		// INPUT PROBING (avoid missing codec params)
		"-analyzeduration", "10000000",
		"-probesize", "10000000",

		"-fflags", "+nobuffer",
		"-flags", "low_delay",

		"-re",
		"-i", r.uri,

		"-fflags", "+genpts",

		// VIDEO
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-tune", "zerolatency",
		"-x264-params", "bf=0:keyint=20:min-keyint=20:scenecut=0:repeat-headers=1",
		"-pix_fmt", "yuv420p",

		// AUDIO
		"-c:a", "aac",
		"-ar", "44100",
		"-ac", "2",

		// OUTPUT STABILITY (FLV / RTMP)
		"-max_interleave_delta", "0",
		"-muxdelay", "0",

		"-f", "flv",
		r.ffmpegURL,
	)

	cmd.Stdout = os.Stdout

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	r.ffmpegCmd = cmd
	r.ffmpegErr = stderr

	go func() {
		if err := cmd.Wait(); err != nil {
			getLogger().Error("hls reader live: ffmpeg exited", zap.String("stream_id", r.id), zap.Error(err))
		}
		r.lastIO = time.Time{}
	}()

	return nil
}

func (r *hlsInputLive) startLogger() error {
	if r.ffmpegErr == nil {
		return fmt.Errorf("ffmpeg stderr pipe not initialized")
	}

	go func() {
		sc := bufio.NewScanner(r.ffmpegErr)
		for sc.Scan() {
			select {
			case <-r.done:
				return
			default:
			}

			line := sc.Text()
			fps := parseFFmpegFps(line)

			if fps >= r.fpsThreshold {
				r.lastIO = time.Now()
				r.isStarted = true
				r.startedOnce.Do(func() {
					close(r.started)
				})
			}

			// logger.Error("input stream ffmpeg frame",
			// 	zap.String("stream_id", r.State().StreamID),
			// 	zap.Int64("fps", fps),
			// 	zap.Time("last_io", r.lastIO))
		}
	}()

	return nil
}

func (r *hlsInputLive) stopFFmpegPipeline() {
	if r.ffmpegCmd == nil || r.ffmpegCmd.Process == nil {
		return
	}

	_ = r.ffmpegCmd.Process.Signal(syscall.SIGTERM)

	time.Sleep(2 * time.Second)
	if err := r.ffmpegCmd.Process.Signal(syscall.Signal(0)); err == nil {
		if killErr := r.ffmpegCmd.Process.Kill(); killErr != nil {
			getLogger().Error("hls reader live: failed to kill ffmpeg process", zap.String("stream_id", r.id), zap.Error(killErr))
		}
	}
	r.ffmpegCmd = nil
	r.ffmpegErr = nil
	r.lastIO = time.Time{}
	r.isStarted = false
}

func parseFFmpegFps(line string) int64 {
	idx := strings.Index(line, "fps=")
	if idx == -1 {
		return -1
	}

	rest := strings.TrimLeft(line[idx+len("fps="):], " \t")
	if rest == "" {
		return -1
	}

	end := 0
	for end < len(rest) {
		c := rest[end]
		if c < '0' || c > '9' {
			break
		}
		end++
	}
	if end == 0 {
		return -1
	}

	frame, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil {
		return -1
	}

	return frame
}
