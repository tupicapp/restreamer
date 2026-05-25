package outputs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tupicapp/restreamer/core/shared"

	"go.uber.org/zap"
)

type fileOutput struct {
	id  string
	url string

	videoChan chan *shared.Frame
	audioChan chan *shared.Frame

	done    chan struct{}
	Started chan struct{}

	closeOnce sync.Once

	videoFile *os.File
	audioFile *os.File

	isStarted bool
	isInit    bool

	TotalAudioFrames   int64
	TotalVideoFrames   int64
	DroppedAudioFrames float64
	DroppedVideoFrames float64
	currentVideoFps    float64
	currentAudioFps    float64
	videoFpsTimer      time.Time
	audioFpsTimer      time.Time

	lastVideoWrite time.Time
	lastAudioWrite time.Time

	outputDir string
	events    *shared.EventEmitter
}

func NewFileOutput(id, basePath string) (*fileOutput, error) {
	if basePath == "" {
		return nil, fmt.Errorf("file output requires base path")
	}
	return &fileOutput{
		id:            id,
		url:           basePath,
		videoChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		audioChan:     make(chan *shared.Frame, DefaultChannelBufferSize),
		done:          make(chan struct{}),
		Started:       make(chan struct{}),
		videoFpsTimer: time.Now(),
		audioFpsTimer: time.Now(),
		events:        shared.NewEventEmitter(128),
	}, nil
}

func (o *fileOutput) Type() string { return "writer" }

func (o *fileOutput) GetVideoChan() chan *shared.Frame { return o.videoChan }
func (o *fileOutput) GetAudioChan() chan *shared.Frame { return o.audioChan }
func (o *fileOutput) GetID() string                    { return o.id }

func (o *fileOutput) IsRestartable() bool           { return true }
func (o *fileOutput) IsKeyFrame(*shared.Frame) bool { return true }
func (o *fileOutput) OnSwitch()                     {}
func (o *fileOutput) EventChan() chan shared.Event {
	if o.events == nil {
		return nil
	}
	return o.events.Chan()
}

func (r *fileOutput) RestartInterval() time.Duration { return 10 * time.Second }

func (o *fileOutput) Start() {
	if o.isInit {
		o.isStarted = true
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "file destination resumed", Meta: shared.StreamLifecycleMeta{URL: o.url, Restartable: o.IsRestartable()}})
		return
	}
	o.isInit = true

	dirName := sanitizeFileName(o.id) + "_" + time.Now().Format("20060102_150405")
	o.outputDir = filepath.Join(o.url, dirName)
	getLogger().Info("file output: writing to directory", zap.String("path", o.outputDir))
	if err := os.MkdirAll(o.outputDir, 0o755); err != nil {
		getLogger().Error("file output mkdir failed", zap.String("path", o.outputDir), zap.Error(err))
		return
	}

	videoPath := filepath.Join(o.outputDir, "video.h264")
	audioPath := filepath.Join(o.outputDir, "audio.aac")

	getLogger().Info("file output: writing video", zap.String("path", videoPath))
	getLogger().Info("file output: writing audio", zap.String("path", audioPath))

	vf, err := os.Create(videoPath)
	if err != nil {
		getLogger().Error("file output create h264 failed", zap.String("path", videoPath), zap.Error(err))
		return
	}
	af, err := os.Create(audioPath)
	if err != nil {
		getLogger().Error("file output create aac failed", zap.String("path", audioPath), zap.Error(err))
		vf.Close()
		return
	}

	o.videoFile = vf
	o.audioFile = af

	o.isStarted = true
	close(o.Started)
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: o.id, StreamType: o.Type(), Message: "file destination started", Meta: shared.StreamLifecycleMeta{URL: o.url, Restartable: o.IsRestartable()}})

	go o.runVideo()
	go o.runAudio()
}

func (o *fileOutput) runVideo() {
	logger := getLogger()

	// Keep SPS/PPS in memory to prepend before keyframes if needed
	var spsPps []byte

	defer func() {
		close(o.videoChan)
		close(o.audioChan)
	}()

	for {
		select {
		case <-o.done:
			return
		case frame := <-o.videoChan:
			if frame == nil || o.videoFile == nil {
				continue
			}

			if !o.isStarted {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// Prepend SPS/PPS if this is a keyframe and SPS/PPS available
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

			if err := o.writeAnnexBFrame(o.videoFile, payload); err != nil {
				logger.Error("file output write h264 failed", zap.String("stream_id", o.id), zap.Error(err))
				o.DroppedVideoFrames++
				continue
			}

			logger.Debug("file output: video frame written", zap.Duration("pts", frame.PTS))

			o.lastVideoWrite = time.Now()
			o.TotalVideoFrames++
			o.updateVideoFps()
		}
	}
}

// writeAnnexBFrame writes NALUs to a file in Annex-B format
func (o *fileOutput) writeAnnexBFrame(file *os.File, nalus [][]byte) error {
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

// prependStartCode adds 0x00000001 before each NALU
func prependStartCode(nalu []byte) []byte {
	startCode := []byte{0x00, 0x00, 0x00, 0x01}
	return append(startCode, nalu...)
}

func (o *fileOutput) runAudio() {
	logger := getLogger()

	// AAC parameters – adjust according to your actual audio stream
	profile := 2 // AAC-LC
	sampleRate := 44100
	channels := 2

	for {
		select {
		case <-o.done:
			return
		case frame := <-o.audioChan:
			if frame == nil || o.audioFile == nil {
				continue
			}

			if !o.isStarted || len(frame.Payload) == 0 || len(frame.Payload[0]) == 0 {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// Build ADTS header for this frame
			aacFrame := frame.Payload[0]
			adtsHeader := buildADTSHeader(len(aacFrame), profile, sampleRate, channels)

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

			logger.Debug("file output: audio frame written", zap.Duration("pts", frame.PTS))

			o.lastAudioWrite = time.Now()
			o.TotalAudioFrames++
			o.updateAudioFps()
		}
	}
}

func (o *fileOutput) updateVideoFps() {
	if o.TotalVideoFrames%30 == 0 {
		dur := time.Since(o.videoFpsTimer)
		if dur > 0 {
			o.currentVideoFps = 30 / dur.Seconds()
		}
		o.videoFpsTimer = time.Now()
	}
}

func (o *fileOutput) updateAudioFps() {
	if o.TotalAudioFrames%100 == 0 {
		dur := time.Since(o.audioFpsTimer)
		if dur > 0 {
			o.currentAudioFps = 100 / dur.Seconds()
		}
		o.audioFpsTimer = time.Now()
	}
}

func (o *fileOutput) GetStateTimes() (time.Time, time.Time) {
	return o.lastVideoWrite, o.lastAudioWrite
}

func (o *fileOutput) State() *shared.State {
	videoWrite, audioWrite := o.GetStateTimes()
	return &shared.State{
		IsStarted:          o.isStarted,
		StreamID:           o.id,
		Url:                o.url,
		Type:               o.Type(),
		TotalVideoFrames:   o.TotalVideoFrames,
		TotalAudioFrames:   o.TotalAudioFrames,
		DroppedVideoFrames: o.DroppedVideoFrames,
		DroppedAudioFrames: o.DroppedAudioFrames,
		VideoFps:           o.currentVideoFps,
		AudioFps:           o.currentAudioFps,
		LastIO:             maxTime(videoWrite, audioWrite),
	}
}

func (o *fileOutput) WaitForStart(ctx context.Context) error {
	select {
	case <-o.Started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *fileOutput) Stop() {
	o.isStarted = false
	o.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: o.id, StreamType: o.Type(), Message: "file destination stopped"})
}

func (o *fileOutput) Close() {
	o.closeOnce.Do(func() {
		close(o.done)
		if o.videoFile != nil {
			o.videoFile.Close()
		}
		if o.audioFile != nil {
			o.audioFile.Close()
		}
		o.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: o.id, StreamType: o.Type(), Message: "file destination closed"})
		o.events.Close()

	})
}

func (o *fileOutput) Clone() (shared.Stream, error) {
	return NewFileOutput(o.id, o.url)
}
