package inputs

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	shared "restreamer/core/shared"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/format/flv"
	"go.uber.org/zap"
)

type flvInput struct {
	id       string
	filePath string

	videoChan chan *Frame
	audioChan chan *Frame

	audioMu sync.RWMutex
	videoMu sync.RWMutex

	IsStarted   bool
	IsInitiated bool

	TotalVideoFrames   int64
	TotalAudioFrames   int64
	DroppedVideoFrames int64
	DroppedAudioFrames int64
	LastIO             time.Time
	RunnerDetails      string
	AudioFps           float64
	VideoFps           float64

	AudioSequenceID int64
	VideoSequenceID int64
	sequenceIDMu    sync.Mutex // Protects VideoSequenceID and AudioSequenceID

	done   chan struct{}
	events *shared.EventEmitter

	startTime time.Time
}

func NewFLV(id, filePath string) Stream {
	return &flvInput{
		id:        id,
		filePath:  filePath,
		videoChan: make(chan *Frame, DefaultChannelBufferSize),
		audioChan: make(chan *Frame, DefaultChannelBufferSize),
		done:      make(chan struct{}),
		events:    shared.NewEventEmitter(128),
	}
}

func (r *flvInput) GetVideoChan() chan *Frame { return r.videoChan }
func (r *flvInput) GetAudioChan() chan *Frame { return r.audioChan }
func (r *flvInput) GetID() string             { return r.id }
func (r *flvInput) Type() string              { return string(InputTypeFILE) }
func (r *flvInput) AudioLock() *sync.RWMutex  { return &r.audioMu }
func (r *flvInput) VideoLock() *sync.RWMutex  { return &r.videoMu }
func (r *flvInput) IsRestartable() bool       { return true }

func (r *flvInput) State() *State {
	return &State{
		LastIO:             r.LastIO,
		IsStarted:          r.IsStarted,
		StreamID:           r.id,
		Url:                r.filePath,
		Type:               r.Type(),
		DroppedAudioFrames: float64(r.DroppedAudioFrames),
		DroppedVideoFrames: float64(r.DroppedVideoFrames),
		TotalVideoFrames:   r.TotalVideoFrames,
		TotalAudioFrames:   r.TotalAudioFrames,
		AudioFps:           r.AudioFps,
		VideoFps:           r.VideoFps,
	}
}

func (r *flvInput) Clone() (Stream, error) {
	return NewFLV(r.id, r.filePath), nil
}

func (r *flvInput) WaitForStart(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if r.IsStarted {
				return nil
			}
		}
	}
}

func (r *flvInput) EventChan() chan shared.Event {
	if r.events == nil {
		return nil
	}
	return r.events.Chan()
}

func (r *flvInput) Start() {
	if r.IsInitiated {
		r.IsStarted = true
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: r.id, StreamType: r.Type(), Message: "flv reader resumed", Meta: shared.StreamLifecycleMeta{URL: r.filePath, Restartable: r.IsRestartable()}})
		return
	}

	r.IsInitiated = true
	r.IsStarted = true
	r.RunnerDetails = "flv reader loop"

	getLogger().Debug("flv reader started", zap.String("stream_id", r.id), zap.String("file", r.filePath))
	r.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: r.id, StreamType: r.Type(), Message: "flv reader started", Meta: shared.StreamLifecycleMeta{URL: r.filePath, Restartable: r.IsRestartable()}})

	go r.run()
}

func (r *flvInput) Stop() {
	r.IsStarted = false
	r.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: r.id, StreamType: r.Type(), Message: "flv reader stopped"})
}

func (r *flvInput) Close() {
	r.Stop()
	select {
	case <-r.done:
	default:
		close(r.done)
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: r.id, StreamType: r.Type(), Message: "flv reader closed"})
		r.events.Close()
	}
}

func (b *flvInput) RestartInterval() time.Duration { return 10 * time.Second }

func (r *flvInput) IsKeyFrame(frame *Frame) bool {
	if frame == nil {
		return false
	}

	for _, nalu := range frame.Payload {
		if len(nalu) == 0 {
			continue
		}
		if nalu[0]&0x1F == 5 {
			return true
		}
	}

	return false
}

func (r *flvInput) OnSwitch() {}

func (r *flvInput) run() {
	logger := getLogger()

	file, err := os.Open(r.filePath)
	if err != nil {
		logger.Error("flv reader failed to open file", zap.String("stream_id", r.id), zap.String("file", r.filePath), zap.Error(err))
		close(r.videoChan)
		close(r.audioChan)
		return
	}
	defer file.Close()

	demuxer := flv.NewDemuxer(file)
	streams, err := demuxer.Streams()
	if err != nil {
		logger.Error("flv reader failed to parse streams", zap.String("stream_id", r.id), zap.String("file", r.filePath), zap.Error(err))
		close(r.videoChan)
		close(r.audioChan)
		return
	}

	videoIdx := -1
	audioIdx := -1
	for idx, stream := range streams {
		switch {
		case stream.Type().IsVideo():
			if videoIdx == -1 {
				videoIdx = idx
			}
		case stream.Type().IsAudio():
			if audioIdx == -1 {
				audioIdx = idx
			}
		}
	}

	if videoIdx == -1 && audioIdx == -1 {
		logger.Error("flv reader found no playable streams", zap.String("stream_id", r.id), zap.String("file", r.filePath))
		close(r.videoChan)
		close(r.audioChan)
		return
	}

	startWallClock := time.Now()
	r.startTime = startWallClock
	var firstPTS time.Duration = -1

	defer func() {
		close(r.videoChan)
		close(r.audioChan)
	}()

	for {
		select {
		case <-r.done:
			return
		default:
		}

		if !r.IsStarted {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		pkt, err := demuxer.ReadPacket()
		if err != nil {
			if errors.Is(err, io.EOF) {
				logger.Info("flv reader reached EOF", zap.String("stream_id", r.id), zap.String("file", r.filePath))
			} else {
				logger.Error("flv reader read error", zap.String("stream_id", r.id), zap.String("file", r.filePath), zap.Error(err))
			}
			return
		}

		packetPTS := pkt.Time + pkt.CompositionTime
		if firstPTS < 0 {
			firstPTS = packetPTS
			startWallClock = time.Now()
		}

		targetDelay := packetPTS - firstPTS
		elapsed := time.Since(startWallClock)
		if wait := targetDelay - elapsed; wait > 0 {
			select {
			case <-r.done:
				return
			case <-time.After(wait):
			}
		}

		r.LastIO = time.Now()

		if int(pkt.Idx) == videoIdx {
			r.handleVideoPacket(pkt, logger, startWallClock)
			continue
		}

		if int(pkt.Idx) == audioIdx {
			r.handleAudioPacket(pkt, logger, startWallClock)
			continue
		}
	}
}

func (r *flvInput) handleVideoPacket(pkt av.Packet, logger *zap.Logger, startedAt time.Time) {
	var avcc h264.AVCC
	if err := avcc.Unmarshal(pkt.Data); err != nil {
		logger.Debug("flv reader failed to decode video packet", zap.String("stream_id", r.id), zap.Error(err))
		return
	}

	r.sequenceIDMu.Lock()
	r.VideoSequenceID++
	sequenceID := r.VideoSequenceID
	r.sequenceIDMu.Unlock()

	payload := cloneNALUs(avcc)

	frame := &Frame{
		PTS:        pkt.Time + pkt.CompositionTime,
		DTS:        pkt.Time,
		Payload:    payload,
		Codec:      "h264",
		Timestamp:  time.Now(),
		InputID:    r.id,
		SequenceID: sequenceID,
	}

	frame.IsKeyFrame = pkt.IsKeyFrame || r.IsKeyFrame(frame)

	r.TotalVideoFrames++
	if elapsed := time.Since(startedAt).Seconds(); elapsed > 0 {
		r.VideoFps = float64(r.TotalVideoFrames) / elapsed
	}

	select {
	case r.videoChan <- frame:
	case <-time.After(50 * time.Millisecond):
		r.DroppedVideoFrames++
		logger.Warn("flv reader: dropped video frame (channel timeout)",
			zap.String("stream_id", r.id),
			zap.Int64("sequence_id", sequenceID),
			zap.Duration("pts", frame.PTS),
			zap.String("input_id", frame.InputID),
			zap.Bool("is_keyframe", frame.IsKeyFrame))
	}
}

func (r *flvInput) handleAudioPacket(pkt av.Packet, logger *zap.Logger, startedAt time.Time) {
	data := make([]byte, len(pkt.Data))
	copy(data, pkt.Data)

	r.sequenceIDMu.Lock()
	r.AudioSequenceID++
	sequenceID := r.AudioSequenceID
	r.sequenceIDMu.Unlock()

	frame := &Frame{
		PTS:        pkt.Time,
		DTS:        pkt.Time,
		Payload:    [][]byte{data},
		Codec:      "aac",
		Timestamp:  time.Now(),
		InputID:    r.id,
		IsKeyFrame: true,
		SequenceID: sequenceID,
	}

	r.TotalAudioFrames++
	if elapsed := time.Since(startedAt).Seconds(); elapsed > 0 {
		r.AudioFps = float64(r.TotalAudioFrames) / elapsed
	}

	select {
	case r.audioChan <- frame:
	case <-time.After(50 * time.Millisecond):
		r.DroppedAudioFrames++
		logger.Warn("flv reader: dropped audio frame (channel timeout)",
			zap.String("stream_id", r.id),
			zap.Int64("sequence_id", sequenceID),
			zap.Duration("pts", frame.PTS),
			zap.String("input_id", frame.InputID))
	}
}

func cloneNALUs(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i, nalu := range in {
		b := make([]byte, len(nalu))
		copy(b, nalu)
		out[i] = b
	}
	return out
}
