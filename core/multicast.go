package irajstreamer

import (
	"restreamer/core/logger"
	"sync"
	"time"

	"go.uber.org/zap"
)

type multiCaster struct {
	audioChan chan *Frame
	videoChan chan *Frame
	streamer  *Streamer
	done      chan struct{}
}

func NewMultiCaster() MultiCaster {
	return &multiCaster{
		audioChan: make(chan *Frame, 100),
		videoChan: make(chan *Frame, 100),
		done:      make(chan struct{}),
	}
}

func (m *multiCaster) SetStreamer(streamer *Streamer) {
	m.streamer = streamer
}

func (m *multiCaster) GetAudioChan() chan *Frame {
	return m.audioChan
}

func (m *multiCaster) GetVideoChan() chan *Frame {
	return m.videoChan
}

func (m *multiCaster) Start() {
	// Start write goroutines (multicast frames to outputs)
	go m.writeVideo()
	go m.writeAudio()
}

// writeVideo reads from GOPBuffer video channel and multicasts to outputs
// Rate control is handled by GOPBuffer
func (m *multiCaster) writeVideo() {
	logger := logger.GetLogger()
	wg := sync.WaitGroup{}

	defer func() {
		m.streamer.outputsMu.Lock()
		defer m.streamer.outputsMu.Unlock()
		for _, output := range m.streamer.outputs {
			close(output.GetVideoChan())
		}
	}()

	for {
		select {
		case <-m.done:
			return
		case frame := <-m.videoChan:
			if frame == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// Multicast frame to all outputs (rate control handled by GOPBuffer)
			m.streamer.outputsMu.RLock()
			for _, output := range m.streamer.outputs {
				wg.Add(1)
				go func(out Stream, f *Frame) {

					defer wg.Done()
					select {
					case out.GetVideoChan() <- f:
					case <-time.After(1000 * time.Millisecond):
						logger.Warn("multicast: output dropped video frame",
							zap.String("output_id", out.GetID()),
							zap.Int64("sequence_id", f.SequenceID),
							zap.Duration("pts", f.PTS),
							zap.String("input_id", f.InputID),
							zap.Bool("is_keyframe", f.IsKeyFrame))
						m.streamer.DroppedVideoFrames++
					}
				}(output, frame)
			}
			wg.Wait()

			m.streamer.outputsMu.RUnlock()
		}
	}
}

// writeAudio reads from GOPBuffer audio channel and multicasts to outputs
// Rate control is handled by GOPBuffer
func (m *multiCaster) writeAudio() {
	logger := logger.GetLogger()
	wg := sync.WaitGroup{}

	defer func() {
		m.streamer.outputsMu.Lock()
		defer m.streamer.outputsMu.Unlock()
		for _, output := range m.streamer.outputs {
			close(output.GetAudioChan())
		}
	}()

	for {
		select {
		case <-m.done:
			return
		case frame := <-m.audioChan:
			if frame == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			// Multicast frame to all outputs (rate control handled by GOPBuffer)
			m.streamer.outputsMu.RLock()
			for _, output := range m.streamer.outputs {
				wg.Add(1)
				go func(out Stream, f *Frame) {
					defer wg.Done()
					select {
					case out.GetAudioChan() <- f:
					case <-time.After(1000 * time.Millisecond):
						logger.Warn("multicast: output dropped audio frame",
							zap.String("output_id", out.GetID()),
							zap.Int64("sequence_id", f.SequenceID),
							zap.Duration("pts", f.PTS),
							zap.String("input_id", f.InputID))
						m.streamer.DroppedAudioFrames++
					}
				}(output, frame)
			}

			wg.Wait()

			m.streamer.outputsMu.RUnlock()
		}
	}
}

func (m *multiCaster) Close() {
	close(m.done)
}
