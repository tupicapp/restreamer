package irajstreamer

import (
	"time"

	"github.com/tupicapp/restreamer/core/logger"
	"go.uber.org/zap"
)

func (s *Streamer) startSwitcher() {
	for {
		select {
		case <-s.done:
			return
		case inputID := <-s.SwitchChan:
			s.switchInput(inputID)
		}
	}
}

func (s *Streamer) startAudio() {
l1:
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.inputsMu.RLock()
		activeInput, ok := s.inputs[s.activeInputID]
		s.inputsMu.RUnlock()

		if !ok {
			time.Sleep(5 * time.Millisecond)

			continue
		}

		var audioFrame *Frame

		select {
		case audioFrame, ok = <-activeInput.GetAudioChan():
			if !ok || audioFrame == nil {
				continue l1
			}
		case <-time.After(5 * time.Millisecond):
			continue
		}

		select {
		case s.MultiCaster.GetAudioChan() <- audioFrame:
		case <-time.After(1000 * time.Millisecond):
			logger.GetLogger().Warn("streamer: dropped audio frame to multicaster",
				zap.String("stream_id", s.activeInputID),
				zap.Int64("sequence_id", audioFrame.SequenceID),
				zap.Duration("pts", audioFrame.PTS),
				zap.String("input_id", audioFrame.InputID))
		}
	}
}

func (s *Streamer) startVideo() {
l1:
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.inputsMu.RLock()
		activeInput, ok := s.inputs[s.activeInputID]
		s.inputsMu.RUnlock()

		if !ok {
			time.Sleep(5 * time.Millisecond)

			continue
		}

		var videoframe *Frame

		select {
		case videoframe, ok = <-activeInput.GetVideoChan():
			if !ok || videoframe == nil {
				continue l1
			}

		case <-time.After(5 * time.Millisecond):
			continue
		}

		select {
		case s.MultiCaster.GetVideoChan() <- videoframe:
		case <-time.After(1000 * time.Millisecond):
			logger.GetLogger().Warn("streamer: dropped video frame to multicaster",
				zap.String("stream_id", s.activeInputID),
				zap.Int64("sequence_id", videoframe.SequenceID),
				zap.Duration("pts", videoframe.PTS),
				zap.String("input_id", videoframe.InputID),
				zap.Bool("is_keyframe", videoframe.IsKeyFrame))
		}
	}
}

func (s *Streamer) StartLife() {
	if s.MultiCaster != nil {
		s.MultiCaster.Start()
	}

	go s.startVideo()
	go s.startAudio()
	go s.startSwitcher()
}
