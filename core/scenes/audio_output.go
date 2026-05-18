package scenes

import (
	"time"

	shared "restreamer/irajstreamer/core/shared"
)

func (s *Scene) consumeAudio() {
	if len(s.runtimes) == 0 {
		return
	}

	index := s.audioPassthroughIndex()
	if index < 0 || index >= len(s.runtimes) {
		index = 0
	}

	audioCh := s.runtimes[index].spec.Stream.GetAudioChan()
	for {
		select {
		case <-s.done:
			return
		case frame, ok := <-audioCh:
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			if !s.enqueueLatestAudio(s.prepareOutputAudioFrame(frame)) {
				return
			}
		}
	}
}

func (s *Scene) audioPassthroughIndex() int {
	if index := s.audioMixPassthroughIndex(); index >= 0 {
		return index
	}
	return s.cfg.audioPassthroughFrom
}

func (s *Scene) prepareOutputAudioFrame(frame *shared.Frame) *shared.Frame {
	if frame == nil {
		return nil
	}

	out := *frame
	out.InputID = s.id

	if len(frame.Payload) > 0 {
		out.Payload = make([][]byte, 0, len(frame.Payload))
		for _, payload := range frame.Payload {
			out.Payload = append(out.Payload, append([]byte(nil), payload...))
		}
	}

	return &out
}

func (s *Scene) enqueueLatestAudio(frame *shared.Frame) bool {
	if frame == nil {
		return true
	}

	for {
		select {
		case <-s.done:
			return false
		case s.audioChan <- frame:
			s.totalAudioFrames++
			s.touchLastIO()
			if s.started != nil {
				select {
				case <-s.started:
				default:
					close(s.started)
				}
			}
			return true
		default:
		}

		select {
		case <-s.done:
			return false
		case <-s.audioChan:
			s.droppedAudioFrames++
		case <-time.After(250 * time.Millisecond):
			s.droppedAudioFrames++
			return false
		}
	}
}
