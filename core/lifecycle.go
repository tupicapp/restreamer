package irajstreamer

import (
	"time"

	"github.com/tupicapp/restreamer/core/logger"
	shared "github.com/tupicapp/restreamer/core/shared"
	"go.uber.org/zap"
)

const removableCleanupInterval = time.Second
const staleActiveInputSwitchAfter = time.Second

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

func (s *Streamer) startCleanup() {
	ticker := time.NewTicker(removableCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.removeRemovableInputs()
			s.removeRemovableOutputs()
		}
	}
}

func (s *Streamer) startAutoSwitch() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.autoSwitchFromStaleActiveInput()
		}
	}
}

func (s *Streamer) autoSwitchFromStaleActiveInput() {
	s.inputsMu.RLock()
	activeID := s.activeInputID
	active := s.inputs[activeID]
	if activeID == "" || active == nil {
		s.inputsMu.RUnlock()
		return
	}

	activeState := active.State()
	if activeState == nil || activeState.LastIO.IsZero() || time.Since(activeState.LastIO) <= staleActiveInputSwitchAfter {
		s.inputsMu.RUnlock()
		return
	}

	var nextID string
	var nextLastIO time.Time
	for id, input := range s.inputs {
		if id == activeID || input == nil {
			continue
		}
		state := input.State()
		if state == nil || state.IsRemovable || state.LastIO.IsZero() {
			continue
		}
		if nextID == "" || state.LastIO.After(nextLastIO) {
			nextID = id
			nextLastIO = state.LastIO
		}
	}
	s.inputsMu.RUnlock()

	if nextID != "" {
		s.Switch(nextID)
	}
}

func (s *Streamer) removeRemovableInputs() {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()

	for id, input := range s.inputs {
		if input == nil {
			continue
		}
		if id == s.activeInputID || id == s.stagedInputID {
			continue
		}
		state := input.State()
		if state == nil || !state.IsRemovable {
			continue
		}
		s.removeInputLocked(id, input, true)
	}
}

func (s *Streamer) removeRemovableOutputs() {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	for id, output := range s.outputs {
		if output == nil {
			continue
		}
		state := output.State()
		if state == nil || !state.IsRemovable {
			continue
		}

		output.Stop()
		output.Close()
		s.emitEvent(Event{
			Type:       shared.EventTypeDestinationRemoved,
			StreamID:   s.streamerIDOrDefault(),
			StreamType: "streamer",
			Message:    "destination removed from streamer",
			Meta: shared.ChildStreamMeta{
				Role:      "destination",
				ChildID:   id,
				ChildType: output.Type(),
				Managed:   true,
			},
		})
		delete(s.outputs, id)
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
	go s.startCleanup()
	go s.startAutoSwitch()
}
