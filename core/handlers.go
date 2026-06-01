package irajstreamer

import (
	"errors"
	"sync"

	"github.com/tupicapp/restreamer/core/logger"
	shared "github.com/tupicapp/restreamer/core/shared"
	"go.uber.org/zap"
)

func (s *Streamer) UpdateStreams(inputs []Stream, outputs []Stream) error {
	// ---- Update Inputs and Outputs concurrently ----
	var wg sync.WaitGroup
	wg.Add(2)

	// Inputs goroutine
	go func() {
		defer wg.Done()
		s.inputsMu.Lock()
		defer s.inputsMu.Unlock()

		current := make(map[string]struct{})
		for _, newInput := range inputs {
			current[newInput.GetID()] = struct{}{}
			if err := s.upsertInputLocked(newInput); err != nil {
				logger.GetLogger().Warn("streamer: input upsert failed",
					zap.String("input_id", newInput.GetID()),
					zap.Error(err))
			}
		}

		// remove inputs not in current list
		for id := range s.inputs {
			if _, ok := current[id]; !ok {
				s.removeInputLocked(id, nil, false)
			}
		}
	}()

	// Outputs goroutine
	go func() {
		defer wg.Done()
		s.outputsMu.Lock()
		defer s.outputsMu.Unlock()

		current := make(map[string]struct{})
		for _, newOutput := range outputs {
			current[newOutput.GetID()] = struct{}{}
			if err := s.upsertOutputLocked(newOutput); err != nil {
				logger.GetLogger().Warn("streamer: output upsert failed",
					zap.String("output_id", newOutput.GetID()),
					zap.Error(err))
			}
		}

		// remove outputs not in current list
		for id, existing := range s.outputs {
			if _, ok := current[id]; !ok {
				existing.Close()
				s.emitEvent(shared.Event{
					Type:       shared.EventTypeDestinationRemoved,
					StreamID:   s.streamerIDOrDefault(),
					StreamType: "streamer",
					Message:    "destination removed from streamer",
					Meta: shared.ChildStreamMeta{
						Role:      "destination",
						ChildID:   id,
						ChildType: existing.Type(),
						Managed:   true,
					},
				})
				delete(s.outputs, id)
			}
		}
	}()

	wg.Wait()

	return nil
}

func (s *Streamer) AddInput(i Stream) error {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	return s.upsertInputLocked(i)
}

// if it doesnt exist its no-op
func (s *Streamer) RemoveInput(streamID string) {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	s.removeInputLocked(streamID, nil, false)
}

func (s *Streamer) RemoveInputIfSame(streamID string, expected Stream) bool {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	return s.removeInputLocked(streamID, expected, true)
}

func (s *Streamer) AddOutput(o Stream) error {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()
	return s.upsertOutputLocked(o)
}

func (s *Streamer) RemoveOutput(outputID string) {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	output, ok := s.outputs[outputID]
	if !ok {
		return
	}

	output.Stop()
	output.Close()

	s.emitEvent(shared.Event{
		Type:       shared.EventTypeDestinationRemoved,
		StreamID:   s.streamerIDOrDefault(),
		StreamType: "streamer",
		Message:    "destination removed from streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "destination",
			ChildID:   outputID,
			ChildType: output.Type(),
			Managed:   true,
		},
	})

	delete(s.outputs, outputID)
}

func (s *Streamer) StopOutput(outputID string) bool {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	output, ok := s.outputs[outputID]
	if !ok {
		return false
	}

	output.Stop()
	return true
}

func (s *Streamer) Stop() {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	s.IsStarted = false
	for _, o := range s.outputs {
		o.Stop()
	}
}

func (s *Streamer) Start() {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	s.IsStarted = true
	logger := logger.GetLogger()
	for _, o := range s.outputs {
		o.Start()
		logger.Info("streamer: started output", zap.String("output_id", o.GetID()))
	}
	s.emitEvent(shared.Event{
		Type:       shared.EventTypeStreamStarted,
		StreamID:   s.streamerIDOrDefault(),
		StreamType: "streamer",
		Message:    "streamer started",
		Meta: shared.StreamLifecycleMeta{
			Restartable: false,
		},
	})
}

func (s *Streamer) switchInput(inputID string) {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()

	prevInputID := s.activeInputID
	if prevInputID != "" && prevInputID != inputID {
		if prevInput, ok := s.inputs[prevInputID]; ok && shouldPauseWhenInactive(prevInput) {
			prevInput.Stop()
		}
	}
	if nextInput, ok := s.inputs[inputID]; ok && shouldPauseWhenInactive(nextInput) {
		nextInput.Start()
	}
	s.activeInputID = inputID
}

func (s *Streamer) Switch(inputID string) bool {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	_, ok := s.inputs[inputID]
	if !ok {
		return false
	}

	if inputID == s.stagedInputID {
		return true
	}

	prevInputID := s.activeInputID
	s.stagedInputID = inputID

	select {
	case s.SwitchChan <- inputID:
		s.emitEvent(shared.Event{
			Type:       shared.EventTypeInputSwitched,
			StreamID:   s.streamerIDOrDefault(),
			StreamType: "streamer",
			Message:    "input switch requested",
			Meta: shared.InputSwitchedMeta{
				PreviousInputID: prevInputID,
				CurrentInputID:  inputID,
			},
		})
	default:
		return false
	}

	return true
}

func (s *Streamer) State() StreamerState {
	streamInputs := make([]*State, 0, len(s.inputs))
	for _, val := range s.inputs {
		streamInputs = append(streamInputs, val.State())
	}
	activeInput := s.activeInputID

	streamOutputs := make([]*State, 0, len(s.outputs))
	for _, val := range s.outputs {
		streamOutputs = append(streamOutputs, val.State())
	}

	return StreamerState{
		IsStarted:      s.IsStarted,
		CurrentInputID: activeInput,
		StreamInputs:   streamInputs,
		StreamOutputs:  streamOutputs,
	}
}

type streamStateIdentity struct {
	URL    string               `json:"url,omitempty"`
	Served []shared.ServedState `json:"served,omitempty"`
}

func currentStreamStateIdentity(stream Stream) streamStateIdentity {
	if stream == nil || stream.State() == nil {
		return streamStateIdentity{}
	}
	state := stream.State()
	return streamStateIdentity{
		URL:    state.Url,
		Served: append([]shared.ServedState(nil), state.Served...),
	}
}

func (s *Streamer) upsertOutputLocked(newOutput Stream) error {
	if newOutput == nil {
		return errors.New("nil output is not accepted")
	}

	oldOutput, exists := s.outputs[newOutput.GetID()]
	if exists {
		oldHash, _ := hashStruct(currentStreamStateIdentity(oldOutput))
		newHash, _ := hashStruct(currentStreamStateIdentity(newOutput))
		if newHash == oldHash {
			return nil
		}

		logger.GetLogger().Info("streamer: output updated", zap.String("output_id", newOutput.GetID()))
		managed := Manage(newOutput)
		managed.Start()
		s.outputs[newOutput.GetID()] = managed
		s.watchStream(managed)
		s.emitEvent(shared.Event{
			Type:       shared.EventTypeDestinationAdded,
			StreamID:   s.streamerIDOrDefault(),
			StreamType: "streamer",
			Message:    "destination updated in streamer",
			Meta: shared.ChildStreamMeta{
				Role:      "destination",
				ChildID:   newOutput.GetID(),
				ChildType: newOutput.Type(),
				ChildURL:  newOutput.State().Url,
				Managed:   true,
				Replaced:  true,
			},
		})
		oldOutput.Close()
		return nil
	}

	logger.GetLogger().Info("streamer: output added", zap.String("output_id", newOutput.GetID()))
	managed := Manage(newOutput)
	managed.Start()
	s.outputs[newOutput.GetID()] = managed
	s.watchStream(managed)
	s.emitEvent(shared.Event{
		Type:       shared.EventTypeDestinationAdded,
		StreamID:   s.streamerIDOrDefault(),
		StreamType: "streamer",
		Message:    "destination added to streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "destination",
			ChildID:   newOutput.GetID(),
			ChildType: newOutput.Type(),
			ChildURL:  newOutput.State().Url,
			Managed:   true,
		},
	})
	return nil
}

func (s *Streamer) removeInputLocked(streamID string, expected Stream, matchExpected bool) bool {
	i, ok := s.inputs[streamID]
	if !ok {
		return false
	}
	if matchExpected && !sameManagedStreamInstance(i, expected) {
		return false
	}

	i.Stop()
	i.Close()

	if s.activeInputID == streamID {
		s.activeInputID = ""
	}
	if s.stagedInputID == streamID {
		s.stagedInputID = ""
	}

	s.emitEvent(shared.Event{
		Type:       shared.EventTypeInputRemoved,
		StreamID:   s.streamerIDOrDefault(),
		StreamType: "streamer",
		Message:    "input removed from streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "input",
			ChildID:   streamID,
			ChildType: i.Type(),
			Managed:   true,
		},
	})

	delete(s.inputs, streamID)
	return true
}

func sameManagedStreamInstance(stored Stream, expected Stream) bool {
	if stored == expected {
		return true
	}
	if stored == nil || expected == nil {
		return false
	}
	if managed, ok := stored.(*streamManager); ok {
		return managed.Stream == expected
	}
	if managed, ok := expected.(*streamManager); ok {
		return managed.Stream == stored
	}
	return false
}

func (s *Streamer) upsertInputLocked(newInput Stream) error {
	if newInput == nil {
		return errors.New("nil input is not accepted")
	}

	oldInput, exists := s.inputs[newInput.GetID()]
	if exists {
		oldHash, _ := hashStruct(currentStreamStateIdentity(oldInput))
		newHash, _ := hashStruct(currentStreamStateIdentity(newInput))
		if newHash == oldHash {
			return nil
		}

		logger.GetLogger().Info("streamer: input updated", zap.String("input_id", newInput.GetID()))
		managed := Manage(newInput)
		if s.shouldStartInputLocked(managed, newInput.GetID()) {
			managed.Start()
		}
		s.inputs[newInput.GetID()] = managed
		s.watchStream(managed)
		s.emitEvent(shared.Event{
			Type:       shared.EventTypeInputAdded,
			StreamID:   s.streamerIDOrDefault(),
			StreamType: "streamer",
			Message:    "input updated in streamer",
			Meta: shared.ChildStreamMeta{
				Role:      "input",
				ChildID:   newInput.GetID(),
				ChildType: newInput.Type(),
				ChildURL:  newInput.State().Url,
				Managed:   true,
				Replaced:  true,
			},
		})
		oldInput.Close()
		return nil
	}

	logger.GetLogger().Info("streamer: input added", zap.String("input_id", newInput.GetID()))
	managed := Manage(newInput)
	if s.shouldStartInputLocked(managed, newInput.GetID()) {
		managed.Start()
	}
	s.inputs[newInput.GetID()] = managed
	s.watchStream(managed)
	s.emitEvent(shared.Event{
		Type:       shared.EventTypeInputAdded,
		StreamID:   s.streamerIDOrDefault(),
		StreamType: "streamer",
		Message:    "input added to streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "input",
			ChildID:   newInput.GetID(),
			ChildType: newInput.Type(),
			ChildURL:  newInput.State().Url,
			Managed:   true,
		},
	})
	return nil
}
