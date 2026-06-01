package irajstreamer

import (
	"context"
	"github.com/tupicapp/restreamer/core/logger"
	"sync"
	"time"

	"go.uber.org/zap"
)

const nonRestartableRemovableAfter = 5 * time.Second

type streamManager struct {
	Stream
	startOnce      sync.Once
	closeOnce      sync.Once
	streamsToClose chan Stream
	done           chan struct{}
}

func Manage(s Stream) Stream {
	if s == nil {
		return nil
	}
	if _, ok := s.(*streamManager); ok {
		return s
	}
	return &streamManager{
		Stream:         s,
		done:           make(chan struct{}),
		streamsToClose: make(chan Stream, 10),
	}
}

func (s *streamManager) Start() {
	if s.Stream != nil && s.Stream.IsRestartable() {
		s.startOnce.Do(func() {
			go s.startWatch()
		})
	}

	s.Stream.Start()
}

func (s *streamManager) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})

	s.Stream.Close()
}

func (s *streamManager) State() *State {
	if s == nil || s.Stream == nil {
		return nil
	}

	state := s.Stream.State()
	if state == nil {
		return nil
	}

	cloned := *state
	if !s.Stream.IsRestartable() && state.IsStarted && !state.LastIO.IsZero() && time.Since(state.LastIO) > nonRestartableRemovableAfter {
		cloned.IsRemovable = true
	}
	return &cloned
}

func (s *streamManager) startWatch() {
	c := time.Tick(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), s.RestartInterval())
	err := s.Stream.WaitForStart(ctx)
	if err != nil {
		logger.GetLogger().Error("manager failed to wait for stream to start",
			zap.String("stream_id", s.GetID()),
			zap.Error(err))
	}
	cancel()

	for {
		select {
		case <-c:
			state := s.State()
			logger := logger.GetLogger()

			logger.Info("manager checking stream state",
				zap.String("stream_type", s.Type()),
				zap.String("stream_id", s.GetID()),
				zap.Int64("last_io_ms_ago", time.Since(state.LastIO).Milliseconds()))

			if time.Since(state.LastIO) > s.RestartInterval() {
				logger.Warn("manager restarting stream",
					zap.String("stream_type", s.Type()),
					zap.String("stream_id", s.GetID()))

				newStream, err := s.Clone()
				if err != nil {
					logger.Error("manager failed to clone stream for restart",
						zap.String("stream_id", s.GetID()),
						zap.Error(err))

					continue
				}

				oldState := s.Stream.State()

				newStream.Start()
				formerStream := s.Stream

				select {
				case streamToClose := <-s.streamsToClose:
					streamToClose.Close()
					logger.Info("manager closing stream",
						zap.String("stream_type", s.Type()),
						zap.String("stream_id", s.GetID()))
				default:
				}

				go func() {
					s.streamsToClose <- formerStream
				}()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

				newStream.WaitForStart(ctx)

				s.Stream = newStream

				cancel()

				if !oldState.IsStarted {
					newStream.Stop()
				}

				logger.Warn("manager stream restarted", zap.String("stream_id", s.GetID()))
			}
		case <-s.done:
			return
		}
	}
}
