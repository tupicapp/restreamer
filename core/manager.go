package irajstreamer

import (
	"context"
	"restreamer/core/logger"
	"sync"
	"time"

	"go.uber.org/zap"
)

type streamManager struct {
	Stream
	startOnce      sync.Once
	closeOnce      sync.Once
	streamsToClose chan Stream
	done           chan struct{}
}

func Manage(s Stream) Stream {
	if s.IsRestartable() {
		return &streamManager{
			Stream:         s,
			done:           make(chan struct{}),
			streamsToClose: make(chan Stream, 10),
		}
	}

	return s
}

func (s *streamManager) Start() {
	s.startOnce.Do(func() {
		go s.startWatch()
	})

	s.Stream.Start()
}

func (s *streamManager) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})

	s.Stream.Close()
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
