package irajstreamer

import (
	"strings"

	"github.com/tupicapp/restreamer/core/logger"
	"go.uber.org/zap"
)

type StreamerOption func(*Streamer)

func WithStreamerID(streamerID string) StreamerOption {
	return func(s *Streamer) {
		s.id = strings.TrimSpace(streamerID)
	}
}

func WithOutputLiveFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		if err := s.hlsFolders.SetOutputLiveFolder(folder); err != nil {
			logger.GetLogger().Warn("streamer: invalid output live folder", zap.Error(err))
		}
	}
}

func WithOutputRecordFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		if err := s.hlsFolders.SetOutputRecordFolder(folder); err != nil {
			logger.GetLogger().Warn("streamer: invalid output record folder", zap.Error(err))
		}
	}
}

func WithInputRecordRootFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		if err := s.hlsFolders.SetInputRecordRootFolder(folder); err != nil {
			logger.GetLogger().Warn("streamer: invalid input record root folder", zap.Error(err))
		}
	}
}

func WithOutputRecordRootFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		if err := s.hlsFolders.SetOutputRecordRootFolder(folder); err != nil {
			logger.GetLogger().Warn("streamer: invalid output record root folder", zap.Error(err))
		}
	}
}

func WithHLSConfig(cfg HLSConfig) StreamerOption {
	return func(s *Streamer) {
		s.hlsConfig = cfg
	}
}

func WithRecorderConfig(cfg RecorderConfig) StreamerOption {
	return func(s *Streamer) {
		s.recorderConfig = cfg
	}
}
