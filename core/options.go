package irajstreamer

import (
	"strings"

	"github.com/tupicapp/restreamer/core/logger"
	shared "github.com/tupicapp/restreamer/core/shared"
	"go.uber.org/zap"
)

func WithChannelID(channelID string) StreamerOption {
	return func(s *Streamer) {
		s.channelID = strings.TrimSpace(channelID)
	}
}

func WithChannelLiveFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		adapted, err := shared.AdaptFolder(folder)
		if err != nil {
			logger.GetLogger().Warn("streamer: invalid channel live folder", zap.Error(err))
			return
		}
		s.channelLiveFolder = adapted
	}
}

func WithChannelRecordFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		adapted, err := shared.AdaptFolder(folder)
		if err != nil {
			logger.GetLogger().Warn("streamer: invalid channel record folder", zap.Error(err))
			return
		}
		s.channelRecordFolder = adapted
	}
}

func WithRecordRootFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		adapted, err := shared.AdaptFolder(folder)
		if err != nil {
			logger.GetLogger().Warn("streamer: invalid record root folder", zap.Error(err))
			return
		}
		s.recordRootFolder = adapted
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
