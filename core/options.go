package irajstreamer

import "strings"

type StreamerOption func(*Streamer)

func WithStreamerID(streamerID string) StreamerOption {
	return func(s *Streamer) {
		s.id = strings.TrimSpace(streamerID)
	}
}
