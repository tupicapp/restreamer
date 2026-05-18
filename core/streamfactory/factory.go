package streamfactory

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	core "restreamer/irajstreamer/core"
	"restreamer/irajstreamer/core/inputs"
	"restreamer/irajstreamer/core/outputs"
	"restreamer/internal/config"
	"restreamer/internal/storage"
)

type streamKind string

const (
	streamKindRTMP    streamKind = "rtmp"
	streamKindFile    streamKind = "file"
	streamKindHLSLive streamKind = "hlslive"
	streamKindYouTube streamKind = "youtube"
	streamKindSRT     streamKind = "srt"
	streamKindRTSP    streamKind = "rtsp"
)

func NewInput(id, streamURL string) (core.Stream, error) {
	switch detectInputKind(streamURL) {
	case streamKindRTMP:
		return inputs.NewRTMP(id, streamURL), nil
	case streamKindFile:
		stream := inputs.NewHLS(id, streamURL, inputs.OptionWithRealTime(true))
		if stream == nil {
			return nil, fmt.Errorf("failed to create file input for %q", streamURL)
		}
		return stream, nil
	case streamKindHLSLive:
		return inputs.NewHLSLive(id, streamURL), nil
	default:
		return inputs.NewRTMP(id, streamURL), nil
	}
}

func NewOutput(id, streamURL string) (core.Stream, error) {
	switch detectOutputKind(streamURL) {
	case streamKindRTMP:
		return outputs.NewRtmpWriter(id, streamURL)
	case streamKindYouTube:
		return outputs.NewRtmpYouTubeOutput(id, streamURL)
	default:
		return outputs.NewRtmpWriter(id, streamURL)
	}
}

// HLSOutputOptions configures an HLS output destination.
type HLSOutputOptions struct {
	// IsLive enables sliding-window mode: the playlist shows only the last
	// PlaylistSize segments and old TS files are cleaned up on CleanInterval.
	// When false (default), the playlist accumulates all segments (VOD/record).
	IsLive bool

	// SegmentDuration is the target duration per TS segment. Zero uses the default (2s).
	SegmentDuration time.Duration

	// PlaylistSize is the number of segments kept in the live sliding window.
	// Ignored in record mode. Zero uses the default (6).
	PlaylistSize int

	// CleanInterval is how often stale segments are removed from disk in live mode.
	// Zero disables automatic cleanup (only the sliding window trims playlist entries;
	// TS files on disk must be cleaned manually).
	CleanInterval time.Duration
}

// NewHLSOutput creates an HLS output that writes MPEG-TS segments and an M3U8
// playlist into the directory resolved from outputPath.
//
// outputPath may be:
//   - a directory: /tmp/stream/         → segments written there
//   - an m3u8 file path: /tmp/stream/stream.m3u8 → parent dir used, filename fixed to stream.m3u8
func NewHLSOutput(id, outputPath string, opts HLSOutputOptions) (core.Stream, error) {
	abs, err := filepath.Abs(outputPath)
	if err != nil {
		return nil, fmt.Errorf("resolve hls output path %q: %w", outputPath, err)
	}

	dir := abs
	if strings.HasSuffix(strings.ToLower(abs), ".m3u8") {
		dir = filepath.Dir(abs)
	}

	folder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(dir)},
	}).RecordingsRoot().Folder(filepath.Base(dir))

	hlsOpts := make([]outputs.HLSLiveOption, 0, 4)
	if opts.SegmentDuration > 0 {
		hlsOpts = append(hlsOpts, outputs.WithHLSSegmentDuration(opts.SegmentDuration))
	}
	if opts.PlaylistSize > 0 {
		hlsOpts = append(hlsOpts, outputs.WithHLSPlaylistSize(opts.PlaylistSize))
	}
	if opts.IsLive {
		hlsOpts = append(hlsOpts, outputs.WithHLSLiveMode())
		if opts.CleanInterval > 0 {
			hlsOpts = append(hlsOpts, outputs.WithHLSCleanInterval(opts.CleanInterval))
		}
	}

	return outputs.NewHLSLiveDestination(id, folder, hlsOpts...)
}

// IsHLSOutputPath reports whether the given URL/path should be treated as an
// HLS output — i.e. a local filesystem path (no :// scheme) or a .m3u8 URL.
func IsHLSOutputPath(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return false
	}
	return strings.HasSuffix(s, ".m3u8") || !strings.Contains(s, "://")
}

func detectInputKind(streamURL string) streamKind {
	lowerURL := strings.ToLower(strings.TrimSpace(streamURL))

	switch {
	case strings.Contains(lowerURL, "youtube"):
		return streamKindYouTube
	case strings.Contains(lowerURL, "srt://"):
		return streamKindSRT
	case strings.Contains(lowerURL, "rtsp://"):
		return streamKindRTSP
	case strings.Contains(lowerURL, "rtmp://"):
		return streamKindRTMP
	case strings.Contains(lowerURL, "http://"), strings.Contains(lowerURL, "https://"):
		if strings.Contains(lowerURL, "h1.gibical") || strings.Contains(lowerURL, "http://localhost") {
			return streamKindFile
		}
		return streamKindHLSLive
	default:
		return streamKindRTMP
	}
}

func detectOutputKind(streamURL string) streamKind {
	lowerURL := strings.ToLower(strings.TrimSpace(streamURL))

	switch {
	case strings.Contains(lowerURL, "youtube"):
		return streamKindYouTube
	case strings.Contains(lowerURL, "srt://"):
		return streamKindSRT
	case strings.Contains(lowerURL, "rtsp://"):
		return streamKindRTSP
	case strings.Contains(lowerURL, "rtmp://"):
		return streamKindRTMP
	case strings.Contains(lowerURL, "http://"), strings.Contains(lowerURL, "https://"):
		return streamKindFile
	default:
		return streamKindRTMP
	}
}
