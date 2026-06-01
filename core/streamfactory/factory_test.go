package streamfactory

import (
	"path/filepath"
	"testing"
)

func TestDetectInputKind(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want streamKind
	}{
		{name: "rtmp", url: "rtmp://localhost/live/a", want: streamKindRTMP},
		{name: "local hls fileish", url: "http://localhost/live/index.m3u8", want: streamKindFile},
		{name: "remote hls live", url: "https://example.com/live/index.m3u8", want: streamKindHLSLive},
		{name: "youtube", url: "https://youtube.com/watch?v=abc", want: streamKindYouTube},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := detectInputKind(tt.url); got != tt.want {
				t.Fatalf("detectInputKind(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestDetectOutputKind(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want streamKind
	}{
		{name: "rtmp", url: "rtmp://localhost/live/a", want: streamKindRTMP},
		{name: "youtube", url: "https://youtube.com/live2", want: streamKindYouTube},
		{name: "http output", url: "https://example.com/out.mp4", want: streamKindFile},
		{name: "local path", url: "/tmp/output", want: streamKindFile},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := detectOutputKind(tt.url); got != tt.want {
				t.Fatalf("detectOutputKind(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestNewHLSOutput_PublicBaseURL_PopulatesStateURL(t *testing.T) {
	outDir := t.TempDir()

	stream, err := NewHLSOutput(
		"hls-out",
		filepath.Join(outDir, "stream.m3u8"),
		HLSOutputOptions{PublicBaseURL: "https://cdn.example.com/live/output"},
	)
	if err != nil {
		t.Fatalf("NewHLSOutput failed: %v", err)
	}

	state := stream.State()
	if state.Url != "https://cdn.example.com/live/output/stream.m3u8" {
		t.Fatalf("unexpected state url: %q", state.Url)
	}
	if state.LocalPath != outDir {
		t.Fatalf("unexpected local path: %q", state.LocalPath)
	}
	if state.ServeType != "hls" {
		t.Fatalf("unexpected serve type: %q", state.ServeType)
	}
	if state.ServeMode != "live" {
		t.Fatalf("unexpected serve mode: %q", state.ServeMode)
	}
	if got := len(state.Served); got != 1 {
		t.Fatalf("unexpected served count: got %d want 1", got)
	}
}
