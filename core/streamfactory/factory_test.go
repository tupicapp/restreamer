package streamfactory

import "testing"

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
