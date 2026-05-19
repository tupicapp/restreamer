package test

import (
	core "restreamer/core"
	"strings"
	"testing"
)

func TestRewriteHLSPlaylist_RewritesRelativeSegmentURI(t *testing.T) {
	playlist := strings.Join([]string{
		"#EXTM3U",
		"#EXTINF:2.000,",
		"seg_000001.ts",
		"",
	}, "\n")

	got := core.RewriteHLSPlaylist(playlist, "/hls/channel-a/program-a")
	if !strings.Contains(got, "/hls/channel-a/program-a/seg_000001.ts") {
		t.Fatalf("expected relative segment URI to be rewritten, got: %s", got)
	}
}

func TestRewriteHLSPlaylist_DoesNotRewriteAbsoluteSegmentURI(t *testing.T) {
	playlist := strings.Join([]string{
		"#EXTM3U",
		"#EXTINF:2.000,",
		"/v1/restream/hls/channel-a/program-a/seg_000001.ts",
		"",
	}, "\n")

	got := core.RewriteHLSPlaylist(playlist, "/hls/channel-a/program-a")
	if !strings.Contains(got, "/v1/restream/hls/channel-a/program-a/seg_000001.ts") {
		t.Fatalf("expected absolute segment URI to remain unchanged, got: %s", got)
	}
	if strings.Contains(got, "/hls/channel-a/program-a/v1/restream/hls/") {
		t.Fatalf("absolute segment URI was incorrectly double-prefixed, got: %s", got)
	}
}

func TestRewriteHLSPlaylist_RewritesLegacyRootRelativeProgramURIToConfiguredPrefix(t *testing.T) {
	playlist := strings.Join([]string{
		"#EXTM3U",
		"#EXTINF:2.000,",
		"/hls/channel-a/program-a/seg_000001.ts",
		"",
	}, "\n")

	got := core.RewriteHLSPlaylist(playlist, "http://hello/channel-a/program-a")
	if !strings.Contains(got, "http://hello/channel-a/program-a/seg_000001.ts") {
		t.Fatalf("expected legacy root-relative URI to be rewritten, got: %s", got)
	}
	if strings.Contains(got, "/hls/channel-a/program-a/seg_000001.ts") {
		t.Fatalf("expected legacy root-relative URI to be removed, got: %s", got)
	}
}

func TestJoinHLSPrefix_URLBase(t *testing.T) {
	got := core.JoinHLSPrefix("https://cdn.example.com/v1/restream/hls", "channel-a", "program-a")
	want := "https://cdn.example.com/v1/restream/hls/channel-a/program-a"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
