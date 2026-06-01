package test

import (
	"testing"

	"github.com/tupicapp/restreamer/core/shared"
)

func TestRewriteHLSPlaylist_RewritesRelativeSegmentURI(t *testing.T) {
	playlist := "#EXTM3U\n#EXTINF:2.0,\nseg_000001.ts\n"

	got := shared.RewriteHLSPlaylist(playlist, "/hls/inputs/input-a")

	want := "#EXTM3U\n#EXTINF:2.0,\n/hls/inputs/input-a/seg_000001.ts\n"
	if got != want {
		t.Fatalf("RewriteHLSPlaylist mismatch:\n got=%q\nwant=%q", got, want)
	}
}

func TestRewriteHLSPlaylist_DoesNotRewriteAbsoluteSegmentURI(t *testing.T) {
	playlist := "#EXTM3U\n#EXTINF:2.0,\nhttps://cdn.example.com/seg_000001.ts\n"

	got := shared.RewriteHLSPlaylist(playlist, "/hls/inputs/input-a")

	if got != playlist {
		t.Fatalf("RewriteHLSPlaylist should keep absolute URIs unchanged:\n got=%q\nwant=%q", got, playlist)
	}
}

func TestRewriteHLSPlaylist_RewritesLegacyRootRelativeInputURIToConfiguredPrefix(t *testing.T) {
	playlist := "#EXTM3U\n#EXTINF:2.0,\n/v1/restream/hls/inputs/input-a/seg_000001.ts\n"

	got := shared.RewriteHLSPlaylist(playlist, "http://hello/inputs/input-a")

	want := "#EXTM3U\n#EXTINF:2.0,\nhttp://hello/inputs/input-a/seg_000001.ts\n"
	if got != want {
		t.Fatalf("RewriteHLSPlaylist mismatch:\n got=%q\nwant=%q", got, want)
	}
}

func TestJoinHLSPrefix_URLBase(t *testing.T) {
	got := shared.JoinURLPrefix("https://cdn.example.com/base", "stream.m3u8")
	want := "https://cdn.example.com/base/stream.m3u8"
	if got != want {
		t.Fatalf("JoinURLPrefix mismatch: got=%q want=%q", got, want)
	}
}
