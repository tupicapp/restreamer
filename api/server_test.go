package api

import "testing"

func TestAbsolutizeURL(t *testing.T) {
	t.Parallel()

	baseURL := "http://127.0.0.1:8080"
	got := absolutizeURL(baseURL, "/hls/channels/123/stream.m3u8")
	want := "http://127.0.0.1:8080/hls/channels/123/stream.m3u8"
	if got != want {
		t.Fatalf("absolutizeURL() = %q, want %q", got, want)
	}
}
