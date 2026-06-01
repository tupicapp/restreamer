package inputs

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// --- playlist fixtures -------------------------------------------------------

const vodMediaPlaylist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXTINF:10.0,
seg0.ts
#EXTINF:10.0,
seg1.ts
#EXT-X-ENDLIST
`

const liveMediaPlaylist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.0,
seg0.ts
#EXTINF:10.0,
seg1.ts
`

func multivariantPlaylist(variantPath string) string {
	return fmt.Sprintf(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=640x360
%s
`, variantPath)
}

// --- helpers -----------------------------------------------------------------

func servePlaylist(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, body)
	}))
}

func serveRouter(t *testing.T, routes map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := routes[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, body)
	}))
}

// --- ProbeHLSLive tests ------------------------------------------------------

func TestProbeHLSLive_MediaVOD(t *testing.T) {
	srv := servePlaylist(t, vodMediaPlaylist)
	defer srv.Close()

	isLive, err := ProbeHLSLive(srv.URL + "/playlist.m3u8")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isLive {
		t.Fatal("expected VOD (not live): playlist has #EXT-X-ENDLIST")
	}
}

func TestProbeHLSLive_MediaLive(t *testing.T) {
	srv := servePlaylist(t, liveMediaPlaylist)
	defer srv.Close()

	isLive, err := ProbeHLSLive(srv.URL + "/playlist.m3u8")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isLive {
		t.Fatal("expected live: playlist has no #EXT-X-ENDLIST")
	}
}

func TestProbeHLSLive_MultivariantPointingToVOD(t *testing.T) {
	srv := serveRouter(t, map[string]string{
		"/master.m3u8":  multivariantPlaylist("/variant.m3u8"),
		"/variant.m3u8": vodMediaPlaylist,
	})
	defer srv.Close()

	isLive, err := ProbeHLSLive(srv.URL + "/master.m3u8")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isLive {
		t.Fatal("expected VOD: variant playlist has #EXT-X-ENDLIST")
	}
}

func TestProbeHLSLive_MultivariantPointingToLive(t *testing.T) {
	srv := serveRouter(t, map[string]string{
		"/master.m3u8":  multivariantPlaylist("/variant.m3u8"),
		"/variant.m3u8": liveMediaPlaylist,
	})
	defer srv.Close()

	isLive, err := ProbeHLSLive(srv.URL + "/master.m3u8")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isLive {
		t.Fatal("expected live: variant playlist has no #EXT-X-ENDLIST")
	}
}

func TestProbeHLSLive_LocalFileVOD(t *testing.T) {
	path := filepath.Join(t.TempDir(), "playlist.m3u8")
	if err := os.WriteFile(path, []byte(vodMediaPlaylist), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	isLive, err := ProbeHLSLive(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isLive {
		t.Fatal("expected VOD: local file has #EXT-X-ENDLIST")
	}
}

func TestProbeHLSLive_LocalFileLive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.m3u8")
	if err := os.WriteFile(path, []byte(liveMediaPlaylist), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	isLive, err := ProbeHLSLive(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isLive {
		t.Fatal("expected live: local file has no #EXT-X-ENDLIST")
	}
}

func TestProbeHLSLive_UnreachableURL(t *testing.T) {
	_, err := ProbeHLSLive("http://127.0.0.1:19999/no-server.m3u8")
	if err == nil {
		t.Fatal("expected error for unreachable URL")
	}
}

func TestProbeHLSLive_NonExistentLocalFile(t *testing.T) {
	_, err := ProbeHLSLive("/tmp/this-file-does-not-exist-at-all.m3u8")
	if err == nil {
		t.Fatal("expected error for non-existent local file")
	}
}

func TestProbeHLSLive_InvalidPlaylistFallsBackToStringCheck(t *testing.T) {
	// Serve garbage that isn't a valid M3U8 but contains #EXT-X-ENDLIST.
	srv := servePlaylist(t, "not-an-m3u8-but-has #EXT-X-ENDLIST in it")
	defer srv.Close()

	isLive, err := ProbeHLSLive(srv.URL + "/garbage.m3u8")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isLive {
		t.Fatal("expected VOD: string contains #EXT-X-ENDLIST even in malformed playlist")
	}
}

func TestProbeHLSLive_EmptyEndlist(t *testing.T) {
	// Minimal playlist with only #EXT-X-ENDLIST — edge case.
	srv := servePlaylist(t, "#EXTM3U\n#EXT-X-ENDLIST\n")
	defer srv.Close()

	isLive, err := ProbeHLSLive(srv.URL + "/empty.m3u8")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isLive {
		t.Fatal("expected VOD: playlist has #EXT-X-ENDLIST")
	}
}

// --- NewHLSAuto routing tests ------------------------------------------------

func TestNewHLSAuto_RoutesToHLSInputForVOD(t *testing.T) {
	srv := servePlaylist(t, vodMediaPlaylist)
	defer srv.Close()

	s, err := NewHLSAuto("test-vod", srv.URL+"/playlist.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto error: %v", err)
	}
	defer s.Close()

	if _, ok := s.(*hlsInput); !ok {
		t.Fatalf("expected *hlsInput for VOD, got %T", s)
	}
	if s.GetID() != "test-vod" {
		t.Fatalf("expected id 'test-vod', got %q", s.GetID())
	}
}

func TestNewHLSAuto_RoutesToHLSLiveForLive(t *testing.T) {
	srv := servePlaylist(t, liveMediaPlaylist)
	defer srv.Close()

	s, err := NewHLSAuto("test-live", srv.URL+"/playlist.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto error: %v", err)
	}
	defer s.Close()

	if _, ok := s.(*hlsInputLive); !ok {
		t.Fatalf("expected *hlsInputLive for live stream, got %T", s)
	}
	if s.GetID() != "test-live" {
		t.Fatalf("expected id 'test-live', got %q", s.GetID())
	}
}

func TestNewHLSAuto_ErrorOnBadURL(t *testing.T) {
	_, err := NewHLSAuto("test-err", "http://127.0.0.1:19999/no-server.m3u8", nil)
	if err == nil {
		t.Fatal("expected error for unreachable URL")
	}
}

func TestNewHLSAuto_VODHasCorrectChannels(t *testing.T) {
	srv := servePlaylist(t, vodMediaPlaylist)
	defer srv.Close()

	s, err := NewHLSAuto("test-chan", srv.URL+"/playlist.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto error: %v", err)
	}
	defer s.Close()

	if s.GetVideoChan() == nil {
		t.Fatal("GetVideoChan() is nil")
	}
	if s.GetAudioChan() == nil {
		t.Fatal("GetAudioChan() is nil")
	}
}

func TestNewHLSAuto_LiveHasCorrectChannels(t *testing.T) {
	srv := servePlaylist(t, liveMediaPlaylist)
	defer srv.Close()

	s, err := NewHLSAuto("test-chan-live", srv.URL+"/playlist.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto error: %v", err)
	}
	defer s.Close()

	if s.GetVideoChan() == nil {
		t.Fatal("GetVideoChan() is nil")
	}
	if s.GetAudioChan() == nil {
		t.Fatal("GetAudioChan() is nil")
	}
}

func TestNewHLSAuto_VODImplementsStreamInterface(t *testing.T) {
	srv := servePlaylist(t, vodMediaPlaylist)
	defer srv.Close()

	s, err := NewHLSAuto("test-iface", srv.URL+"/playlist.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto error: %v", err)
	}
	defer s.Close()

	// Exercise every method on the Stream interface to confirm they don't panic.
	_ = s.GetID()
	_ = s.GetVideoChan()
	_ = s.GetAudioChan()
	_ = s.State()
	_ = s.Type()
	_ = s.IsRestartable()
	_ = s.RestartInterval()
	_ = s.EventChan()
}

func TestNewHLSAuto_LiveImplementsStreamInterface(t *testing.T) {
	srv := servePlaylist(t, liveMediaPlaylist)
	defer srv.Close()

	s, err := NewHLSAuto("test-iface-live", srv.URL+"/playlist.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto error: %v", err)
	}
	defer s.Close()

	_ = s.GetID()
	_ = s.GetVideoChan()
	_ = s.GetAudioChan()
	_ = s.State()
	_ = s.Type()
	_ = s.IsRestartable()
	_ = s.RestartInterval()
	_ = s.EventChan()
}

func TestNewHLSAuto_VODandLiveYieldSameInterfaceMethods(t *testing.T) {
	// Both variants must satisfy Stream — verified at compile time by the
	// return type, but this test documents the expectation explicitly.
	vodSrv := servePlaylist(t, vodMediaPlaylist)
	defer vodSrv.Close()
	liveSrv := servePlaylist(t, liveMediaPlaylist)
	defer liveSrv.Close()

	vod, err := NewHLSAuto("vod", vodSrv.URL+"/v.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto vod: %v", err)
	}
	defer vod.Close()

	live, err := NewHLSAuto("live", liveSrv.URL+"/l.m3u8", nil)
	if err != nil {
		t.Fatalf("NewHLSAuto live: %v", err)
	}
	defer live.Close()

	// Both must expose non-nil channels.
	if vod.GetVideoChan() == nil || live.GetVideoChan() == nil {
		t.Fatal("video channels must be non-nil for both stream types")
	}
	if vod.GetAudioChan() == nil || live.GetAudioChan() == nil {
		t.Fatal("audio channels must be non-nil for both stream types")
	}

	// IDs must be preserved.
	if vod.GetID() != "vod" {
		t.Fatalf("vod id: got %q", vod.GetID())
	}
	if live.GetID() != "live" {
		t.Fatalf("live id: got %q", live.GetID())
	}
}

// --- probeIsLive unit tests (no network) -------------------------------------

func TestProbeIsLive_WithEndlist(t *testing.T) {
	isLive, err := probeIsLive("http://example.com/p.m3u8", []byte(vodMediaPlaylist))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isLive {
		t.Fatal("expected VOD")
	}
}

func TestProbeIsLive_WithoutEndlist(t *testing.T) {
	isLive, err := probeIsLive("http://example.com/p.m3u8", []byte(liveMediaPlaylist))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isLive {
		t.Fatal("expected live")
	}
}
