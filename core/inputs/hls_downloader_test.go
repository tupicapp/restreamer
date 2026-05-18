package inputs

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHlsDownload_MasterAndMediaPlaylist(t *testing.T) {
	master := "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\nstream_2.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=200000\nstream_3.m3u8\n"
	media := "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4,\nseg1.m4s\n#EXT-X-ENDLIST\n"
	initPayload := []byte("init")
	segPayload := []byte("segment")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/master.m3u8":
			_, _ = w.Write([]byte(master))
		case "/stream_2.m3u8":
			_, _ = w.Write([]byte(media))
		case "/init.mp4":
			_, _ = w.Write(initPayload)
		case "/seg1.m4s":
			_, _ = w.Write(segPayload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outDir := t.TempDir()
	if err := HlsDownload(server.URL+"/master.m3u8", outDir); err != nil {
		t.Fatalf("HlsDownload failed: %v", err)
	}

	assertFileExists(t, filepath.Join(outDir, "master.m3u8"))
	assertFileExists(t, filepath.Join(outDir, "stream_2", "playlist.m3u8"))
	assertFileExists(t, filepath.Join(outDir, "stream_2", "init.mp4"))
	assertFileExists(t, filepath.Join(outDir, "stream_2", "seg1.m4s"))
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected file %s to exist: %v", path, err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected file %s to be non-empty", path)
	}
}
