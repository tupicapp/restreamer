package shared_test

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/tupicapp/restreamer/core/shared"
	"github.com/tupicapp/restreamer/core/storage"
)

func TestHLSFoldersAvailableInputHLSURLs(t *testing.T) {
	folders := shared.NewHLSFolders()

	for _, inputID := range []string{"input-b", "input-c"} {
		dir := filepath.Join(t.TempDir(), inputID)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) error = %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "stream.m3u8"), []byte("#EXTM3U\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(stream.m3u8) error = %v", err)
		}
		if err := folders.SetInputHLSFolder(inputID, storage.NewFolder(dir)); err != nil {
			t.Fatalf("SetInputHLSFolder(%q) error = %v", inputID, err)
		}
	}

	got := folders.AvailableInputHLSURLs([]string{"input-c", "input-b"}, "stream.m3u8", "/v1/restream/hls")
	want := []string{
		"/v1/restream/hls/inputs/input-b/stream.m3u8",
		"/v1/restream/hls/inputs/input-c/stream.m3u8",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("AvailableInputHLSURLs() mismatch:\n got=%v\nwant=%v", got, want)
	}
}

func TestHLSFoldersInputRecordHLSURLsIncludesMappedAndDiscoveredFolders(t *testing.T) {
	folders := shared.NewHLSFolders()

	mappedRoot := filepath.Join(t.TempDir(), "mapped")
	mappedSession := filepath.Join(mappedRoot, "0002")
	if err := os.MkdirAll(mappedSession, 0o755); err != nil {
		t.Fatalf("MkdirAll(mappedSession) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(mappedSession, "stream.m3u8"), []byte("#EXTM3U\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(mapped playlist) error = %v", err)
	}
	if err := folders.SetInputRecordFolder("input-a", storage.NewFolder(mappedRoot)); err != nil {
		t.Fatalf("SetInputRecordFolder(mapped) error = %v", err)
	}

	recordRoot := filepath.Join(t.TempDir(), "input-record-root")
	discoveredSession := filepath.Join(recordRoot, "input-b", "0003")
	if err := os.MkdirAll(discoveredSession, 0o755); err != nil {
		t.Fatalf("MkdirAll(discoveredSession) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(discoveredSession, "stream.m3u8"), []byte("#EXTM3U\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(discovered playlist) error = %v", err)
	}
	if err := folders.SetInputRecordRootFolder(storage.NewFolder(recordRoot)); err != nil {
		t.Fatalf("SetInputRecordRootFolder() error = %v", err)
	}

	got := folders.InputRecordHLSURLs([]string{"input-a", "input-b"}, "stream.m3u8", "/v1/restream/records")
	want := []string{
		"/v1/restream/records/inputs/input-a/0002/stream.m3u8",
		"/v1/restream/records/inputs/input-b/0003/stream.m3u8",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("InputRecordHLSURLs() mismatch:\n got=%v\nwant=%v", got, want)
	}
}

func TestHLSFoldersInputPlaylistAndOpenInputFile(t *testing.T) {
	folders := shared.NewHLSFolders()
	dir := filepath.Join(t.TempDir(), "input-a")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", dir, err)
	}
	playlist := "#EXTM3U\n#EXTINF:2.0,\nseg_000001.ts\n"
	if err := os.WriteFile(filepath.Join(dir, "stream.m3u8"), []byte(playlist), 0o644); err != nil {
		t.Fatalf("WriteFile(stream.m3u8) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "seg_000001.ts"), []byte("segment"), 0o644); err != nil {
		t.Fatalf("WriteFile(segment) error = %v", err)
	}
	if err := folders.SetInputHLSFolder("input-a", storage.NewFolder(dir)); err != nil {
		t.Fatalf("SetInputHLSFolder() error = %v", err)
	}

	rewritten, err := folders.InputHLSPlaylist("input-a", "stream.m3u8", "/hls/inputs/input-a")
	if err != nil {
		t.Fatalf("InputHLSPlaylist() error = %v", err)
	}
	if !strings.Contains(rewritten, "/hls/inputs/input-a/seg_000001.ts") {
		t.Fatalf("expected rewritten segment URL, got %q", rewritten)
	}

	rc, contentType, err := folders.OpenInputHLSFile("input-a", "seg_000001.ts", "stream.m3u8")
	if err != nil {
		t.Fatalf("OpenInputHLSFile() error = %v", err)
	}
	defer rc.Close()

	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(segment) error = %v", err)
	}
	if string(body) != "segment" {
		t.Fatalf("segment body mismatch: got %q", string(body))
	}
	if contentType != "video/mp2t" {
		t.Fatalf("content type mismatch: got %q want %q", contentType, "video/mp2t")
	}
}
