package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFolderObjectURLUsesConfiguredPublicBaseURL(t *testing.T) {
	folder := NewFolder("/tmp/hls", WithPublicBaseURL("https://cdn.example.com/live/output"))

	got, err := folder.ObjectURL("stream.m3u8")
	if err != nil {
		t.Fatalf("ObjectURL failed: %v", err)
	}
	if got != "https://cdn.example.com/live/output/stream.m3u8" {
		t.Fatalf("unexpected object url: %q", got)
	}

	child := folder.Folder("nested")
	got, err = child.ObjectURL("seg_000001.ts")
	if err != nil {
		t.Fatalf("child ObjectURL failed: %v", err)
	}
	if got != "https://cdn.example.com/live/output/nested/seg_000001.ts" {
		t.Fatalf("unexpected child object url: %q", got)
	}
}

func TestFolderCleanPreservesPlaylistFiles(t *testing.T) {
	baseDir := t.TempDir()
	folder := NewFolder(baseDir)

	playlistPath := filepath.Join(baseDir, "stream.m3u8")
	segmentPath := filepath.Join(baseDir, "seg_000001.ts")

	if err := os.WriteFile(playlistPath, []byte("#EXTM3U\n"), 0644); err != nil {
		t.Fatalf("write playlist: %v", err)
	}
	if err := os.WriteFile(segmentPath, []byte("segment"), 0644); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	oldTime := time.Now().Add(-10 * time.Minute)
	if err := os.Chtimes(playlistPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes playlist: %v", err)
	}
	if err := os.Chtimes(segmentPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes segment: %v", err)
	}

	removed, err := folder.Clean(time.Now().Add(-time.Minute), 0)
	if err != nil {
		t.Fatalf("Clean failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("unexpected removed count: got %d want 1", removed)
	}

	if _, err := os.Stat(playlistPath); err != nil {
		t.Fatalf("playlist should remain after clean: %v", err)
	}
	if _, err := os.Stat(segmentPath); !os.IsNotExist(err) {
		t.Fatalf("segment should be removed, stat err=%v", err)
	}
}
