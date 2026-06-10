package inputs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestHLS(t *testing.T) *hlsInput {
	t.Helper()

	reader, ok := NewHLS("unit-reader", "http://example.com/playlist.m3u8").(*hlsInput)
	if !ok || reader == nil {
		t.Fatal("expected hlsInput instance")
	}

	return reader
}

func assertFrameDTSOrder(t *testing.T, frames []*Frame, want []time.Duration) {
	t.Helper()

	if len(frames) != len(want) {
		t.Fatalf("expected %d frames, got %d", len(want), len(frames))
	}

	for i, frame := range frames {
		if frame.DTS != want[i] {
			t.Fatalf("frame %d DTS = %v, want %v", i, frame.DTS, want[i])
		}
	}
}

func TestNormalizeHLSURI(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "playlist.m3u8")
	if err := os.WriteFile(filePath, []byte("#EXTM3U\n"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	tests := []struct {
		name  string
		input string
		check func(*testing.T, string)
	}{
		{
			name:  "local file path",
			input: filePath,
			check: func(t *testing.T, got string) {
				t.Helper()
				if !strings.HasPrefix(got, "file://") {
					t.Fatalf("expected file:// URI, got %s", got)
				}
			},
		},
		{
			name:  "remote URI",
			input: "http://example.com/playlist.m3u8",
			check: func(t *testing.T, got string) {
				t.Helper()
				if got != "http://example.com/playlist.m3u8" {
					t.Fatalf("expected remote URI to be unchanged, got %s", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized, err := normalizeHLSURI(tt.input)
			if err != nil {
				t.Fatalf("normalizeHLSURI failed: %v", err)
			}

			tt.check(t, normalized)
		})
	}
}

func TestHLSDownloadPlaylistFromFileURI(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "playlist.m3u8")
	want := []byte("#EXTM3U\n#EXT-X-VERSION:3\n")
	if err := os.WriteFile(playlistPath, want, 0o644); err != nil {
		t.Fatalf("failed to create playlist: %v", err)
	}

	reader, ok := NewHLS("unit-reader", playlistPath).(*hlsInput)
	if !ok || reader == nil {
		t.Fatal("expected hlsInput instance")
	}

	got, err := reader.downloadPlaylist(reader.baseURL.String())
	if err != nil {
		t.Fatalf("downloadPlaylist failed: %v", err)
	}

	if string(got) != string(want) {
		t.Fatalf("downloadPlaylist returned %q, want %q", string(got), string(want))
	}
}

func TestHLSStateMarksRemovableAfterEndListIsDrained(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "playlist.m3u8")
	content := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nseg0.ts\n#EXT-X-ENDLIST\n"
	if err := os.WriteFile(playlistPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to create playlist: %v", err)
	}

	reader, ok := NewHLS("unit-reader", playlistPath).(*hlsInput)
	if !ok || reader == nil {
		t.Fatal("expected hlsInput instance")
	}

	if err := reader.updateMediaPlaylist(); err != nil {
		t.Fatalf("updateMediaPlaylist failed: %v", err)
	}

	if got := reader.State().IsRemovable; got {
		t.Fatal("expected queued final segment to keep hls input non-removable")
	}

	select {
	case <-reader.segmentsChan:
	default:
		t.Fatal("expected queued final segment")
	}

	if got := reader.State().IsRemovable; !got {
		t.Fatal("expected hls input to become removable after endlist is drained")
	}
}

func TestHLSStateNotRemovableWithoutEndList(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "playlist.m3u8")
	content := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nseg0.ts\n"
	if err := os.WriteFile(playlistPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to create playlist: %v", err)
	}

	reader, ok := NewHLS("unit-reader", playlistPath).(*hlsInput)
	if !ok || reader == nil {
		t.Fatal("expected hlsInput instance")
	}

	if err := reader.updateMediaPlaylist(); err != nil {
		t.Fatalf("updateMediaPlaylist failed: %v", err)
	}

	select {
	case <-reader.segmentsChan:
	default:
		t.Fatal("expected queued segment")
	}

	if got := reader.State().IsRemovable; got {
		t.Fatal("expected hls input without endlist to stay non-removable")
	}
}

func TestHLSLoopModeRequeuesEndListPlaylistAfterDrain(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "playlist.m3u8")
	content := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nseg0.ts\n#EXTINF:2,\nseg1.ts\n#EXT-X-ENDLIST\n"
	if err := os.WriteFile(playlistPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to create playlist: %v", err)
	}

	reader, ok := NewHLS("unit-reader", playlistPath, WithLoop()).(*hlsInput)
	if !ok || reader == nil {
		t.Fatal("expected hlsInput instance")
	}

	if err := reader.updateMediaPlaylist(); err != nil {
		t.Fatalf("updateMediaPlaylist failed: %v", err)
	}

	if got := len(reader.segmentsChan); got != 2 {
		t.Fatalf("expected 2 queued segments on first pass, got %d", got)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-reader.segmentsChan:
		default:
			t.Fatalf("expected queued segment %d", i)
		}
	}

	if got := reader.State().IsRemovable; got {
		t.Fatal("expected loop-enabled hls input to stay non-removable after drain")
	}

	if err := reader.updateMediaPlaylist(); err != nil {
		t.Fatalf("second updateMediaPlaylist failed: %v", err)
	}

	if got := len(reader.segmentsChan); got != 2 {
		t.Fatalf("expected 2 queued segments after loop reset, got %d", got)
	}
}

func TestHLSLoopModeLeavesFrameTimestampsUnchangedAcrossPasses(t *testing.T) {
	reader := newTestHLS(t)
	reader.loopEnabled = true

	reader.enqueuePendingVideo(&Frame{
		PTS:      2 * time.Second,
		DTS:      2 * time.Second,
		Duration: 500 * time.Millisecond,
	})

	reader.pendingMu.Lock()
	if got := reader.pendingVideoBuf[0].PTS; got != 2*time.Second {
		reader.pendingMu.Unlock()
		t.Fatalf("first pass PTS = %v, want %v", got, 2*time.Second)
	}
	reader.pendingVideoBuf = nil
	reader.videoSegmentCounts = nil
	reader.pendingMu.Unlock()

	reader.stateMu.Lock()
	reader.sawEndList = true
	reader.stateMu.Unlock()
	reader.prepareLoopReset()

	reader.enqueuePendingVideo(&Frame{
		PTS:      0,
		DTS:      0,
		Duration: 400 * time.Millisecond,
	})

	reader.pendingMu.Lock()
	defer reader.pendingMu.Unlock()

	if len(reader.pendingVideoBuf) != 1 {
		t.Fatalf("expected one buffered frame on second pass, got %d", len(reader.pendingVideoBuf))
	}
	if got := reader.pendingVideoBuf[0].PTS; got != 0 {
		t.Fatalf("second pass PTS = %v, want %v", got, 0*time.Second)
	}
	if got := reader.pendingVideoBuf[0].DTS; got != 0 {
		t.Fatalf("second pass DTS = %v, want %v", got, 0*time.Second)
	}
}
