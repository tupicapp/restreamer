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

func TestHLSReader_PopSortedBuffers(t *testing.T) {
	t.Run("video", func(t *testing.T) {
		reader := newTestHLS(t)
		reader.pendingVideoBuf = []*Frame{
			{DTS: 30 * time.Millisecond},
			{DTS: 10 * time.Millisecond},
			{DTS: 20 * time.Millisecond},
			{DTS: 40 * time.Millisecond},
		}

		batch := reader.popSortedVideo(4, 2)

		assertFrameDTSOrder(t, batch, []time.Duration{
			10 * time.Millisecond,
			20 * time.Millisecond,
		})

		if len(reader.pendingVideoBuf) != 2 {
			t.Fatalf("expected 2 remaining video frames, got %d", len(reader.pendingVideoBuf))
		}
	})

	t.Run("audio", func(t *testing.T) {
		reader := newTestHLS(t)
		reader.pendingAudioBuf = []*Frame{
			{DTS: 15 * time.Millisecond},
			{DTS: 5 * time.Millisecond},
			{DTS: 25 * time.Millisecond},
			{DTS: 35 * time.Millisecond},
		}

		batch := reader.popSortedAudio(4, 3)

		assertFrameDTSOrder(t, batch, []time.Duration{
			5 * time.Millisecond,
			15 * time.Millisecond,
			25 * time.Millisecond,
		})

		if len(reader.pendingAudioBuf) != 1 {
			t.Fatalf("expected 1 remaining audio frame, got %d", len(reader.pendingAudioBuf))
		}
	})

	t.Run("below sort size returns nil", func(t *testing.T) {
		reader := newTestHLS(t)
		reader.pendingVideoBuf = []*Frame{
			{DTS: 10 * time.Millisecond},
			{DTS: 20 * time.Millisecond},
			{DTS: 30 * time.Millisecond},
		}

		if batch := reader.popSortedVideo(4, 2); batch != nil {
			t.Fatalf("expected nil when buffer size below sortSize")
		}
	})
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
