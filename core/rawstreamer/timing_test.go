package rawstreamer

import (
	"testing"
	"time"

	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

func TestDeriveOutputVideoPTSPrefersSourceTimelineOnFirstFrame(t *testing.T) {
	frame := &raw.VideoFrame{
		Frame: &shared.Frame{
			PTS: 1234 * time.Millisecond,
		},
	}

	got := deriveOutputVideoPTS(frame, -1, 40*time.Millisecond)
	if got != 1234*time.Millisecond {
		t.Fatalf("unexpected initial pts: got %v want %v", got, 1234*time.Millisecond)
	}
}

func TestDeriveOutputVideoPTSStaysMonotonicWhenSourceFrameRepeats(t *testing.T) {
	frame := &raw.VideoFrame{
		Frame: &shared.Frame{
			PTS: 2 * time.Second,
		},
	}

	got := deriveOutputVideoPTS(frame, 2*time.Second, 40*time.Millisecond)
	if got != 2040*time.Millisecond {
		t.Fatalf("unexpected repeated-frame pts: got %v want %v", got, 2040*time.Millisecond)
	}
}
