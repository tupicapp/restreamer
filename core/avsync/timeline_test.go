package avsync

import (
	"testing"
	"time"
)

func TestTimelineKeepsSharedOriginAcrossTracks(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	timeline, err := NewTimeline(25, 48000, 1024)
	if err != nil {
		t.Fatalf("NewTimeline() error = %v", err)
	}

	audio := timeline.NextAudio(base)
	video := timeline.NextVideo(base.Add(80 * time.Millisecond))

	if audio.PTS != 0 {
		t.Fatalf("unexpected first audio pts: got %v want 0", audio.PTS)
	}
	if video.PTS != 80*time.Millisecond {
		t.Fatalf("unexpected first video pts: got %v want 80ms", video.PTS)
	}
	if !video.Timestamp.Equal(base.Add(80 * time.Millisecond)) {
		t.Fatalf("unexpected video timestamp: got %v want %v", video.Timestamp, base.Add(80*time.Millisecond))
	}
}

func TestTimelineMaintainsMonotonicPerTrack(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	timeline, err := NewTimeline(25, 48000, 1024)
	if err != nil {
		t.Fatalf("NewTimeline() error = %v", err)
	}

	first := timeline.NextVideo(base)
	second := timeline.NextVideo(base)
	third := timeline.NextVideo(base.Add(10 * time.Millisecond))

	if first.PTS != 0 {
		t.Fatalf("unexpected first pts: got %v want 0", first.PTS)
	}
	if second.PTS != 40*time.Millisecond {
		t.Fatalf("unexpected second pts: got %v want 40ms", second.PTS)
	}
	if third.PTS != 80*time.Millisecond {
		t.Fatalf("unexpected third pts: got %v want 80ms", third.PTS)
	}
}

func TestTimelineAudioUsesConfiguredAccessUnitDuration(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	timeline, err := NewTimeline(25, 48000, 1024)
	if err != nil {
		t.Fatalf("NewTimeline() error = %v", err)
	}

	first := timeline.NextAudio(base)
	second := timeline.NextAudio(base)

	wantDuration := time.Duration(1024) * time.Second / 48000
	if first.Duration != wantDuration {
		t.Fatalf("unexpected audio duration: got %v want %v", first.Duration, wantDuration)
	}
	if second.PTS != wantDuration {
		t.Fatalf("unexpected second audio pts: got %v want %v", second.PTS, wantDuration)
	}
}
