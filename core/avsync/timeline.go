package avsync

import (
	"fmt"
	"sync"
	"time"
)

type FrameTiming struct {
	PTS       time.Duration
	DTS       time.Duration
	Duration  time.Duration
	Timestamp time.Time
}

type Timeline struct {
	mu sync.Mutex

	videoDuration time.Duration
	audioDuration time.Duration

	started   bool
	startTime time.Time

	nextVideoIndex int64
	nextAudioIndex int64
}

func NewTimeline(videoFPS int, audioSampleRate int, audioSamplesPerAU int) (*Timeline, error) {
	if videoFPS <= 0 {
		return nil, fmt.Errorf("invalid video fps %d", videoFPS)
	}
	if audioSampleRate <= 0 {
		return nil, fmt.Errorf("invalid audio sample rate %d", audioSampleRate)
	}
	if audioSamplesPerAU <= 0 {
		return nil, fmt.Errorf("invalid audio samples per access unit %d", audioSamplesPerAU)
	}

	return &Timeline{
		videoDuration: time.Second / time.Duration(videoFPS),
		audioDuration: time.Duration(audioSamplesPerAU) * time.Second / time.Duration(audioSampleRate),
	}, nil
}

func (t *Timeline) NextVideo(now time.Time) FrameTiming {
	return t.next(now, t.videoDuration, &t.nextVideoIndex)
}

func (t *Timeline) NextAudio(now time.Time) FrameTiming {
	return t.next(now, t.audioDuration, &t.nextAudioIndex)
}

func (t *Timeline) StartTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.startTime
}

func (t *Timeline) next(now time.Time, duration time.Duration, nextIndex *int64) FrameTiming {
	t.mu.Lock()
	defer t.mu.Unlock()

	if now.IsZero() {
		now = time.Now()
	}
	if !t.started {
		t.started = true
		t.startTime = now
	}

	elapsed := now.Sub(t.startTime)
	if elapsed < 0 {
		elapsed = 0
	}

	candidateIndex := int64(0)
	if duration > 0 {
		candidateIndex = int64(elapsed / duration)
	}
	if candidateIndex < *nextIndex {
		candidateIndex = *nextIndex
	}

	pts := time.Duration(candidateIndex) * duration
	*nextIndex = candidateIndex + 1

	return FrameTiming{
		PTS:       pts,
		DTS:       pts,
		Duration:  duration,
		Timestamp: t.startTime.Add(pts),
	}
}
