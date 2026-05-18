package filters

import (
	"restreamer/irajstreamer/core/shared"
	"testing"
	"time"
)

func mkVideo(inputID string, seq int64, pts time.Duration, isKey bool) *shared.Frame {
	return &shared.Frame{
		PTS:        pts,
		DTS:        pts,
		Duration:   33 * time.Millisecond,
		Payload:    [][]byte{[]byte("v")},
		Codec:      "h264",
		Timestamp:  time.Now(),
		InputID:    inputID,
		IsKeyFrame: isKey,
		SequenceID: seq,
		GOPID:      0,
	}
}

func mkAudio(inputID string, seq int64, pts time.Duration) *shared.Frame {
	return &shared.Frame{
		PTS:        pts,
		DTS:        pts,
		Duration:   23 * time.Millisecond,
		Payload:    [][]byte{[]byte("a")},
		Codec:      "aac",
		Timestamp:  time.Now(),
		InputID:    inputID,
		IsKeyFrame: true,
		SequenceID: seq,
		GOPID:      seq,
	}
}

func drainFrames(ch <-chan *shared.Frame, d time.Duration) []*shared.Frame {
	var out []*shared.Frame
	deadline := time.After(d)
	for {
		select {
		case f := <-ch:
			if f != nil {
				out = append(out, f)
			}
		case <-deadline:
			return out
		}
	}
}

func TestGOPBuffer_SwitchGatedOnVideoKeyframe_AudioHeldForSync(t *testing.T) {
	b := NewGOPBuffer(true, true, true)
	defer b.Close()
	go b.Run()

	// Input A: send keyframe + a bit of audio after it.
	b.AudioFrameChan <- mkAudio("A", 1, 100*time.Millisecond)
	b.VideoFrameChan <- mkVideo("A", 1, 100*time.Millisecond, true)
	b.VideoFrameChan <- mkVideo("A", 2, 133*time.Millisecond, false)
	b.AudioFrameChan <- mkAudio("A", 2, 123*time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	// Start switch to B: audio arrives first; video arrives but without keyframe: must not leak to output.
	b.AudioFrameChan <- mkAudio("B", 1, 200*time.Millisecond)
	b.VideoFrameChan <- mkVideo("B", 1, 200*time.Millisecond, false)
	b.AudioFrameChan <- mkAudio("B", 2, 223*time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	// Drain any emitted frames so far.
	v1 := drainFrames(b.VideoReadyChan, 50*time.Millisecond)
	a1 := drainFrames(b.AudioReadyChan, 50*time.Millisecond)

	for _, f := range v1 {
		if f.InputID == "B" {
			t.Fatalf("video from B leaked before keyframe: seq=%d pts=%v key=%v", f.SequenceID, f.PTS, f.IsKeyFrame)
		}
	}
	for _, f := range a1 {
		if f.InputID == "B" {
			t.Fatalf("audio from B leaked before video keyframe: seq=%d pts=%v", f.SequenceID, f.PTS)
		}
	}

	// Now deliver B keyframe: should commit the switch, then release buffered B audio at/after that cut.
	b.VideoFrameChan <- mkVideo("B", 2, 240*time.Millisecond, true)
	b.AudioFrameChan <- mkAudio("B", 3, 246*time.Millisecond)
	b.VideoFrameChan <- mkVideo("B", 3, 273*time.Millisecond, false)

	time.Sleep(150 * time.Millisecond)

	v2 := drainFrames(b.VideoReadyChan, 200*time.Millisecond)
	a2 := drainFrames(b.AudioReadyChan, 200*time.Millisecond)

	// Must contain B keyframe.
	foundBKey := false
	var bKeyPTS time.Duration
	for _, f := range v2 {
		if f.InputID == "B" && f.IsKeyFrame {
			foundBKey = true
			bKeyPTS = f.PTS
			break
		}
	}
	if !foundBKey {
		t.Fatalf("expected to find B keyframe in output, got video=%d audio=%d", len(v2), len(a2))
	}

	// Audio from B must not precede B keyframe in output timeline.
	for _, f := range a2 {
		if f.InputID != "B" {
			continue
		}
		if f.PTS < bKeyPTS {
			t.Fatalf("audio from B precedes B keyframe (A/V desync): audioPTS=%v keyPTS=%v", f.PTS, bKeyPTS)
		}
	}

	// Monotonic PTS per track.
	var lastV, lastA time.Duration
	for _, f := range v2 {
		if f.PTS < lastV {
			t.Fatalf("non-monotonic video pts: %v < %v", f.PTS, lastV)
		}
		lastV = f.PTS
	}
	for _, f := range a2 {
		if f.PTS < lastA {
			t.Fatalf("non-monotonic audio pts: %v < %v", f.PTS, lastA)
		}
		lastA = f.PTS
	}
}

func TestGOPBuffer_VideoUsesDTSForOutputOrder(t *testing.T) {
	b := NewGOPBufferWithOptions(true, true, true, false, false)
	defer b.Close()
	go b.Run()

	mk := func(seq int64, pts, dts time.Duration) *shared.Frame {
		return &shared.Frame{
			PTS:        pts,
			DTS:        dts,
			Duration:   33 * time.Millisecond,
			Payload:    [][]byte{[]byte("v")},
			Codec:      "h264",
			InputID:    "A",
			IsKeyFrame: seq == 1,
			SequenceID: seq,
		}
	}

	// B-frame style timing: display order (PTS) differs from decode order (DTS).
	b.VideoFrameChan <- mk(1, 66*time.Millisecond, 33*time.Millisecond)
	b.VideoFrameChan <- mk(2, 33*time.Millisecond, 66*time.Millisecond)
	b.VideoFrameChan <- mk(3, 99*time.Millisecond, 99*time.Millisecond)

	time.Sleep(120 * time.Millisecond)

	frames := drainFrames(b.VideoReadyChan, 80*time.Millisecond)
	if len(frames) < 3 {
		t.Fatalf("expected at least 3 video frames, got %d", len(frames))
	}

	if frames[0].DTS > frames[1].DTS || frames[1].DTS > frames[2].DTS {
		t.Fatalf("video DTS order is not monotonic: %v, %v, %v", frames[0].DTS, frames[1].DTS, frames[2].DTS)
	}
}
