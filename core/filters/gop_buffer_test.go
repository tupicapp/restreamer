package filters

import (
	"github.com/tupicapp/restreamer/core/shared"
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

func TestTimelineRebaser_DoesNotReanchorContinuousStream(t *testing.T) {
	r := NewTimelineRebaser()

	var outs []*shared.Frame
	for i := 0; i < 120; i++ {
		pts := 100*time.Millisecond + time.Duration(i)*33*time.Millisecond
		out := r.Process(mkVideo("A", int64(i+1), pts, i == 0))
		outs = append(outs, out...)
	}

	if len(outs) != 120 {
		t.Fatalf("expected 120 output frames, got %d", len(outs))
	}

	for i := 1; i < len(outs); i++ {
		gotDelta := outs[i].PTS - outs[i-1].PTS
		if gotDelta != 33*time.Millisecond {
			t.Fatalf("frame %d delta changed in continuous stream: got %v want %v", i, gotDelta, 33*time.Millisecond)
		}
	}

	wantEnd := time.Duration(119) * 33 * time.Millisecond
	if outs[len(outs)-1].PTS != wantEnd {
		t.Fatalf("continuous stream was unexpectedly re-anchored: got end %v want %v", outs[len(outs)-1].PTS, wantEnd)
	}
}

func TestTimelineRebaser_ReanchorsRealSourceJump(t *testing.T) {
	r := NewTimelineRebaser()

	out1 := r.Process(mkVideo("A", 1, 100*time.Millisecond, true))
	out2 := r.Process(mkVideo("A", 2, 133*time.Millisecond, false))
	out3 := r.Process(mkVideo("A", 3, 10*time.Second, false))
	out4 := r.Process(mkVideo("A", 4, 10033*time.Millisecond, false))

	if len(out1) != 1 || len(out2) != 1 || len(out3) != 1 || len(out4) != 1 {
		t.Fatalf("expected one output frame per input, got %d %d %d %d", len(out1), len(out2), len(out3), len(out4))
	}

	if out1[0].PTS != 0 {
		t.Fatalf("expected first rebased frame at zero, got %v", out1[0].PTS)
	}
	if out2[0].PTS != 33*time.Millisecond {
		t.Fatalf("expected second frame to continue normally, got %v", out2[0].PTS)
	}
	if out3[0].PTS != 66*time.Millisecond {
		t.Fatalf("expected large source jump to re-anchor near continuity point, got %v", out3[0].PTS)
	}
	if out4[0].PTS != 99*time.Millisecond {
		t.Fatalf("expected post-jump frame to continue from re-anchored base, got %v", out4[0].PTS)
	}
}
