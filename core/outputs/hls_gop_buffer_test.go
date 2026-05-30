package outputs

import (
	"testing"
	"time"

	"github.com/tupicapp/restreamer/core/shared"
)

func drainHLSReadyFrames(ch <-chan *shared.Frame, d time.Duration) []*shared.Frame {
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

func TestHLSGOPBuffer_SwitchRequiresDecodableKeyframeAndHoldsAudio(t *testing.T) {
	b := newHLSGOPBuffer()
	defer b.Close()
	go b.Run()

	now := time.Now()
	video := func(inputID string, seq int64, pts time.Duration, key bool, payload [][]byte) *shared.Frame {
		return &shared.Frame{
			InputID:    inputID,
			Codec:      "h264",
			Payload:    payload,
			IsKeyFrame: key,
			PTS:        pts,
			DTS:        pts,
			Duration:   33 * time.Millisecond,
			SequenceID: seq,
			Timestamp:  now,
		}
	}
	audio := func(inputID string, seq int64, pts time.Duration) *shared.Frame {
		return &shared.Frame{
			InputID:    inputID,
			Codec:      "aac",
			Payload:    [][]byte{{0x11, 0x22}},
			IsKeyFrame: true,
			PTS:        pts,
			DTS:        pts,
			Duration:   23 * time.Millisecond,
			SequenceID: seq,
			Timestamp:  now,
		}
	}

	b.VideoFrameChan <- video("A", 1, 100*time.Millisecond, true, [][]byte{
		{0x67, 0x42, 0x00, 0x1f},
		{0x68, 0xce, 0x38, 0x80},
		{0x65, 0x88, 0x84},
	})
	b.AudioFrameChan <- audio("A", 1, 123*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	_ = drainHLSReadyFrames(b.GetReadyChan(), 60*time.Millisecond)

	b.AudioFrameChan <- audio("B", 1, 200*time.Millisecond)
	b.VideoFrameChan <- video("B", 1, 200*time.Millisecond, false, [][]byte{{0x41, 0x9a, 0x22}})
	b.VideoFrameChan <- video("B", 2, 233*time.Millisecond, true, [][]byte{{0x65, 0x88, 0x84}})
	b.AudioFrameChan <- audio("B", 2, 246*time.Millisecond)

	time.Sleep(80 * time.Millisecond)

	before := drainHLSReadyFrames(b.GetReadyChan(), 80*time.Millisecond)
	for _, f := range before {
		if f.InputID == "B" {
			t.Fatalf("input B leaked before decodable keyframe: codec=%s seq=%d key=%v", f.Codec, f.SequenceID, f.IsKeyFrame)
		}
	}

	b.VideoFrameChan <- video("B", 3, 280*time.Millisecond, true, [][]byte{
		{0x67, 0x4d, 0x00, 0x1f},
		{0x68, 0xee, 0x3c, 0x80},
		{0x65, 0x88, 0x84},
	})
	b.AudioFrameChan <- audio("B", 3, 303*time.Millisecond)

	time.Sleep(140 * time.Millisecond)

	after := drainHLSReadyFrames(b.GetReadyChan(), 150*time.Millisecond)
	foundBKey := false
	var bKeyPTS time.Duration
	for _, f := range after {
		if f.InputID == "B" && f.Codec == "h264" && f.IsKeyFrame {
			foundBKey = true
			bKeyPTS = f.PTS
			if !f.Discontinuity {
				t.Fatal("expected committed switch keyframe to be marked as discontinuity")
			}
			break
		}
	}
	if !foundBKey {
		t.Fatalf("expected decodable B keyframe in output, got %d frames", len(after))
	}

	for _, f := range after {
		if f.InputID == "B" && f.Codec == "aac" && f.PTS < bKeyPTS {
			t.Fatalf("audio from B preceded decodable cut: audioPTS=%v keyPTS=%v", f.PTS, bKeyPTS)
		}
		if f.InputID == "A" {
			t.Fatalf("old-input frame leaked after discontinuity commit: codec=%s seq=%d pts=%v", f.Codec, f.SequenceID, f.PTS)
		}
	}
}

func TestHLSTimelineRebaser_AudioTimestampResetDoesNotDragVideoTimeline(t *testing.T) {
	r := newHLSTimelineRebaser()

	video := func(seq int64, pts time.Duration, key bool) *shared.Frame {
		return &shared.Frame{
			InputID:    "A",
			Codec:      "h264",
			Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
			IsKeyFrame: key,
			PTS:        pts,
			DTS:        pts,
			Duration:   33 * time.Millisecond,
			SequenceID: seq,
		}
	}
	audio := func(seq int64, pts time.Duration) *shared.Frame {
		return &shared.Frame{
			InputID:    "A",
			Codec:      "aac",
			Payload:    [][]byte{{0x11, 0x22}},
			IsKeyFrame: true,
			PTS:        pts,
			DTS:        pts,
			Duration:   23 * time.Millisecond,
			SequenceID: seq,
		}
	}

	out1 := r.Process(video(1, 100*time.Millisecond, true))
	out2 := r.Process(audio(1, 123*time.Millisecond))
	out3 := r.Process(video(2, 133*time.Millisecond, false))
	out4 := r.Process(audio(2, 10*time.Millisecond))
	out5 := r.Process(video(3, 166*time.Millisecond, false))

	if len(out1) != 1 || len(out2) != 1 || len(out3) != 1 || len(out4) != 1 || len(out5) != 1 {
		t.Fatalf("expected one output frame per input, got %d %d %d %d %d", len(out1), len(out2), len(out3), len(out4), len(out5))
	}

	if out3[0].PTS != 33*time.Millisecond {
		t.Fatalf("expected second video frame to stay on continuous timeline, got %v", out3[0].PTS)
	}
	if out5[0].PTS > 100*time.Millisecond {
		t.Fatalf("expected video timeline to remain near continuity after audio reset, got %v", out5[0].PTS)
	}
	if out4[0].PTS < out2[0].PTS {
		t.Fatalf("expected rebased audio to stay monotonic after source reset, got %v then %v", out2[0].PTS, out4[0].PTS)
	}

	skew := out5[0].PTS - out4[0].PTS
	if skew < 0 {
		skew = -skew
	}
	if skew > 60*time.Millisecond {
		t.Fatalf("expected audio/video skew to stay bounded after audio reset, got %v", skew)
	}
}

func TestHLSTimelineRebaser_SwitchAudioAnchorIsSmoothed(t *testing.T) {
	r := newHLSTimelineRebaser()

	video := func(inputID string, seq int64, pts time.Duration, key bool) *shared.Frame {
		payload := [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}}
		if !key {
			payload = [][]byte{{0x41, 0x9a, 0x22}}
		}
		return &shared.Frame{
			InputID:    inputID,
			Codec:      "h264",
			Payload:    payload,
			IsKeyFrame: key,
			PTS:        pts,
			DTS:        pts,
			Duration:   33 * time.Millisecond,
			SequenceID: seq,
		}
	}
	audio := func(inputID string, seq int64, pts time.Duration) *shared.Frame {
		return &shared.Frame{
			InputID:    inputID,
			Codec:      "aac",
			Payload:    [][]byte{{0x11, 0x22}},
			IsKeyFrame: true,
			PTS:        pts,
			DTS:        pts,
			Duration:   23 * time.Millisecond,
			SequenceID: seq,
		}
	}

	_ = r.Process(video("A", 1, 100*time.Millisecond, true))
	outA1 := r.Process(audio("A", 1, 123*time.Millisecond))
	outA2 := r.Process(audio("A", 2, 146*time.Millisecond))
	_ = r.Process(video("A", 2, 133*time.Millisecond, false))

	_ = r.Process(audio("B", 1, 700*time.Millisecond))
	switchOut := r.Process(video("B", 1, 180*time.Millisecond, true))
	lateAudioOut := r.Process(audio("B", 2, 730*time.Millisecond))

	if len(outA1) != 1 || len(outA2) != 1 {
		t.Fatalf("expected one output frame from steady-state audio packets")
	}

	var outBKey *shared.Frame
	var firstBAudio *shared.Frame
	for _, f := range switchOut {
		if f == nil {
			continue
		}
		if f.Codec == "h264" && outBKey == nil {
			outBKey = f
		}
		if f.Codec == "aac" && firstBAudio == nil {
			firstBAudio = f
		}
	}
	for _, f := range lateAudioOut {
		if f != nil && f.Codec == "aac" && firstBAudio == nil {
			firstBAudio = f
		}
	}

	if outBKey == nil || firstBAudio == nil {
		t.Fatalf("expected switch output to contain committed keyframe and audio")
	}

	audioJump := firstBAudio.PTS - outA2[0].PTS
	if audioJump <= 0 {
		t.Fatalf("expected forward audio progress after switch, got %v", audioJump)
	}
	if audioJump > 100*time.Millisecond {
		t.Fatalf("expected smoothed post-switch audio anchor, got jump %v", audioJump)
	}

	if firstBAudio.PTS < outBKey.PTS {
		t.Fatalf("expected switched audio to remain at or after switch keyframe, got audio=%v key=%v", firstBAudio.PTS, outBKey.PTS)
	}
}
