package scenes

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"restreamer/irajstreamer/core/raw"
	shared "restreamer/irajstreamer/core/shared"
)

func TestScene_EnqueueLatestAudioKeepsNewestFramesWhenOutputBacksUp(t *testing.T) {
	scene := &Scene{
		id:        "scene-audio",
		audioChan: make(chan *shared.Frame, 2),
		done:      make(chan struct{}),
	}

	for seq := int64(1); seq <= 5; seq++ {
		ok := scene.enqueueLatestAudio(&shared.Frame{
			PTS:        time.Duration(seq) * 20 * time.Millisecond,
			DTS:        time.Duration(seq) * 20 * time.Millisecond,
			Duration:   20 * time.Millisecond,
			Payload:    [][]byte{[]byte("a")},
			Codec:      "aac",
			Timestamp:  time.Now(),
			InputID:    "scene-audio",
			IsKeyFrame: true,
			SequenceID: seq,
			GOPID:      seq,
		})
		if !ok {
			t.Fatalf("enqueueLatestAudio() returned false for seq=%d", seq)
		}
	}

	var got []*shared.Frame
	for i := 0; i < 2; i++ {
		select {
		case frame := <-scene.GetAudioChan():
			if frame == nil {
				t.Fatal("scene output audio frame is nil")
			}
			got = append(got, frame)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for buffered scene audio frame %d", i+1)
		}
	}

	want := []int64{4, 5}
	if len(got) != len(want) {
		t.Fatalf("unexpected audio frame count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].SequenceID != want[i] {
			t.Fatalf("unexpected buffered audio sequence at %d: got %d want %d", i, got[i].SequenceID, want[i])
		}
	}
}

func TestScene_ConsumeAudioForwardsPassthroughFrames(t *testing.T) {
	source := newMockSceneStream("audio-src")
	scene := &Scene{
		id:        "scene-audio",
		audioChan: make(chan *shared.Frame, 1),
		done:      make(chan struct{}),
		started:   make(chan struct{}),
		cfg: config{
			audioPassthroughFrom: 0,
		},
		runtimes: []*inputRuntime{
			{spec: Input{Stream: source}},
		},
	}
	defer scene.Close()

	go scene.consumeAudio()

	want := &shared.Frame{
		PTS:        20 * time.Millisecond,
		DTS:        20 * time.Millisecond,
		Duration:   20 * time.Millisecond,
		Payload:    [][]byte{[]byte("aac")},
		Codec:      "aac",
		PacketType: "raw",
		InputID:    "audio-src",
		IsKeyFrame: true,
		SequenceID: 7,
		Timestamp:  time.Now(),
	}

	source.audioCh <- want

	select {
	case got := <-scene.GetAudioChan():
		if got == nil {
			t.Fatal("scene output audio frame is nil")
		}
		if got.SequenceID != want.SequenceID {
			t.Fatalf("unexpected audio sequence: got %d want %d", got.SequenceID, want.SequenceID)
		}
		if got.InputID != scene.id {
			t.Fatalf("unexpected audio input id: got %q want %q", got.InputID, scene.id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for passthrough audio frame")
	}
}

func TestScene_ConsumeAudioUsesSingleHundredPercentRatioAsPassthrough(t *testing.T) {
	sourceA := newMockSceneStream("audio-a")
	sourceB := newMockSceneStream("audio-b")
	scene := &Scene{
		id:        "scene-audio",
		audioChan: make(chan *shared.Frame, 1),
		done:      make(chan struct{}),
		started:   make(chan struct{}),
		cfg: config{
			audioPassthroughFrom: 0,
			audioMixRatios:       []int{0, 100},
		},
		runtimes: []*inputRuntime{
			{spec: Input{Stream: sourceA}},
			{spec: Input{Stream: sourceB}},
		},
	}
	defer scene.Close()

	if scene.shouldMixAudio() {
		t.Fatal("single 100% audio ratio should use passthrough, not mix")
	}

	go scene.consumeAudio()

	sourceA.audioCh <- &shared.Frame{
		PTS:        10 * time.Millisecond,
		DTS:        10 * time.Millisecond,
		Duration:   10 * time.Millisecond,
		Payload:    [][]byte{[]byte("a")},
		Codec:      "aac",
		InputID:    "audio-a",
		SequenceID: 1,
		Timestamp:  time.Now(),
	}
	sourceB.audioCh <- &shared.Frame{
		PTS:        20 * time.Millisecond,
		DTS:        20 * time.Millisecond,
		Duration:   10 * time.Millisecond,
		Payload:    [][]byte{[]byte("b")},
		Codec:      "aac",
		InputID:    "audio-b",
		SequenceID: 2,
		Timestamp:  time.Now(),
	}

	select {
	case got := <-scene.GetAudioChan():
		if got == nil {
			t.Fatal("scene output audio frame is nil")
		}
		if got.InputID != scene.id {
			t.Fatalf("unexpected passthrough source identity: got %q want %q", got.InputID, scene.id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for passthrough audio frame from 100% ratio input")
	}
}

func TestScene_PrepareComposedVideoFrameUsesSceneInputID(t *testing.T) {
	scene := &Scene{id: "scene-output"}
	frame := &raw.VideoFrame{
		Frame: &shared.Frame{
			InputID:    "source-input",
			Payload:    [][]byte{make([]byte, 6)},
			Codec:      raw.VideoCodec,
			PacketType: raw.YUV420PPixFmt,
		},
		Width:  2,
		Height: 2,
		PixFmt: raw.YUV420PPixFmt,
	}

	timestamp := time.Now()
	scene.prepareComposedVideoFrame(frame, 80*time.Millisecond, 40*time.Millisecond, timestamp)

	if frame.Frame.InputID != scene.id {
		t.Fatalf("unexpected composed video input id: got %q want %q", frame.Frame.InputID, scene.id)
	}
	if frame.Frame.PTS != 80*time.Millisecond {
		t.Fatalf("unexpected composed video pts: got %v want %v", frame.Frame.PTS, 80*time.Millisecond)
	}
	if frame.Frame.DTS != 80*time.Millisecond {
		t.Fatalf("unexpected composed video dts: got %v want %v", frame.Frame.DTS, 80*time.Millisecond)
	}
	if frame.Frame.Duration != 40*time.Millisecond {
		t.Fatalf("unexpected composed video duration: got %v want %v", frame.Frame.Duration, 40*time.Millisecond)
	}
	if !frame.Frame.Timestamp.Equal(timestamp) {
		t.Fatalf("unexpected composed video timestamp: got %v want %v", frame.Frame.Timestamp, timestamp)
	}
}

func TestNormalizeAudioMixRatios_DefaultsToPassthroughWhenUnspecified(t *testing.T) {
	got, err := normalizeAudioMixRatios(3, nil)
	if err != nil {
		t.Fatalf("normalizeAudioMixRatios() error = %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil ratios when unspecified, got %v", got)
	}
}

func TestNormalizeAudioMixRatios_RejectsInvalidSum(t *testing.T) {
	_, err := normalizeAudioMixRatios(3, []int{10, 20, 30})
	if err == nil {
		t.Fatal("expected invalid sum error, got nil")
	}
}

func TestMixPCM16AudioFrames(t *testing.T) {
	pts := 90 * time.Millisecond
	frames := []*raw.AudioFrame{
		mustPCM16AudioFrame(t, "input-1", pts, []int16{1000, -1000}, 44100, 2, 1),
		mustPCM16AudioFrame(t, "input-2", pts, []int16{2000, 2000}, 44100, 2, 1),
		mustPCM16AudioFrame(t, "input-3", pts, []int16{-1000, 1000}, 44100, 2, 1),
	}

	got, err := mixPCM16AudioFrames("scene-mix", []int{10, 30, 60}, frames)
	if err != nil {
		t.Fatalf("mixPCM16AudioFrames() error = %v", err)
	}

	samples := decodePCM16Samples(t, got.Frame.Payload[0])
	want := []int16{100, 1100}
	if len(samples) != len(want) {
		t.Fatalf("unexpected sample count: got %d want %d", len(samples), len(want))
	}
	for i := range want {
		if samples[i] != want[i] {
			t.Fatalf("sample[%d] = %d, want %d", i, samples[i], want[i])
		}
	}
	if got.Frame.InputID != "scene-mix" {
		t.Fatalf("unexpected mixed input ID: got %q want %q", got.Frame.InputID, "scene-mix")
	}
}

func TestBuildBufferedMixedPCM16AudioFrame_MixesInputsInLockstep(t *testing.T) {
	buffers := [][]int16{
		{1000, -1000, 1000, -1000},
		{500, 500, 500, 500},
	}

	got := buildBufferedMixedPCM16AudioFrame(
		"scene-mix",
		[]int{50, 50},
		buffers,
		1,
		40*time.Millisecond,
		time.Now(),
	)

	samples := decodePCM16Samples(t, got.Frame.Payload[0])
	want := []int16{750, -250}
	if len(samples) != len(want) {
		t.Fatalf("unexpected sample count: got %d want %d", len(samples), len(want))
	}
	for i := range want {
		if samples[i] != want[i] {
			t.Fatalf("sample[%d] = %d, want %d", i, samples[i], want[i])
		}
	}

	if len(buffers[0]) != 2 || len(buffers[1]) != 2 {
		t.Fatalf("expected one stereo frame consumed from each buffer, got len0=%d len1=%d", len(buffers[0]), len(buffers[1]))
	}
}

func TestBuildBufferedMixedPCM16AudioFrame_PadsMissingInputWithSilence(t *testing.T) {
	buffers := [][]int16{
		{1000, -1000},
		nil,
	}

	got := buildBufferedMixedPCM16AudioFrame(
		"scene-mix",
		[]int{70, 30},
		buffers,
		1,
		60*time.Millisecond,
		time.Now(),
	)

	samples := decodePCM16Samples(t, got.Frame.Payload[0])
	want := []int16{700, -700}
	for i := range want {
		if samples[i] != want[i] {
			t.Fatalf("sample[%d] = %d, want %d", i, samples[i], want[i])
		}
	}
}

func TestNewScene_AcceptsAudioMixRatios(t *testing.T) {
	sourceA := newMockSceneStream("audio-src-a")
	sourceB := newMockSceneStream("audio-src-b")

	scene, err := NewScene("scene-audio", raw.NewBlackCanvasSpec(2, 2), []Input{
		{Stream: sourceA, Layout: raw.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2}},
		{Stream: sourceB, Layout: raw.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2}},
	}, WithAudioMixRatios([]int{40, 60}))
	if err != nil {
		t.Fatalf("NewScene() error = %v", err)
	}
	defer scene.Close()

	if len(scene.cfg.audioMixRatios) != 2 || scene.cfg.audioMixRatios[0] != 40 || scene.cfg.audioMixRatios[1] != 60 {
		t.Fatalf("unexpected scene audio ratios: %v", scene.cfg.audioMixRatios)
	}
}

func TestNewScene_SingleHundredPercentAudioRatioDisablesMixing(t *testing.T) {
	sourceA := newMockSceneStream("audio-src-a")
	sourceB := newMockSceneStream("audio-src-b")

	scene, err := NewScene("scene-audio", raw.NewBlackCanvasSpec(2, 2), []Input{
		{Stream: sourceA, Layout: raw.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2}},
		{Stream: sourceB, Layout: raw.VideoLayout{X: 0, Y: 0, Width: 2, Height: 2}},
	}, WithAudioMixRatios([]int{0, 100}))
	if err != nil {
		t.Fatalf("NewScene() error = %v", err)
	}
	defer scene.Close()

	if scene.shouldMixAudio() {
		t.Fatal("expected single 100% ratio to disable audio mixing")
	}
	if got := scene.audioPassthroughIndex(); got != 1 {
		t.Fatalf("unexpected passthrough index: got %d want 1", got)
	}
}

type mockSceneStream struct {
	id      string
	videoCh chan *shared.Frame
	audioCh chan *shared.Frame
	started chan struct{}
	events  chan shared.Event
}

func newMockSceneStream(id string) *mockSceneStream {
	return &mockSceneStream{
		id:      id,
		videoCh: make(chan *shared.Frame, 1),
		audioCh: make(chan *shared.Frame, 16),
		started: make(chan struct{}),
		events:  make(chan shared.Event, 8),
	}
}

func (m *mockSceneStream) GetVideoChan() chan *shared.Frame { return m.videoCh }
func (m *mockSceneStream) GetAudioChan() chan *shared.Frame { return m.audioCh }
func (m *mockSceneStream) GetID() string                    { return m.id }
func (m *mockSceneStream) EventChan() chan shared.Event     { return m.events }
func (m *mockSceneStream) Start()                           {}
func (m *mockSceneStream) Stop()                            {}
func (m *mockSceneStream) Close()                           {}
func (m *mockSceneStream) Type() string                     { return "mock" }
func (m *mockSceneStream) IsRestartable() bool              { return false }
func (m *mockSceneStream) RestartInterval() time.Duration   { return 0 }

func (m *mockSceneStream) State() *shared.State {
	return &shared.State{
		StreamID: m.id,
		Type:     "mock",
	}
}

func (m *mockSceneStream) Clone() (shared.Stream, error) {
	return newMockSceneStream(m.id), nil
}

func (m *mockSceneStream) WaitForStart(ctx context.Context) error {
	select {
	case <-m.started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func mustPCM16AudioFrame(
	t *testing.T,
	inputID string,
	pts time.Duration,
	samples []int16,
	sampleRate int,
	channels int,
	samplesPerChannel int,
) *raw.AudioFrame {
	t.Helper()

	payload := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(sample))
	}

	frame := &raw.AudioFrame{
		Frame: &shared.Frame{
			PTS:        pts,
			DTS:        pts,
			Duration:   time.Duration(samplesPerChannel) * time.Second / time.Duration(sampleRate),
			Payload:    [][]byte{payload},
			Codec:      raw.AudioCodecPCMS16LE,
			PacketType: raw.AudioCodecPCMS16LE,
			Timestamp:  time.Now(),
			InputID:    inputID,
			IsKeyFrame: true,
		},
		SampleRate:        sampleRate,
		Channels:          channels,
		SampleFormat:      raw.AudioCodecPCMS16LE,
		SamplesPerChannel: samplesPerChannel,
	}
	if err := frame.Validate(); err != nil {
		t.Fatalf("audio frame validation failed: %v", err)
	}

	return frame
}

func decodePCM16Samples(t *testing.T, payload []byte) []int16 {
	t.Helper()

	if len(payload)%2 != 0 {
		t.Fatalf("payload length %d is not sample aligned", len(payload))
	}

	samples := make([]int16, len(payload)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(payload[i*2:]))
	}
	return samples
}
