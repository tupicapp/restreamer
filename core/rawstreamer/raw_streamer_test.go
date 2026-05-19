package rawstreamer

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"restreamer/core/avsync"
	"restreamer/core/raw"
	shared "restreamer/core/shared"
)

func TestNormalizeAudioMixRatios(t *testing.T) {
	got, err := NormalizeAudioMixRatios(4, []int{10, 20, 30, 40})
	if err != nil {
		t.Fatalf("NormalizeAudioMixRatios() error = %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("unexpected ratio count: got %d want 4", len(got))
	}
}

func TestNormalizeAudioMixRatiosRejectsInvalidSum(t *testing.T) {
	_, err := NormalizeAudioMixRatios(2, []int{50, 40})
	if err == nil {
		t.Fatal("expected invalid sum error, got nil")
	}
}

func TestSnapshotPlacementsAllowsPartialReadiness(t *testing.T) {
	streamer := &RawStreamer{
		runtimes: []*inputRuntime{
			{
				spec: Input{
					Layout: raw.VideoLayout{X: 0, Y: 0, Width: 640, Height: 360},
				},
				latestFrame: &raw.VideoFrame{
					Frame: &shared.Frame{
						Payload: [][]byte{{0x00}},
					},
					Width:  1,
					Height: 1,
					PixFmt: raw.YUV420PPixFmt,
				},
				ready: true,
			},
			{
				spec: Input{
					Layout: raw.VideoLayout{X: 640, Y: 0, Width: 640, Height: 360},
				},
			},
		},
	}

	placements, ok := streamer.snapshotPlacements()
	if !ok {
		t.Fatal("expected placements to be available with one ready input")
	}
	if len(placements) != 1 {
		t.Fatalf("unexpected placement count: got %d want 1", len(placements))
	}
	if placements[0].Layout.Width != 640 || placements[0].Layout.Height != 360 {
		t.Fatalf("unexpected layout: %+v", placements[0].Layout)
	}
}

func TestSnapshotPlacementsRejectsAllMissingInputs(t *testing.T) {
	streamer := &RawStreamer{
		runtimes: []*inputRuntime{
			{spec: Input{Layout: raw.VideoLayout{X: 0, Y: 0, Width: 640, Height: 360}}},
		},
	}

	placements, ok := streamer.snapshotPlacements()
	if ok {
		t.Fatal("expected no placements when every input is missing")
	}
	if len(placements) != 0 {
		t.Fatalf("expected zero placements, got %v", placements)
	}
}

func TestUpdateH264HeadersCachesLatestSPSAndPPS(t *testing.T) {
	headers := updateH264Headers(nil, [][]byte{
		{0x67, 0x42, 0x00, 0x1f},
		{0x68, 0xce, 0x06, 0xe2},
	})

	if len(headers) != 2 {
		t.Fatalf("unexpected header count: got %d want 2", len(headers))
	}
	if !bytes.Equal(headers[0], []byte{0x67, 0x42, 0x00, 0x1f}) {
		t.Fatalf("unexpected sps: %v", headers[0])
	}
	if !bytes.Equal(headers[1], []byte{0x68, 0xce, 0x06, 0xe2}) {
		t.Fatalf("unexpected pps: %v", headers[1])
	}
}

func TestCloneFrameWithH264HeadersPrependsMissingHeadersToKeyframe(t *testing.T) {
	frame := &shared.Frame{
		Codec:      "h264",
		IsKeyFrame: true,
		Payload: [][]byte{
			{0x65, 0x88, 0x84},
		},
	}

	out := cloneFrameWithH264Headers(frame, [][]byte{
		{0x67, 0x42, 0x00, 0x1f},
		{0x68, 0xce, 0x06, 0xe2},
	})

	if len(out.Payload) != 3 {
		t.Fatalf("unexpected payload count: got %d want 3", len(out.Payload))
	}
	if h264NALType(out.Payload[0]) != 7 {
		t.Fatalf("expected first nalu to be sps, got type %d", h264NALType(out.Payload[0]))
	}
	if h264NALType(out.Payload[1]) != 8 {
		t.Fatalf("expected second nalu to be pps, got type %d", h264NALType(out.Payload[1]))
	}
	if h264NALType(out.Payload[2]) != 5 {
		t.Fatalf("expected third nalu to be idr, got type %d", h264NALType(out.Payload[2]))
	}
}

type audioConfigOnlyStream struct {
	audioConfig []byte
	audioCh     chan *shared.Frame
	videoCh     chan *shared.Frame
}

func (s *audioConfigOnlyStream) GetVideoChan() chan *shared.Frame { return s.videoCh }
func (s *audioConfigOnlyStream) GetAudioChan() chan *shared.Frame { return s.audioCh }
func (s *audioConfigOnlyStream) GetID() string                    { return "audio-config-only" }
func (s *audioConfigOnlyStream) Type() string                     { return "reader" }
func (s *audioConfigOnlyStream) AudioLock() *sync.RWMutex         { return &sync.RWMutex{} }
func (s *audioConfigOnlyStream) VideoLock() *sync.RWMutex         { return &sync.RWMutex{} }
func (s *audioConfigOnlyStream) Start()                           {}
func (s *audioConfigOnlyStream) Stop()                            {}
func (s *audioConfigOnlyStream) Close()                           {}
func (s *audioConfigOnlyStream) Clone() (shared.Stream, error)    { return s, nil }
func (s *audioConfigOnlyStream) IsRestartable() bool              { return false }
func (s *audioConfigOnlyStream) RestartInterval() time.Duration   { return 0 }
func (s *audioConfigOnlyStream) WaitForStart(ctx context.Context) error {
	return nil
}
func (s *audioConfigOnlyStream) State() *shared.State         { return &shared.State{} }
func (s *audioConfigOnlyStream) EventChan() chan shared.Event { return nil }
func (s *audioConfigOnlyStream) AudioSpecificConfig() []byte {
	return append([]byte(nil), s.audioConfig...)
}

func TestAudioSpecificConfigFallsBackToPassthroughInputConfig(t *testing.T) {
	streamer := &RawStreamer{
		runtimes: []*inputRuntime{
			{
				spec: Input{
					Stream: &audioConfigOnlyStream{
						audioConfig: []byte{0x12, 0x10},
						audioCh:     make(chan *shared.Frame),
						videoCh:     make(chan *shared.Frame),
					},
				},
			},
		},
	}

	got := streamer.AudioSpecificConfig()
	if !bytes.Equal(got, []byte{0x12, 0x10}) {
		t.Fatalf("unexpected audio config: got %v want %v", got, []byte{0x12, 0x10})
	}
}

func TestBuildBufferedPCM16AudioFramePadsSilenceWhenBufferRunsShort(t *testing.T) {
	buffer := []int16{100, -100, 200, -200}
	timing := avsync.FrameTiming{
		PTS:       80 * time.Millisecond,
		DTS:       80 * time.Millisecond,
		Duration:  time.Duration(2) * time.Second / mixedAudioSampleRate,
		Timestamp: time.Unix(1_700_000_000, 0).Add(80 * time.Millisecond),
	}

	frame := buildBufferedPCM16AudioFrame("scene", &buffer, 2, timing)
	if frame == nil || frame.Frame == nil {
		t.Fatal("expected audio frame")
	}
	if frame.Frame.PTS != timing.PTS {
		t.Fatalf("unexpected pts: got %v want %v", frame.Frame.PTS, timing.PTS)
	}
	if frame.SamplesPerChannel != 2 {
		t.Fatalf("unexpected samples per channel: got %d want 2", frame.SamplesPerChannel)
	}
	if got := decodeInt16At(frame.Frame.Payload[0], 0); got != 100 {
		t.Fatalf("unexpected sample[0]: got %d want 100", got)
	}
	if got := decodeInt16At(frame.Frame.Payload[0], 1); got != -100 {
		t.Fatalf("unexpected sample[1]: got %d want -100", got)
	}
	if got := decodeInt16At(frame.Frame.Payload[0], 2); got != 200 {
		t.Fatalf("unexpected sample[2]: got %d want 200", got)
	}
	if got := decodeInt16At(frame.Frame.Payload[0], 3); got != -200 {
		t.Fatalf("unexpected sample[3]: got %d want -200", got)
	}
	if len(buffer) != 0 {
		t.Fatalf("expected source buffer to be consumed, got %d samples", len(buffer))
	}
}

func TestBuildBufferedPCM16AudioFrameProducesSilenceForEmptyBuffer(t *testing.T) {
	var buffer []int16
	timing := avsync.FrameTiming{
		Duration:  time.Duration(2) * time.Second / mixedAudioSampleRate,
		Timestamp: time.Unix(1_700_000_000, 0),
	}

	frame := buildBufferedPCM16AudioFrame("scene", &buffer, 2, timing)
	if frame == nil || frame.Frame == nil {
		t.Fatal("expected audio frame")
	}
	for i := 0; i < 4; i++ {
		if got := decodeInt16At(frame.Frame.Payload[0], i); got != 0 {
			t.Fatalf("unexpected sample[%d]: got %d want 0", i, got)
		}
	}
}

func decodeInt16At(payload []byte, index int) int16 {
	offset := index * 2
	return int16(binary.LittleEndian.Uint16(payload[offset:]))
}
