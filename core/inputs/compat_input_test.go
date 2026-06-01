package inputs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
)

type compatMockStream struct {
	id          string
	videoCh     chan *Frame
	audioCh     chan *Frame
	trackInfoCh chan InputTrackInfo
	trackInfo   InputTrackInfo
	started     chan struct{}
	done        chan struct{}
}

func newCompatMockStream(id string, info InputTrackInfo) *compatMockStream {
	return &compatMockStream{
		id:          id,
		videoCh:     make(chan *Frame, 32),
		audioCh:     make(chan *Frame, 32),
		trackInfoCh: make(chan InputTrackInfo, 8),
		trackInfo:   info,
		started:     make(chan struct{}),
		done:        make(chan struct{}),
	}
}

func (m *compatMockStream) GetVideoChan() chan *Frame { return m.videoCh }

func (m *compatMockStream) GetAudioChan() chan *Frame { return m.audioCh }

func (m *compatMockStream) GetID() string { return m.id }

func (m *compatMockStream) Start() {
	select {
	case <-m.started:
	default:
		close(m.started)
	}
}

func (m *compatMockStream) Stop() {}

func (m *compatMockStream) Close() {
	select {
	case <-m.done:
	default:
		close(m.done)
		close(m.videoCh)
		close(m.audioCh)
		close(m.trackInfoCh)
	}
}

func (m *compatMockStream) State() *State {
	return &State{StreamID: m.id, Type: "mock", Url: m.id}
}

func (m *compatMockStream) Clone() (Stream, error) {
	return newCompatMockStream(m.id, m.trackInfo), nil
}

func (m *compatMockStream) WaitForStart(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.started:
		return nil
	}
}

func (m *compatMockStream) Type() string                   { return "mock" }
func (m *compatMockStream) IsRestartable() bool            { return false }
func (m *compatMockStream) RestartInterval() time.Duration { return 0 }
func (m *compatMockStream) EventChan() chan Event          { return nil }
func (m *compatMockStream) TrackInfoSnapshot() InputTrackInfo {
	return m.trackInfo
}
func (m *compatMockStream) TrackInfoChan() <-chan InputTrackInfo { return m.trackInfoCh }

func TestCompatibleInput_FillsMissingVideoTrack(t *testing.T) {
	source := newCompatMockStream("audio-only", InputTrackInfo{
		Initialized:     true,
		HasAudio:        true,
		AudioSampleRate: DefaultAudioRate,
	})
	stream := NewCompatibleInput(
		source,
		WithCompatVideoInterval(25*time.Millisecond),
		WithCompatVideoTimeout(40*time.Millisecond),
	)
	source.Start()
	stream.Start()
	defer stream.Close()

	select {
	case frame := <-stream.GetVideoChan():
		if frame == nil {
			t.Fatal("expected synthetic video frame")
		}
		if frame.Codec != "h264" || !frame.IsKeyFrame {
			t.Fatalf("unexpected synthetic video frame: %+v", frame)
		}
		if frame.PacketType != "I" {
			t.Fatalf("expected synthetic video PacketType I, got %q", frame.PacketType)
		}
		if frame.InputID != source.id || frame.SequenceID == 0 || frame.GOPID == 0 {
			t.Fatalf("synthetic video frame missing RTMP-like identity fields: %+v", frame)
		}
		if frame.Timestamp.IsZero() || frame.Duration <= 0 {
			t.Fatalf("synthetic video frame missing timing fields: %+v", frame)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for synthetic video frame")
	}
}

func TestCompatibleInput_InactiveInputStillFeedsSidecars(t *testing.T) {
	source := newCompatMockStream("inactive-source", InputTrackInfo{
		Initialized:     true,
		HasVideo:        true,
		HasAudio:        true,
		AudioSampleRate: DefaultAudioRate,
	})
	sidecar := newCompatMockStream("inactive-sidecar", InputTrackInfo{})

	stream := NewCompatibleInput(
		source,
		WithCompatSidecars(sidecar),
	)
	compat, ok := stream.(*compatInputStream)
	if !ok {
		t.Fatalf("unexpected stream type %T", stream)
	}

	stream.Start()
	defer stream.Close()

	compat.setActive(false)

	frame := &Frame{
		PTS:        time.Second,
		DTS:        time.Second,
		Timestamp:  time.Now(),
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		Codec:      "h264",
		IsKeyFrame: true,
		InputID:    source.id,
	}

	source.videoCh <- frame

	select {
	case got := <-sidecar.GetVideoChan():
		if got == nil {
			t.Fatal("expected sidecar video frame")
		}
		if got.InputID != source.id {
			t.Fatalf("unexpected sidecar input id: got %q want %q", got.InputID, source.id)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for sidecar video frame")
	}

	select {
	case got := <-stream.GetVideoChan():
		t.Fatalf("inactive input should not forward to primary stream channel, got %+v", got)
	case <-time.After(75 * time.Millisecond):
	}
}

func TestCompatibleInput_ActiveMainPathWriteTimesOutQuickly(t *testing.T) {
	source := newCompatMockStream("timeout-source", InputTrackInfo{
		Initialized: true,
		HasVideo:    true,
	})
	stream := NewCompatibleInput(source)
	compat, ok := stream.(*compatInputStream)
	if !ok {
		t.Fatalf("unexpected stream type %T", stream)
	}

	for i := 0; i < cap(compat.videoChan); i++ {
		compat.videoChan <- &Frame{SequenceID: int64(i + 1)}
	}

	start := time.Now()
	ok = compat.emitVideo(&Frame{
		PTS:        time.Second,
		DTS:        time.Second,
		Timestamp:  time.Now(),
		Payload:    [][]byte{{0x65}},
		Codec:      "h264",
		IsKeyFrame: true,
		InputID:    source.id,
	})
	elapsed := time.Since(start)

	if !ok {
		t.Fatal("emitVideo should drop on timeout, not stop the stream")
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("emitVideo took too long on blocked main path: %v", elapsed)
	}
}

func TestCompatibleInput_FillsMissingVideoTrackWithoutSharedTemplate(t *testing.T) {
	source := newCompatMockStream("audio-only-no-template", InputTrackInfo{
		Initialized:     true,
		HasAudio:        true,
		AudioSampleRate: DefaultAudioRate,
	})
	stream := NewCompatibleInput(
		source,
		WithCompatVideoInterval(25*time.Millisecond),
		WithCompatVideoTimeout(40*time.Millisecond),
	)
	source.Start()
	stream.Start()
	defer stream.Close()

	select {
	case frame := <-stream.GetVideoChan():
		if frame == nil {
			t.Fatal("expected fallback synthetic video frame")
		}
		if frame.Codec != "h264" || !frame.IsKeyFrame || frame.PacketType != "I" {
			t.Fatalf("unexpected fallback synthetic video frame: %+v", frame)
		}
		if len(frame.Payload) != len(defaultCompatVideoTemplate.Payload) {
			t.Fatalf("expected fallback synthetic video payload, got %+v", frame.Payload)
		}
		if frame.InputID != source.id || frame.SequenceID == 0 || frame.GOPID == 0 {
			t.Fatalf("fallback synthetic video frame missing RTMP-like identity fields: %+v", frame)
		}
		if frame.Timestamp.IsZero() || frame.Duration <= 0 {
			t.Fatalf("fallback synthetic video frame missing timing fields: %+v", frame)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for fallback synthetic video frame")
	}
}

func TestCompatibleInput_FillsMissingAudioTrack(t *testing.T) {
	source := newCompatMockStream("video-only", InputTrackInfo{
		Initialized: true,
		HasVideo:    true,
	})
	stream := NewCompatibleInput(
		source,
		WithCompatAudioInterval(20*time.Millisecond),
		WithCompatAudioTimeout(30*time.Millisecond),
	)
	source.Start()
	stream.Start()
	defer stream.Close()

	select {
	case frame := <-stream.GetAudioChan():
		if frame == nil {
			t.Fatal("expected synthetic audio frame")
		}
		if frame.Codec != "aac" || frame.SampleRate != DefaultAudioRate {
			t.Fatalf("unexpected synthetic audio frame: %+v", frame)
		}
		if frame.InputID != source.id || frame.SequenceID == 0 || frame.GOPID == 0 {
			t.Fatalf("synthetic audio frame missing RTMP-like identity fields: %+v", frame)
		}
		if frame.Timestamp.IsZero() || frame.Duration <= 0 {
			t.Fatalf("synthetic audio frame missing timing fields: %+v", frame)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for synthetic audio frame")
	}
}

func TestCompatibleInput_FillsStalledAudioTrack(t *testing.T) {
	source := newCompatMockStream("av", InputTrackInfo{
		Initialized:     true,
		HasAudio:        true,
		HasVideo:        true,
		AudioSampleRate: DefaultAudioRate,
	})
	stream := NewCompatibleInput(
		source,
		WithCompatAudioInterval(20*time.Millisecond),
		WithCompatAudioTimeout(40*time.Millisecond),
	)
	source.Start()
	stream.Start()
	defer stream.Close()

	source.audioCh <- &Frame{
		PTS:        10 * time.Millisecond,
		DTS:        10 * time.Millisecond,
		Payload:    [][]byte{{0x01, 0x02, 0x03}},
		Codec:      "aac",
		SampleRate: DefaultAudioRate,
		InputID:    source.id,
		Timestamp:  time.Now(),
	}

	real := mustReadFrame(t, stream.GetAudioChan(), 200*time.Millisecond)
	if real == nil || len(real.Payload) == 0 || len(real.Payload[0]) != 3 {
		t.Fatalf("expected forwarded real audio frame, got %+v", real)
	}

	synthetic := mustReadFrame(t, stream.GetAudioChan(), 250*time.Millisecond)
	if synthetic == nil || synthetic.SequenceID <= real.SequenceID {
		t.Fatalf("expected later synthetic audio frame, got real=%+v synthetic=%+v", real, synthetic)
	}
	if synthetic.Codec != "aac" || synthetic.SampleRate != DefaultAudioRate {
		t.Fatalf("unexpected synthetic audio frame: %+v", synthetic)
	}
	if synthetic.InputID != source.id || synthetic.GOPID != synthetic.SequenceID || synthetic.Timestamp.IsZero() || synthetic.Duration <= 0 {
		t.Fatalf("synthetic audio frame missing RTMP-like fields: %+v", synthetic)
	}
}

func TestCompatibleInput_CloseDoesNotBlockWhenOutputChannelsAreFull(t *testing.T) {
	source := newCompatMockStream("audio-only-close", InputTrackInfo{
		Initialized:     true,
		HasAudio:        true,
		AudioSampleRate: DefaultAudioRate,
	})
	stream := NewCompatibleInput(source)
	compat, ok := stream.(*compatInputStream)
	if !ok {
		t.Fatalf("expected *compatInputStream, got %T", stream)
	}

	fillVideo := true
	for fillVideo {
		select {
		case compat.videoChan <- &Frame{Codec: "h264", IsKeyFrame: true}:
		default:
			fillVideo = false
		}
	}
	fillAudio := true
	for fillAudio {
		select {
		case compat.audioChan <- &Frame{Codec: "aac", IsKeyFrame: true}:
		default:
			fillAudio = false
		}
	}

	done := make(chan struct{})
	go func() {
		stream.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("compat input Close() blocked when output channels were full")
	}
}

func TestCompatibleInput_RuntimeDetectionDisabledStillFillsMissingTrack(t *testing.T) {
	source := newCompatMockStream("video-only-no-runtime", InputTrackInfo{
		Initialized: true,
		HasVideo:    true,
	})
	stream := NewCompatibleInput(
		source,
		WithCompatRuntimeDetection(false),
		WithCompatAudioInterval(20*time.Millisecond),
		WithCompatAudioTimeout(30*time.Millisecond),
	)
	source.Start()
	stream.Start()
	defer stream.Close()

	select {
	case frame := <-stream.GetAudioChan():
		if frame == nil {
			t.Fatal("expected synthetic audio frame")
		}
		if frame.Codec != "aac" || frame.SampleRate != DefaultAudioRate {
			t.Fatalf("unexpected synthetic audio frame: %+v", frame)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for synthetic audio frame")
	}
}

func TestCompatibleInput_RuntimeDetectionDisabledDoesNotFillStalledTrack(t *testing.T) {
	source := newCompatMockStream("av-no-runtime", InputTrackInfo{
		Initialized:     true,
		HasAudio:        true,
		HasVideo:        true,
		AudioSampleRate: DefaultAudioRate,
	})
	stream := NewCompatibleInput(
		source,
		WithCompatRuntimeDetection(false),
		WithCompatAudioInterval(20*time.Millisecond),
		WithCompatAudioTimeout(40*time.Millisecond),
	)
	source.Start()
	stream.Start()
	defer stream.Close()

	source.audioCh <- &Frame{
		PTS:        10 * time.Millisecond,
		DTS:        10 * time.Millisecond,
		Payload:    [][]byte{{0x01, 0x02, 0x03}},
		Codec:      "aac",
		SampleRate: DefaultAudioRate,
		InputID:    source.id,
		Timestamp:  time.Now(),
	}

	real := mustReadFrame(t, stream.GetAudioChan(), 200*time.Millisecond)
	if real == nil || len(real.Payload) == 0 {
		t.Fatalf("expected forwarded real audio frame, got %+v", real)
	}

	select {
	case synthetic := <-stream.GetAudioChan():
		t.Fatalf("did not expect synthetic audio frame when runtime detection is disabled, got %+v", synthetic)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestCompatibleInput_FillsStalledVideoTrack(t *testing.T) {
	source := newCompatMockStream("av", InputTrackInfo{
		Initialized: true,
		HasAudio:    true,
		HasVideo:    true,
	})
	stream := NewCompatibleInput(
		source,
		WithCompatVideoInterval(25*time.Millisecond),
		WithCompatVideoTimeout(40*time.Millisecond),
	)
	source.Start()
	stream.Start()
	defer stream.Close()

	source.videoCh <- &Frame{
		PTS:        10 * time.Millisecond,
		DTS:        10 * time.Millisecond,
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		Codec:      "h264",
		InputID:    source.id,
		Timestamp:  time.Now(),
		IsKeyFrame: true,
	}

	real := mustReadFrame(t, stream.GetVideoChan(), 200*time.Millisecond)
	if real == nil || !real.IsKeyFrame {
		t.Fatalf("expected forwarded real video frame, got %+v", real)
	}

	synthetic := mustReadFrame(t, stream.GetVideoChan(), 250*time.Millisecond)
	if synthetic == nil || synthetic.SequenceID <= real.SequenceID {
		t.Fatalf("expected later synthetic video frame, got real=%+v synthetic=%+v", real, synthetic)
	}
	if synthetic.Codec != "h264" || !synthetic.IsKeyFrame {
		t.Fatalf("unexpected synthetic video frame: %+v", synthetic)
	}
	if synthetic.PacketType != "I" || synthetic.InputID != source.id || synthetic.GOPID == 0 || synthetic.Timestamp.IsZero() || synthetic.Duration <= 0 {
		t.Fatalf("synthetic video frame missing RTMP-like fields: %+v", synthetic)
	}
}

func TestCompatibleInput_DoesNotBorrowTemplatesAcrossReaders(t *testing.T) {
	donorSource := newCompatMockStream("donor", InputTrackInfo{
		Initialized: true,
		HasVideo:    true,
	})
	donorStream := NewCompatibleInput(
		donorSource,
		WithCompatVideoInterval(25*time.Millisecond),
		WithCompatVideoTimeout(40*time.Millisecond),
	)
	donorSource.Start()
	donorStream.Start()
	defer donorStream.Close()

	donorVideo := &Frame{
		PTS:        10 * time.Millisecond,
		DTS:        10 * time.Millisecond,
		Payload:    [][]byte{{0x67, 0x11}, {0x68, 0x22}, {0x65, 0x33}},
		Codec:      "h264",
		InputID:    donorSource.id,
		Timestamp:  time.Now(),
		IsKeyFrame: true,
	}
	donorSource.videoCh <- donorVideo
	_ = mustReadFrame(t, donorStream.GetVideoChan(), 200*time.Millisecond)

	audioOnlySource := newCompatMockStream("audio-only-isolated", InputTrackInfo{
		Initialized:     true,
		HasAudio:        true,
		AudioSampleRate: DefaultAudioRate,
	})
	audioOnlyStream := NewCompatibleInput(
		audioOnlySource,
		WithCompatVideoInterval(25*time.Millisecond),
		WithCompatVideoTimeout(40*time.Millisecond),
	)
	audioOnlySource.Start()
	audioOnlyStream.Start()
	defer audioOnlyStream.Close()

	frame := mustReadFrame(t, audioOnlyStream.GetVideoChan(), 250*time.Millisecond)
	if frame == nil {
		t.Fatal("expected synthetic video frame")
	}
	if compatFramePayloadHash(frame) == compatFramePayloadHash(donorVideo) {
		t.Fatalf("expected reader-local fallback video, got donor template payload %+v", frame.Payload)
	}
	if len(frame.Payload) != len(defaultCompatVideoTemplate.Payload) {
		t.Fatalf("expected default fallback payload, got %+v", frame.Payload)
	}
}

func TestCompatibleInput_LiveRTMPModes(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		wantVideo bool
		wantAudio bool
	}{
		{name: "audio_video", url: "rtmp://localhost:1938/live/1", wantVideo: true, wantAudio: true},
		{name: "audio_only", url: "rtmp://localhost:1938/live/2", wantVideo: false, wantAudio: true},
		{name: "video_only", url: "rtmp://localhost:1938/live/3", wantVideo: true, wantAudio: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireRTMPPublishing(t, tt.url, 10*time.Second)

			stream := NewCompatibleInput(NewRTMP("compat-"+tt.name, tt.url))
			stream.Start()
			defer stream.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := stream.WaitForStart(ctx); err != nil {
				t.Fatalf("WaitForStart() error = %v", err)
			}

			var gotVideo *Frame
			var gotAudio *Frame
			deadline := time.After(3 * time.Second)
			for (tt.wantVideo && gotVideo == nil) || (tt.wantAudio && gotAudio == nil) {
				select {
				case frame := <-stream.GetVideoChan():
					if frame != nil {
						gotVideo = frame
					}
				case frame := <-stream.GetAudioChan():
					if frame != nil {
						gotAudio = frame
					}
				case <-deadline:
					t.Fatalf("timed out waiting for expected frames from %s; video=%v audio=%v", tt.url, gotVideo != nil, gotAudio != nil)
				}
			}

			if tt.wantVideo && gotVideo == nil {
				t.Fatalf("expected video frame from %s", tt.url)
			}
			if tt.wantAudio && gotAudio == nil {
				t.Fatalf("expected audio frame from %s", tt.url)
			}
		})
	}
}

func mustReadFrame(t *testing.T, ch <-chan *Frame, timeout time.Duration) *Frame {
	t.Helper()

	select {
	case frame := <-ch:
		return frame
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for frame after %v", timeout)
		return nil
	}
}

func compatFramePayloadHash(frame *Frame) string {
	if frame == nil {
		return ""
	}

	sum := sha256.New()
	for _, payload := range frame.Payload {
		sum.Write(payload)
	}
	return hex.EncodeToString(sum.Sum(nil))
}

var _ shared.Stream = (*compatMockStream)(nil)
var _ TrackInfoProvider = (*compatMockStream)(nil)
