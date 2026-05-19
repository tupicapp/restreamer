package outputs

import (
	"bytes"
	"restreamer/core/shared"
	"testing"
	"time"
)

type fakeRTMPWriter struct {
	frames []*shared.Frame
}

func (f *fakeRTMPWriter) Write(frame *shared.Frame) error {
	f.frames = append(f.frames, cloneFrame(frame))
	return nil
}

type fakeAudioConfigProvider struct {
	config []byte
}

func (f fakeAudioConfigProvider) AudioSpecificConfig() []byte {
	return append([]byte(nil), f.config...)
}

func TestWriteH264WaitsForCodecParams(t *testing.T) {
	t.Parallel()

	writer := &rtmpWriter{
		id:      "out",
		url:     "rtmp://localhost/live/test",
		done:    make(chan struct{}),
		Started: make(chan struct{}),
	}

	initCalls := 0
	writer.initFn = func(sps, pps []byte) (RtmpWriter, error) {
		initCalls++
		return &fakeRTMPWriter{}, nil
	}

	err := writer.WriteH264(&shared.Frame{
		Codec:      "h264",
		IsKeyFrame: false,
		Payload:    [][]byte{{0x41, 0x9a, 0x22}},
	})
	if err != nil {
		t.Fatalf("WriteH264 returned error: %v", err)
	}

	if initCalls != 0 {
		t.Fatalf("expected no writer initialization, got %d", initCalls)
	}
	if writer.writer != nil {
		t.Fatalf("expected writer to remain nil")
	}
}

func TestWriteH264InitializesWriterFromKeyframeAndFlushesPendingAudio(t *testing.T) {
	t.Parallel()

	rtmpOut := &rtmpWriter{
		id:      "out",
		url:     "rtmp://localhost/live/test",
		done:    make(chan struct{}),
		Started: make(chan struct{}),
	}

	fakeWriter := &fakeRTMPWriter{}
	var gotSPS []byte
	var gotPPS []byte
	rtmpOut.initFn = func(sps, pps []byte) (RtmpWriter, error) {
		gotSPS = append([]byte(nil), sps...)
		gotPPS = append([]byte(nil), pps...)
		return fakeWriter, nil
	}

	audio := &shared.Frame{
		Codec:      "aac",
		PTS:        20 * time.Millisecond,
		SequenceID: 10,
		InputID:    "audio",
		Payload:    [][]byte{{0x11, 0x22}},
	}
	if err := rtmpOut.WriteMpeg4Audio(audio); err != nil {
		t.Fatalf("WriteMpeg4Audio returned error: %v", err)
	}

	sps := []byte{0x67, 0x42, 0x00, 0x1f}
	pps := []byte{0x68, 0xce, 0x06, 0xe2}
	idr := []byte{0x65, 0x88, 0x84}
	video := &shared.Frame{
		Codec:      "h264",
		IsKeyFrame: true,
		PTS:        40 * time.Millisecond,
		DTS:        40 * time.Millisecond,
		SequenceID: 11,
		InputID:    "video",
		Payload: [][]byte{
			append([]byte{0x00, 0x00, 0x00, 0x01}, sps...),
			append([]byte{0x00, 0x00, 0x01}, pps...),
			idr,
		},
	}

	if err := rtmpOut.WriteH264(video); err != nil {
		t.Fatalf("WriteH264 returned error: %v", err)
	}

	if !bytes.Equal(gotSPS, sps) {
		t.Fatalf("unexpected SPS: got %v want %v", gotSPS, sps)
	}
	if !bytes.Equal(gotPPS, pps) {
		t.Fatalf("unexpected PPS: got %v want %v", gotPPS, pps)
	}
	if len(fakeWriter.frames) != 2 {
		t.Fatalf("expected 2 written frames, got %d", len(fakeWriter.frames))
	}
	if fakeWriter.frames[0].Codec != "h264" {
		t.Fatalf("expected first written frame to be video, got %q", fakeWriter.frames[0].Codec)
	}
	if fakeWriter.frames[1].Codec != "aac" {
		t.Fatalf("expected second written frame to be audio, got %q", fakeWriter.frames[1].Codec)
	}
	if len(rtmpOut.pendingAudio) != 0 {
		t.Fatalf("expected pending audio queue to be empty after flush")
	}
}

func TestQueuePendingAudioTracksSampleRate(t *testing.T) {
	t.Parallel()

	rtmpOut := &rtmpWriter{}
	rtmpOut.queuePendingAudioLocked(&shared.Frame{
		Codec:      "aac",
		Payload:    [][]byte{{0x11, 0x22}},
		SampleRate: 48000,
	})

	if rtmpOut.audioSampleRate != 48000 {
		t.Fatalf("unexpected audio sample rate: got %d want 48000", rtmpOut.audioSampleRate)
	}
}

func TestResolveAudioConfigLockedPrefersProviderConfig(t *testing.T) {
	t.Parallel()

	rtmpOut := &rtmpWriter{
		audioSampleRate: 44100,
		audioConfigProvider: fakeAudioConfigProvider{
			config: []byte{0x11, 0x90}, // AAC-LC, 48kHz, stereo
		},
	}

	conf := rtmpOut.resolveAudioConfigLocked()
	if conf == nil {
		t.Fatal("expected audio config, got nil")
	}
	if conf.SampleRate != 48000 {
		t.Fatalf("unexpected sample rate: got %d want 48000", conf.SampleRate)
	}
	if conf.ChannelCount != 2 {
		t.Fatalf("unexpected channel count: got %d want 2", conf.ChannelCount)
	}
}
