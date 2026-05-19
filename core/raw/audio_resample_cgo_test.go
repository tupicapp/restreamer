//go:build cgo && media

package raw

import (
	"testing"

	shared "restreamer/core/shared"
)

func TestConvertPCM16AudioFrameResamplesWithoutCGOPointerPanic(t *testing.T) {
	frame := &AudioFrame{
		Frame: &shared.Frame{
			Payload: [][]byte{make([]byte, 1024*2*2)},
		},
		SampleRate:        44100,
		Channels:          2,
		SampleFormat:      AudioCodecPCMS16LE,
		SamplesPerChannel: 1024,
	}

	out, err := ConvertPCM16AudioFrame(frame, 48000, 2)
	if err != nil {
		t.Fatalf("ConvertPCM16AudioFrame() error = %v", err)
	}
	if out == nil {
		t.Fatal("ConvertPCM16AudioFrame() returned nil frame")
	}
	if out.SampleRate != 48000 {
		t.Fatalf("unexpected sample rate: got %d want 48000", out.SampleRate)
	}
	if out.Channels != 2 {
		t.Fatalf("unexpected channel count: got %d want 2", out.Channels)
	}
	if len(out.Frame.Payload) != 1 || len(out.Frame.Payload[0]) == 0 {
		t.Fatal("expected non-empty resampled payload")
	}
}
