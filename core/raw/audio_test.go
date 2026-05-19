package raw

import (
	"testing"

	shared "github.com/tupicapp/restreamer/core/shared"
)

func TestAudioFrameValidate(t *testing.T) {
	frame := AudioFrame{
		Frame: &shared.Frame{
			Payload: [][]byte{make([]byte, 4096)},
		},
		SampleRate:        44100,
		Channels:          2,
		SampleFormat:      AudioCodecPCMS16LE,
		SamplesPerChannel: 1024,
	}

	if err := frame.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAudioFrameValidateRejectsMisalignedPayload(t *testing.T) {
	frame := AudioFrame{
		Frame: &shared.Frame{
			Payload: [][]byte{make([]byte, 3)},
		},
		SampleRate:   44100,
		Channels:     2,
		SampleFormat: AudioCodecPCMS16LE,
	}

	if err := frame.Validate(); err == nil {
		t.Fatal("Validate() expected error for misaligned payload")
	}
}
