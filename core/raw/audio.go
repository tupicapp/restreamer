package raw

import (
	"fmt"

	shared "github.com/tupicapp/restreamer/core/shared"
)

const (
	AudioCodecPCMS16LE = "pcm_s16le"
	AudioCodecPCMS32LE = "pcm_s32le"
)

type AudioFrame struct {
	Frame             *shared.Frame
	SampleRate        int
	Channels          int
	SampleFormat      string
	SamplesPerChannel int
}

func PCMBytesPerSample(sampleFormat string) (int, error) {
	switch sampleFormat {
	case AudioCodecPCMS16LE:
		return 2, nil
	case AudioCodecPCMS32LE:
		return 4, nil
	default:
		return 0, fmt.Errorf("unsupported audio sample format %q", sampleFormat)
	}
}

func (f AudioFrame) Validate() error {
	if f.Frame == nil {
		return fmt.Errorf("raw audio frame is nil")
	}
	if len(f.Frame.Payload) != 1 {
		return fmt.Errorf("raw audio frame must contain exactly one payload buffer, got %d", len(f.Frame.Payload))
	}
	if f.SampleRate <= 0 {
		return fmt.Errorf("invalid sample rate %d", f.SampleRate)
	}
	if f.Channels <= 0 {
		return fmt.Errorf("invalid channel count %d", f.Channels)
	}

	bytesPerSample, err := PCMBytesPerSample(f.SampleFormat)
	if err != nil {
		return err
	}

	payloadSize := len(f.Frame.Payload[0])
	if payloadSize == 0 {
		return fmt.Errorf("raw audio frame payload is empty")
	}

	frameSize := bytesPerSample * f.Channels
	if payloadSize%frameSize != 0 {
		return fmt.Errorf(
			"raw audio payload size %d is not aligned to %d-byte frames",
			payloadSize,
			frameSize,
		)
	}

	if f.SamplesPerChannel > 0 {
		want := f.SamplesPerChannel * frameSize
		if payloadSize != want {
			return fmt.Errorf("raw audio payload size mismatch: got %d want %d", payloadSize, want)
		}
	}

	return nil
}
