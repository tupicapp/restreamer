//go:build cgo && media

package raw

import (
	"encoding/binary"
	"fmt"
	"math"
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

func TestDefaultPCM16ResampleQualityProfile(t *testing.T) {
	profile := defaultPCM16ResampleQualityProfile()

	if profile.filterSize < 32 {
		t.Fatalf("filter size too small: got %d", profile.filterSize)
	}
	if profile.phaseShift < 8 {
		t.Fatalf("phase shift too small: got %d", profile.phaseShift)
	}
	if profile.cutoff < 0.95 {
		t.Fatalf("cutoff too small: got %f", profile.cutoff)
	}
	if !profile.exactRational {
		t.Fatal("expected exact rational resampling to be enabled")
	}
	if profile.ditherMethod == "" {
		t.Fatal("expected dither method to be configured")
	}
}

func TestConvertPCM16AudioFrameRoundTripPreservesWaveform(t *testing.T) {
	const (
		sourceSampleRate = 44100
		targetSampleRate = 48000
		channels         = 2
		frequency        = 1000.0
		seconds          = 1
		trimSamples      = 512
	)

	source := buildStereoSinePCM16(sourceSampleRate, channels, frequency, seconds)
	frame := &AudioFrame{
		Frame: &shared.Frame{
			Payload: [][]byte{source},
		},
		SampleRate:        sourceSampleRate,
		Channels:          channels,
		SampleFormat:      AudioCodecPCMS16LE,
		SamplesPerChannel: sourceSampleRate * seconds,
	}

	up, err := ConvertPCM16AudioFrame(frame, targetSampleRate, channels)
	if err != nil {
		t.Fatalf("upsample error = %v", err)
	}
	down, err := ConvertPCM16AudioFrame(up, sourceSampleRate, channels)
	if err != nil {
		t.Fatalf("downsample error = %v", err)
	}

	original, err := decodePCM16TestSamples(source)
	if err != nil {
		t.Fatalf("decode original samples: %v", err)
	}
	roundTrip, err := decodePCM16TestSamples(down.Frame.Payload[0])
	if err != nil {
		t.Fatalf("decode round-trip samples: %v", err)
	}

	if len(original) <= trimSamples*channels*2 || len(roundTrip) <= trimSamples*channels*2 {
		t.Fatalf("not enough samples for trimmed comparison: original=%d roundTrip=%d", len(original), len(roundTrip))
	}

	original = original[trimSamples*channels : len(original)-trimSamples*channels]
	roundTrip = roundTrip[trimSamples*channels : len(roundTrip)-trimSamples*channels]

	if len(roundTrip) != len(original) {
		n := min(len(original), len(roundTrip))
		original = original[:n]
		roundTrip = roundTrip[:n]
	}

	rmsError := normalizedRMSError(original, roundTrip)
	if rmsError > 0.02 {
		t.Fatalf("round-trip RMS error too high: got %.5f want <= 0.02", rmsError)
	}
}

func TestPCM16ResamplerChunkedConversionMatchesContinuousConversion(t *testing.T) {
	const (
		sourceSampleRate = 44100
		targetSampleRate = 48000
		channels         = 2
		frequency        = 440.0
		seconds          = 1
		chunkSamples     = 1024
		trimSamples      = 512
	)

	source := buildStereoSinePCM16(sourceSampleRate, channels, frequency, seconds)
	fullFrame := &AudioFrame{
		Frame: &shared.Frame{
			Payload: [][]byte{source},
		},
		SampleRate:        sourceSampleRate,
		Channels:          channels,
		SampleFormat:      AudioCodecPCMS16LE,
		SamplesPerChannel: sourceSampleRate * seconds,
	}

	continuous, err := ConvertPCM16AudioFrame(fullFrame, targetSampleRate, channels)
	if err != nil {
		t.Fatalf("continuous conversion error = %v", err)
	}

	resampler, err := NewPCM16Resampler(targetSampleRate, channels)
	if err != nil {
		t.Fatalf("NewPCM16Resampler() error = %v", err)
	}
	defer func() { _ = resampler.Close() }()

	chunkBytes := chunkSamples * channels * 2
	var chunkedPayload []byte
	for offset := 0; offset < len(source); offset += chunkBytes {
		end := min(len(source), offset+chunkBytes)
		chunk := append([]byte(nil), source[offset:end]...)
		frame := &AudioFrame{
			Frame: &shared.Frame{
				Payload: [][]byte{chunk},
			},
			SampleRate:        sourceSampleRate,
			Channels:          channels,
			SampleFormat:      AudioCodecPCMS16LE,
			SamplesPerChannel: len(chunk) / (channels * 2),
		}

		out, err := resampler.Convert(frame)
		if err != nil {
			t.Fatalf("chunk conversion error at offset %d = %v", offset, err)
		}
		chunkedPayload = append(chunkedPayload, out.Frame.Payload[0]...)
	}

	want, err := decodePCM16TestSamples(continuous.Frame.Payload[0])
	if err != nil {
		t.Fatalf("decode continuous samples: %v", err)
	}
	got, err := decodePCM16TestSamples(chunkedPayload)
	if err != nil {
		t.Fatalf("decode chunked samples: %v", err)
	}

	if len(want) <= trimSamples*channels*2 || len(got) <= trimSamples*channels*2 {
		t.Fatalf("not enough samples for trimmed comparison: want=%d got=%d", len(want), len(got))
	}

	want = want[trimSamples*channels : len(want)-trimSamples*channels]
	got = got[trimSamples*channels : len(got)-trimSamples*channels]
	if len(want) != len(got) {
		n := min(len(want), len(got))
		want = want[:n]
		got = got[:n]
	}

	rmsError := normalizedRMSError(want, got)
	if rmsError > 0.01 {
		t.Fatalf("chunked conversion deviates too much from continuous conversion: got %.5f want <= 0.01", rmsError)
	}
}

func buildStereoSinePCM16(sampleRate int, channels int, frequency float64, seconds int) []byte {
	samplesPerChannel := sampleRate * seconds
	payload := make([]byte, samplesPerChannel*channels*2)
	amplitude := 0.6 * float64(math.MaxInt16)

	for i := 0; i < samplesPerChannel; i++ {
		value := int16(math.Round(amplitude * math.Sin(2*math.Pi*frequency*float64(i)/float64(sampleRate))))
		for ch := 0; ch < channels; ch++ {
			offset := (i*channels + ch) * 2
			binary.LittleEndian.PutUint16(payload[offset:], uint16(value))
		}
	}

	return payload
}

func decodePCM16TestSamples(payload []byte) ([]int16, error) {
	if len(payload)%2 != 0 {
		return nil, fmt.Errorf("payload length %d is not 16-bit aligned", len(payload))
	}

	samples := make([]int16, len(payload)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(payload[i*2:]))
	}
	return samples, nil
}

func normalizedRMSError(a []int16, b []int16) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 1
	}

	n := min(len(a), len(b))
	var sum float64
	for i := 0; i < n; i++ {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}

	return math.Sqrt(sum/float64(n)) / float64(math.MaxInt16)
}
