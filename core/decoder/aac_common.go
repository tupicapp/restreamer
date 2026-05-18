package decoder

import "fmt"

func validateAACDecoderConfig(cfg aacDecoderConfig) error {
	if cfg.outputBuffer <= 0 {
		cfg.outputBuffer = 100
	}

	switch cfg.transport {
	case "", AACTransportRaw, AACTransportADTS:
	default:
		return fmt.Errorf("unsupported AAC transport %q", cfg.transport)
	}

	if cfg.transport == "" {
		cfg.transport = AACTransportRaw
	}

	if cfg.transport == AACTransportRaw && len(cfg.audioConfig) == 0 {
		return fmt.Errorf("aac decoder requires AudioSpecificConfig when using raw AAC transport")
	}

	return nil
}

func normalizeAACDecoderConfig(cfg aacDecoderConfig) aacDecoderConfig {
	if cfg.outputBuffer <= 0 {
		cfg.outputBuffer = 100
	}
	if cfg.transport == "" {
		cfg.transport = AACTransportRaw
	}
	if len(cfg.audioConfig) > 0 {
		cfg.audioConfig = append([]byte(nil), cfg.audioConfig...)
	}
	return cfg
}

func buildAACLCMPEG4AudioConfig(sampleRate int, channels int) ([]byte, error) {
	if sampleRate <= 0 {
		return nil, fmt.Errorf("invalid sample rate %d", sampleRate)
	}
	if channels <= 0 || channels > 7 {
		return nil, fmt.Errorf("invalid channel count %d", channels)
	}

	sampleRateIndex := aacSampleRateIndex(sampleRate)
	if sampleRateIndex < 0 {
		return nil, fmt.Errorf("unsupported AAC sample rate %d", sampleRate)
	}

	return []byte{
		byte((2 << 3) | (sampleRateIndex >> 1)),
		byte((sampleRateIndex&0x01)<<7 | (channels << 3)),
	}, nil
}

func aacSampleRateIndex(sampleRate int) int {
	switch sampleRate {
	case 96000:
		return 0
	case 88200:
		return 1
	case 64000:
		return 2
	case 48000:
		return 3
	case 44100:
		return 4
	case 32000:
		return 5
	case 24000:
		return 6
	case 22050:
		return 7
	case 16000:
		return 8
	case 12000:
		return 9
	case 11025:
		return 10
	case 8000:
		return 11
	case 7350:
		return 12
	default:
		return -1
	}
}
