package encoder

import "fmt"

func validateAACEncoderConfig(cfg aacEncoderConfig) error {
	switch cfg.transport {
	case "", AACTransportRaw, AACTransportADTS:
	default:
		return fmt.Errorf("unsupported AAC transport %q", cfg.transport)
	}

	if cfg.sampleRate < 0 {
		return fmt.Errorf("invalid sample rate %d", cfg.sampleRate)
	}
	if cfg.channels < 0 {
		return fmt.Errorf("invalid channel count %d", cfg.channels)
	}
	if cfg.bitRate < 0 {
		return fmt.Errorf("invalid bitrate %d", cfg.bitRate)
	}
	if cfg.objectType < 0 {
		return fmt.Errorf("invalid AAC object type %d", cfg.objectType)
	}

	return nil
}

func normalizeAACEncoderConfig(cfg aacEncoderConfig) aacEncoderConfig {
	if cfg.outputBuffer <= 0 {
		cfg.outputBuffer = 100
	}
	if cfg.bitRate <= 0 {
		cfg.bitRate = 128_000
	}
	if cfg.transport == "" {
		cfg.transport = AACTransportRaw
	}
	if cfg.objectType == 0 {
		cfg.objectType = AACObjectTypeLC
	}
	return cfg
}
