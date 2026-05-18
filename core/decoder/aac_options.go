package decoder

type AACTransport string

const (
	AACTransportRaw  AACTransport = "raw"
	AACTransportADTS AACTransport = "adts"
)

type aacDecoderConfig struct {
	outputBuffer int
	transport    AACTransport
	audioConfig  []byte
}

type AACDecoderOption func(*aacDecoderConfig)

func WithAACDecoderOutputBuffer(size int) AACDecoderOption {
	return func(cfg *aacDecoderConfig) {
		cfg.outputBuffer = size
	}
}

func WithAACDecoderTransport(transport AACTransport) AACDecoderOption {
	return func(cfg *aacDecoderConfig) {
		cfg.transport = transport
	}
}

func WithAACDecoderAudioSpecificConfig(config []byte) AACDecoderOption {
	return func(cfg *aacDecoderConfig) {
		if len(config) == 0 {
			cfg.audioConfig = nil
			return
		}
		cfg.audioConfig = append([]byte(nil), config...)
	}
}

func WithAACDecoderMPEG4AudioConfig(sampleRate int, channels int) AACDecoderOption {
	return func(cfg *aacDecoderConfig) {
		config, err := buildAACLCMPEG4AudioConfig(sampleRate, channels)
		if err != nil {
			cfg.audioConfig = nil
			return
		}
		cfg.audioConfig = config
	}
}
