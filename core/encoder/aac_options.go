package encoder

type AACTransport string

const (
	AACTransportRaw  AACTransport = "raw"
	AACTransportADTS AACTransport = "adts"
)

const AACObjectTypeLC = 2

type aacEncoderConfig struct {
	outputBuffer int
	sampleRate   int
	channels     int
	bitRate      int
	transport    AACTransport
	objectType   int
	afterburner  bool
}

type AACEncoderOption func(*aacEncoderConfig)

func WithAACEncoderOutputBuffer(size int) AACEncoderOption {
	return func(cfg *aacEncoderConfig) {
		cfg.outputBuffer = size
	}
}

func WithAACEncoderSampleRate(sampleRate int) AACEncoderOption {
	return func(cfg *aacEncoderConfig) {
		cfg.sampleRate = sampleRate
	}
}

func WithAACEncoderChannels(channels int) AACEncoderOption {
	return func(cfg *aacEncoderConfig) {
		cfg.channels = channels
	}
}

func WithAACEncoderBitRate(bitRate int) AACEncoderOption {
	return func(cfg *aacEncoderConfig) {
		cfg.bitRate = bitRate
	}
}

func WithAACEncoderTransport(transport AACTransport) AACEncoderOption {
	return func(cfg *aacEncoderConfig) {
		cfg.transport = transport
	}
}

func WithAACEncoderObjectType(objectType int) AACEncoderOption {
	return func(cfg *aacEncoderConfig) {
		cfg.objectType = objectType
	}
}

func WithAACEncoderAfterburner(enabled bool) AACEncoderOption {
	return func(cfg *aacEncoderConfig) {
		cfg.afterburner = enabled
	}
}
