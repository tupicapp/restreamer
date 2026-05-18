package encoder

type h264EncoderConfig struct {
	outputBuffer int
	fps          int
	gopSize      int
	bitRate      int64
}

type H264EncoderOption func(*h264EncoderConfig)

func WithH264EncoderOutputBuffer(size int) H264EncoderOption {
	return func(cfg *h264EncoderConfig) {
		cfg.outputBuffer = size
	}
}

func WithH264EncoderFPS(fps int) H264EncoderOption {
	return func(cfg *h264EncoderConfig) {
		cfg.fps = fps
	}
}

func WithH264EncoderGOPSize(size int) H264EncoderOption {
	return func(cfg *h264EncoderConfig) {
		cfg.gopSize = size
	}
}

func WithH264EncoderBitRate(bitRate int64) H264EncoderOption {
	return func(cfg *h264EncoderConfig) {
		cfg.bitRate = bitRate
	}
}
