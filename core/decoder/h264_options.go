package decoder

type h264DecoderConfig struct {
	outputWidth  int
	outputHeight int
	outputBuffer int
}

type H264DecoderOption func(*h264DecoderConfig)

func WithH264DecoderOutputResolution(width, height int) H264DecoderOption {
	return func(cfg *h264DecoderConfig) {
		cfg.outputWidth = width
		cfg.outputHeight = height
	}
}

func WithH264DecoderOutputBuffer(size int) H264DecoderOption {
	return func(cfg *h264DecoderConfig) {
		cfg.outputBuffer = size
	}
}
