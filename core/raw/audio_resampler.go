package raw

type PCM16Resampler interface {
	Convert(frame *AudioFrame) (*AudioFrame, error)
	Close() error
}
