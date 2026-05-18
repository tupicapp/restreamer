//go:build !cgo || !media

package encoder

import (
	"fmt"

	"restreamer/irajstreamer/core/raw"
)

func NewAACEncoder(_ string, _ <-chan *raw.AudioFrame, opts ...AACEncoderOption) (AudioEncoder, error) {
	cfg := aacEncoderConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := validateAACEncoderConfig(cfg); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("aac encoder requires cgo with FFmpeg libavcodec/libavutil/libswresample installed")
}
