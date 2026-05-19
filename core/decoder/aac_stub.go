//go:build !cgo || !media

package decoder

import (
	"fmt"

	shared "github.com/tupicapp/restreamer/core/shared"
)

func NewAACDecoder(_ string, _ <-chan *shared.Frame, opts ...AACDecoderOption) (AudioDecoder, error) {
	cfg := aacDecoderConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := validateAACDecoderConfig(cfg); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("aac decoder requires cgo with FFmpeg libavcodec/libavutil/libswresample installed")
}
