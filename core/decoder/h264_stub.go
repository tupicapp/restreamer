//go:build !cgo || !media

package decoder

import (
	"fmt"

	shared "github.com/tupicapp/restreamer/core/shared"
)

func NewH264Decoder(_ string, _ <-chan *shared.Frame, _ ...H264DecoderOption) (VideoDecoder, error) {
	return nil, fmt.Errorf("h264 decoder requires cgo with libavcodec/libswscale installed")
}

func InferResolutionFromH264Frame(_ *shared.Frame) (int, int, bool) {
	return 0, 0, false
}
