//go:build !cgo || !media

package encoder

import (
	"fmt"

	"restreamer/irajstreamer/core/raw"
)

func NewH264Encoder(_ string, _ <-chan *raw.VideoFrame, _ ...H264EncoderOption) (VideoEncoder, error) {
	return nil, fmt.Errorf("h264 encoder requires cgo with libavcodec/libavutil installed")
}
