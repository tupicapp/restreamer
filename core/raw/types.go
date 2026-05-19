package raw

import (
	"fmt"

	shared "github.com/tupicapp/restreamer/core/shared"
)

const (
	VideoCodec    = "rawvideo"
	YUV420PPixFmt = "yuv420p"
)

type VideoFrame struct {
	Frame  *shared.Frame
	Width  int
	Height int
	PixFmt string
}

type CanvasSpec = shared.CanvasSpec
type VideoLayout = shared.VideoLayout

type VideoPlacement struct {
	Input  VideoFrame
	Layout VideoLayout
}

func NewBlackCanvasSpec(width, height int) CanvasSpec {
	return shared.NewBlackCanvasSpec(width, height)
}

func ExpectedYUV420PSize(width, height int) (int, error) {
	return shared.ExpectedYUV420PSize(width, height)
}

func (f VideoFrame) Validate() error {
	if f.Frame == nil {
		return fmt.Errorf("raw frame is nil")
	}
	if len(f.Frame.Payload) == 0 {
		return fmt.Errorf("raw frame payload is empty")
	}
	if f.PixFmt == "" {
		f.PixFmt = YUV420PPixFmt
	}
	if f.PixFmt != YUV420PPixFmt {
		return fmt.Errorf("unsupported pixel format %q", f.PixFmt)
	}

	expected, err := ExpectedYUV420PSize(f.Width, f.Height)
	if err != nil {
		return err
	}
	if len(f.Frame.Payload[0]) != expected {
		return fmt.Errorf(
			"invalid raw frame payload size: got %d want %d for %dx%d",
			len(f.Frame.Payload[0]),
			expected,
			f.Width,
			f.Height,
		)
	}

	return nil
}
