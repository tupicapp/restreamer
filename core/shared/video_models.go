package shared

import "fmt"

type CanvasSpec struct {
	Width       int
	Height      int
	BackgroundY byte
	BackgroundU byte
	BackgroundV byte
}

type VideoLayout struct {
	X            int
	Y            int
	Width        int
	Height       int
	ZIndex       int
	Transparency float64
}

func NewBlackCanvasSpec(width, height int) CanvasSpec {
	return CanvasSpec{
		Width:       width,
		Height:      height,
		BackgroundY: 16,
		BackgroundU: 128,
		BackgroundV: 128,
	}
}

func ExpectedYUV420PSize(width, height int) (int, error) {
	if width <= 0 || height <= 0 {
		return 0, fmt.Errorf("invalid size %dx%d", width, height)
	}
	if width%2 != 0 || height%2 != 0 {
		return 0, fmt.Errorf("yuv420p requires even size, got %dx%d", width, height)
	}

	return width*height + 2*(width/2)*(height/2), nil
}

func (l VideoLayout) Validate() error {
	if l.Width <= 0 || l.Height <= 0 {
		return fmt.Errorf("invalid layout size %dx%d", l.Width, l.Height)
	}
	if l.Transparency < 0 || l.Transparency > 1 {
		return fmt.Errorf("transparency must be between 0 and 1, got %f", l.Transparency)
	}
	if l.X%2 != 0 || l.Y%2 != 0 || l.Width%2 != 0 || l.Height%2 != 0 {
		return fmt.Errorf(
			"yuv420p layout requires even x/y/width/height, got x=%d y=%d w=%d h=%d",
			l.X,
			l.Y,
			l.Width,
			l.Height,
		)
	}

	return nil
}
