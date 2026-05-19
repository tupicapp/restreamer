package raw

import (
	"fmt"
	"math"
	"sort"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
)

func ComposeYUV420P(spec CanvasSpec, placements []VideoPlacement) (*VideoFrame, error) {
	expectedSize, err := ExpectedYUV420PSize(spec.Width, spec.Height)
	if err != nil {
		return nil, err
	}

	canvas := make([]byte, expectedSize)
	dstY, dstU, dstV := splitYUV420P(canvas, spec.Width, spec.Height)

	fillPlane(dstY, spec.BackgroundY)
	fillPlane(dstU, spec.BackgroundU)
	fillPlane(dstV, spec.BackgroundV)

	layers := append([]VideoPlacement(nil), placements...)
	sort.SliceStable(layers, func(i, j int) bool {
		return layers[i].Layout.ZIndex < layers[j].Layout.ZIndex
	})

	for idx, placement := range layers {
		if err := placement.Layout.Validate(); err != nil {
			return nil, fmt.Errorf("placement %d: %w", idx, err)
		}
		if err := placement.Input.Validate(); err != nil {
			return nil, fmt.Errorf("placement %d: %w", idx, err)
		}
		opacity256 := layoutOpacity256(placement.Layout)
		if opacity256 == 0 {
			continue
		}

		srcBuf := placement.Input.Frame.Payload[0]
		srcY, srcU, srcV := splitYUV420P(srcBuf, placement.Input.Width, placement.Input.Height)

		blendPlaneNearest(
			dstY,
			spec.Width,
			spec.Width,
			spec.Height,
			srcY,
			placement.Input.Width,
			placement.Input.Width,
			placement.Input.Height,
			placement.Layout.X,
			placement.Layout.Y,
			placement.Layout.Width,
			placement.Layout.Height,
			opacity256,
		)

		blendPlaneNearest(
			dstU,
			spec.Width/2,
			spec.Width/2,
			spec.Height/2,
			srcU,
			placement.Input.Width/2,
			placement.Input.Width/2,
			placement.Input.Height/2,
			placement.Layout.X/2,
			placement.Layout.Y/2,
			placement.Layout.Width/2,
			placement.Layout.Height/2,
			opacity256,
		)

		blendPlaneNearest(
			dstV,
			spec.Width/2,
			spec.Width/2,
			spec.Height/2,
			srcV,
			placement.Input.Width/2,
			placement.Input.Width/2,
			placement.Input.Height/2,
			placement.Layout.X/2,
			placement.Layout.Y/2,
			placement.Layout.Width/2,
			placement.Layout.Height/2,
			opacity256,
		)
	}

	// Use the first placement's frame metadata for PTS/Duration/Timestamp consistency.
	// If no placements, use zero values (encoder will fall back to its own timing).
	var pts, dts time.Duration
	var duration time.Duration
	var timestamp time.Time
	var inputID string
	if len(placements) > 0 && placements[0].Input.Frame != nil {
		pts = placements[0].Input.Frame.PTS
		dts = placements[0].Input.Frame.DTS
		duration = placements[0].Input.Frame.Duration
		timestamp = placements[0].Input.Frame.Timestamp
		inputID = placements[0].Input.Frame.InputID
	}

	return &VideoFrame{
		Frame: &shared.Frame{
			Payload:    [][]byte{canvas},
			Codec:      VideoCodec,
			PacketType: YUV420PPixFmt,
			PTS:        pts,
			DTS:        dts,
			Duration:   duration,
			Timestamp:  timestamp,
			InputID:    inputID,
		},
		Width:  spec.Width,
		Height: spec.Height,
		PixFmt: YUV420PPixFmt,
	}, nil
}

func fillPlane(buf []byte, value byte) {
	for i := range buf {
		buf[i] = value
	}
}

func SplitYUV420P(buf []byte, width, height int) ([]byte, []byte, []byte) {
	ySize := width * height
	uvSize := (width / 2) * (height / 2)

	yPlane := buf[:ySize]
	uPlane := buf[ySize : ySize+uvSize]
	vPlane := buf[ySize+uvSize:]

	return yPlane, uPlane, vPlane
}

func splitYUV420P(buf []byte, width, height int) ([]byte, []byte, []byte) {
	return SplitYUV420P(buf, width, height)
}

func blendPlaneNearest(
	dst []byte,
	dstStride int,
	dstWidth int,
	dstHeight int,
	src []byte,
	srcStride int,
	srcWidth int,
	srcHeight int,
	dstX int,
	dstY int,
	dstW int,
	dstH int,
	opacity256 int,
) {
	if dstW <= 0 || dstH <= 0 || srcWidth <= 0 || srcHeight <= 0 || opacity256 <= 0 {
		return
	}

	clipLeft := maxInt(dstX, 0)
	clipTop := maxInt(dstY, 0)
	clipRight := minInt(dstX+dstW, dstWidth)
	clipBottom := minInt(dstY+dstH, dstHeight)

	if clipLeft >= clipRight || clipTop >= clipBottom {
		return
	}

	for y := clipTop; y < clipBottom; y++ {
		srcY := (y - dstY) * srcHeight / dstH
		dstRow := y * dstStride
		srcRow := srcY * srcStride

		for x := clipLeft; x < clipRight; x++ {
			srcX := (x - dstX) * srcWidth / dstW
			dstIndex := dstRow + x
			srcValue := src[srcRow+srcX]
			if opacity256 >= 256 {
				dst[dstIndex] = srcValue
				continue
			}

			dstValue := dst[dstIndex]
			dst[dstIndex] = blendByte(dstValue, srcValue, opacity256)
		}
	}
}

func layoutOpacity256(layout VideoLayout) int {
	opacity := 1 - layout.Transparency
	return int(math.Round(opacity * 256))
}

func blendByte(dst, src byte, opacity256 int) byte {
	if opacity256 <= 0 {
		return dst
	}
	if opacity256 >= 256 {
		return src
	}

	inverse := 256 - opacity256
	value := int(src)*opacity256 + int(dst)*inverse + 128
	return byte(value / 256)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
