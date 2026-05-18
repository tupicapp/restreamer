//go:build !cgo || !media

package raw

import "fmt"

func ConvertPCM16AudioFrame(frame *AudioFrame, sampleRate int, channels int) (*AudioFrame, error) {
	return nil, fmt.Errorf(
		"audio resampling requires cgo: input=%v target=%d/%d",
		frame != nil,
		sampleRate,
		channels,
	)
}
