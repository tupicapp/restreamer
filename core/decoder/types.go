package decoder

import "github.com/tupicapp/restreamer/core/raw"

type VideoDecoder interface {
	Start() error
	Output() <-chan *raw.VideoFrame
	Errors() <-chan error
	Close() error
}

type AudioDecoder interface {
	Start() error
	Output() <-chan *raw.AudioFrame
	Errors() <-chan error
	Close() error
}
