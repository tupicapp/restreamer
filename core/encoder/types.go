package encoder

import shared "github.com/tupicapp/restreamer/core/shared"

type VideoEncoder interface {
	Start() error
	Output() <-chan *shared.Frame
	Errors() <-chan error
	Close() error
}

type AudioEncoder interface {
	Start() error
	Output() <-chan *shared.Frame
	Errors() <-chan error
	Close() error
	AudioSpecificConfig() []byte
}
