package encoder

import shared "restreamer/core/shared"

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
