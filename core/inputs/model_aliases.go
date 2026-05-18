package inputs

import (
	shared "restreamer/core/shared"
	"time"
)

type StreamType = shared.StreamType
type Stream = shared.Stream
type Frame = shared.Frame
type State = shared.State
type Event = shared.Event

const (
	InputTypeSRT   = shared.InputTypeSRT
	InputTypeRTMP  = shared.InputTypeRTMP
	InputTypeRTSP  = shared.InputTypeRTSP
	InputTypeFILE  = shared.InputTypeFILE
	InputTypePRINT = shared.InputTypePRINT

	DefaultWidth             = 1280
	DefaultHeight            = 720
	DefaultFPS               = 30
	DefaultAudioChannels     = 2
	DefaultAudioRate         = 44100
	DefaultAudioFormat       = "s16le"
	DefaultChannelBufferSize = 100
	DefaultReadDeadline      = 100 * time.Millisecond
	DefaultWriteDeadline     = 100 * time.Millisecond
	DefaultAudioProfile      = 2
)
