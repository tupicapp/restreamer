package outputs

import "time"

const (
	// Video
	DefaultWidth             = 1280
	DefaultHeight            = 720
	DefaultFPS2              = 60 // 60 if you want super smooth, but 30 is the safe default
	DefaultFPS               = 30 // 60 if you want super smooth, but 30 is the safe default
	DefaultErrorLoggerBuffer = 1024
	DefaultPixFormat         = "yuv420p"

	// Audio
	DefaultAudioChannels = 2           // Stereo
	DefaultAudioProfile  = 2           // 48 kHz, broadcast standard
	DefaultAudioRate     = 44100       // 48 kHz, broadcast standard
	DefaultAudioFormat   = "s16le"     // PCM
	DefaultAudioCodec    = "aac"       // PCM
	DefaultPreset        = "ultrafast" // PCM
	DefaultVideoFormat   = "libx264"

	DefaultChannelBufferSize  = 100
	DefaultReadDeadline       = time.Millisecond * 100
	DefaultWriteDeadline      = time.Millisecond * 100
	DefaultMaxAcceptedLatancy = time.Millisecond * 50
)

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
