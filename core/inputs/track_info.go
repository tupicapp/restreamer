package inputs

type InputTrackInfo struct {
	Initialized     bool
	HasVideo        bool
	HasAudio        bool
	AudioSampleRate int
	AudioConfig     []byte
}

type TrackInfoProvider interface {
	TrackInfoSnapshot() InputTrackInfo
	TrackInfoChan() <-chan InputTrackInfo
}
