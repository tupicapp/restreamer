package config

type Config struct {
	Storage  Storage
	TestURLs TestURLs
}

type Storage struct {
	RecordingsRoot string
}

type TestURLs struct {
	RTMPURL string
}
