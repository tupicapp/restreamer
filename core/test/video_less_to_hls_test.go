package test

import "testing"

func TestVideoLessToHLS(t *testing.T) {
	runCompatRTMPToHLSTest(t, "video-less", testRTMPVideoLessURL, false)
}
