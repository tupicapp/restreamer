package test

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func requireRTMPPublishingOrSkip(t *testing.T, rtmpURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-i", rtmpURL, "-show_streams")
		err := cmd.Run()
		cancel()
		if err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Skipf("RTMP fixture not reachable: %s", rtmpURL)
}
