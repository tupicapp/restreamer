package test

import (
	"context"
	streaminputs "github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
	"path/filepath"
	"testing"
	"time"
)

// TestDirectHLSLivePassthrough snapshots a fixed segment window from a live HLS
// source, runs the direct HLSLive -> HLS output path against that frozen window,
// and verifies the output matches the same reference window.
func TestDirectHLSLivePassthrough(t *testing.T) {
	normalizedURL, normalizedPath, cleanup := setupDeterministicLiveFixtureServer(t, 15*time.Second)
	defer cleanup()

	outDir := filepath.Join(t.TempDir(), "output")
	testDirectHLSLivePassthroughWithSnapshot(t, normalizedURL, normalizedPath, outDir)
}

func testDirectHLSLivePassthroughWithSnapshot(t *testing.T, inputURL, referencePlaylist, outDir string) {
	t.Helper()

	outFolder := storage.NewFolder(outDir)
	hlsDest, err := outputs.NewHLSLiveDestination("hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(2*time.Second),
		outputs.WithHLSPlaylistSize(30),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination: %v", err)
	}

	hlsInput := streaminputs.NewHLSLive("hls-input", inputURL)

	videoIn := hlsInput.GetVideoChan()
	audioIn := hlsInput.GetAudioChan()
	videoOut := hlsDest.GetVideoChan()
	audioOut := hlsDest.GetAudioChan()

	stopChan := make(chan struct{})
	var videoCount, audioCount int64
	lastFrameTime := time.Now()
	noFrameTimeout := 5 * time.Second
	maxPassDuration := 20 * time.Second
	passDeadline := time.Now().Add(maxPassDuration)

	go func() {
		for {
			select {
			case <-stopChan:
				return
			case frame := <-videoIn:
				if frame == nil {
					return
				}
				videoCount++
				lastFrameTime = time.Now()
				select {
				case videoOut <- frame:
				case <-stopChan:
					return
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-stopChan:
				return
			case frame := <-audioIn:
				if frame == nil {
					return
				}
				audioCount++
				lastFrameTime = time.Now()
				select {
				case audioOut <- frame:
				case <-stopChan:
					return
				}
			}
		}
	}()

	hlsInput.Start()
	hlsDest.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := hlsInput.WaitForStart(ctx); err != nil {
		t.Fatalf("hls live input failed to start: %v", err)
	}
	if err := hlsDest.WaitForStart(ctx); err != nil {
		t.Fatalf("hls dest failed to start: %v", err)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if time.Since(lastFrameTime) > noFrameTimeout {
			t.Logf("No frames for %v, stopping", noFrameTimeout)
			break
		}
		if time.Now().After(passDeadline) {
			t.Logf("Reached max pass duration %v, stopping", maxPassDuration)
			break
		}
	}

	close(stopChan)
	time.Sleep(1500 * time.Millisecond)

	hlsInput.Close()
	hlsDest.Close()
	time.Sleep(1 * time.Second)

	t.Logf("Video frames passed: %d", videoCount)
	t.Logf("Audio frames passed: %d", audioCount)

	outputPlaylist := filepath.Join(outDir, "stream.m3u8")
	waitForHLSArtifacts(t, outDir, 20*time.Second, 2)
	hlsErr := EqualHLS(referencePlaylist, outputPlaylist)

	if hlsErr.ProbeError1 != nil {
		t.Errorf("ffprobe failed on input playlist: %v", hlsErr.ProbeError1)
	}
	if hlsErr.ProbeError2 != nil {
		t.Errorf("ffprobe failed on output playlist: %v", hlsErr.ProbeError2)
	}

	if hlsErr.StreamCountMismatch {
		t.Errorf("Stream count mismatch: input=%d, output=%d",
			hlsErr.StreamCount1, hlsErr.StreamCount2)
	}
	for _, sd := range hlsErr.StreamDiffs {
		t.Errorf("Stream[%d] %s mismatch: input=%q output=%q",
			sd.Index, sd.Field, sd.Value1, sd.Value2)
	}

	refVideo, refAudio := collectHLSFrames(t, referencePlaylist, "direct-live-ref")
	outVideo, outAudio := collectHLSFrames(t, outputPlaylist, "direct-live-out")

	assertWindowMatches(t, "direct-hlslive-video", outVideo, refVideo)
	assertWindowMatches(t, "direct-hlslive-audio", outAudio, refAudio)
}
