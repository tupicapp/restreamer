package test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	corehelpers "github.com/tupicapp/restreamer/core"
	"github.com/tupicapp/restreamer/core/storage"
	"github.com/tupicapp/restreamer/core/streamfactory"
)

func TestNoSwitchRTMPCompatibleInputsGenerateDecodableProgramLiveAndRecord(t *testing.T) {
	requireBinary(t, "ffprobe")

	inputs := []struct {
		id  string
		url string
	}{
		{id: "no-switch-rtmp-av", url: testRTMPAVURL},
		{id: "no-switch-rtmp-audio-less", url: testRTMPAudioLessURL},
		{id: "no-switch-rtmp-video-less", url: testRTMPVideoLessURL},
	}
	for _, in := range inputs {
		requireRTMPPublishingOrSkip(t, in.url, 10*time.Second)
	}

	streamer := corehelpers.NewStreamer(corehelpers.WithChannelID("no-switch-program"))
	defer streamer.Close()
	streamer.StartLife()

	type inputArtifacts struct {
		id        string
		stream    Stream
		liveDir   string
		recordDir string
	}
	artifacts := make([]inputArtifacts, 0, len(inputs))

	for _, in := range inputs {
		stream, err := streamfactory.NewInput(in.id, in.url)
		if err != nil {
			t.Fatalf("NewInput(%q) error = %v", in.url, err)
		}

		if err := streamer.AddInput(stream); err != nil {
			t.Fatalf("AddInput(%q) error = %v", in.id, err)
		}

		liveDir := filepath.Join(t.TempDir(), "live-"+in.id)
		if err := os.MkdirAll(liveDir, 0755); err != nil {
			t.Fatalf("MkdirAll(liveDir) error = %v", err)
		}
		if err := streamer.SetInputHLSFolder(in.id, storage.NewFolder(liveDir)); err != nil {
			t.Fatalf("SetInputHLSFolder(%q) error = %v", in.id, err)
		}

		recordDir := filepath.Join(t.TempDir(), "record-"+in.id)
		if err := os.MkdirAll(recordDir, 0755); err != nil {
			t.Fatalf("MkdirAll(recordDir) error = %v", err)
		}
		if err := streamer.SetInputRecordFolder(in.id, storage.NewFolder(recordDir)); err != nil {
			t.Fatalf("SetInputRecordFolder(%q) error = %v", in.id, err)
		}

		artifacts = append(artifacts, inputArtifacts{
			id:        in.id,
			stream:    stream,
			liveDir:   liveDir,
			recordDir: recordDir,
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	for _, in := range artifacts {
		if err := in.stream.WaitForStart(waitCtx); err != nil {
			t.Fatalf("input %q WaitForStart() error = %v", in.id, err)
		}
	}

	inputIDs := make([]string, 0, len(artifacts))
	for _, in := range artifacts {
		inputIDs = append(inputIDs, in.id)
	}
	slices.Sort(inputIDs)
	waitForNoSwitchProgramURLs(t, streamer, inputIDs, 25*time.Second)

	state := streamer.State()
	if state.CurrentInputID != "" {
		t.Fatalf("expected CurrentInputID to remain empty, got %q", state.CurrentInputID)
	}
	assertURLListHasIDs(t, state.AvailableProgramHLSURLs, inputIDs)
	assertURLListHasIDs(t, state.ProgramRecordHLSURLs, inputIDs)

	for _, in := range artifacts {
		waitForHLSArtifacts(t, in.liveDir, 25*time.Second, 2)
		livePlaylist := filepath.Join(in.liveDir, "stream.m3u8")
		assertHLSPlayableWithFFmpeg(t, livePlaylist)

		recordPlaylist := waitForLatestRecordPlaylist(t, in.recordDir, 25*time.Second, 2)
		assertHLSPlayableWithFFmpeg(t, recordPlaylist)
	}
}

func waitForNoSwitchProgramURLs(t *testing.T, streamer *corehelpers.Streamer, inputIDs []string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state := streamer.State()
		if state.CurrentInputID == "" && len(state.AvailableProgramHLSURLs) == len(inputIDs) && len(state.ProgramRecordHLSURLs) == len(inputIDs) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	state := streamer.State()
	t.Fatalf("timed out waiting for no-switch program urls: current=%q live=%v record=%v", state.CurrentInputID, state.AvailableProgramHLSURLs, state.ProgramRecordHLSURLs)
}

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

func waitForLatestRecordPlaylist(t *testing.T, recordDir string, timeout time.Duration, minSegments int) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(recordDir)
		if err != nil {
			t.Fatalf("ReadDir(%s) error = %v", recordDir, err)
		}

		latestSession := ""
		for _, entry := range entries {
			if entry.IsDir() && entry.Name() > latestSession {
				latestSession = entry.Name()
			}
		}
		if latestSession == "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		sessionDir := filepath.Join(recordDir, latestSession)
		playlistPath := filepath.Join(sessionDir, "stream.m3u8")
		if _, err := os.Stat(playlistPath); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		segmentFiles, err := filepath.Glob(filepath.Join(sessionDir, "*.ts"))
		if err != nil {
			t.Fatalf("glob record segments failed: %v", err)
		}
		if len(segmentFiles) >= minSegments {
			return playlistPath
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for record playlist in %s", recordDir)
	return ""
}
