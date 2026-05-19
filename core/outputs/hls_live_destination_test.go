package outputs

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"restreamer/core/config"
	"restreamer/core/shared"
	"restreamer/core/storage"
	"restreamer/core/test_tools"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/spf13/viper"
)

var testConfigOnce sync.Once

func TestHLSFileDestination_UsesGOPBufferAndGatesUntilKeyframe(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls")
	outFolder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	stream, err := NewHLSLiveDestination("hls-dest-test", outFolder)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}

	dest, ok := stream.(*hlsLive)
	if !ok {
		t.Fatalf("unexpected stream type %T", stream)
	}
	defer dest.Close()

	if !dest.gopBuffer.IsRebase() {
		t.Fatalf("expected HLS destination GOP buffer to enable timeline rebasing")
	}

	if dest.GetVideoChan() != dest.gopBuffer.VideoFrameChan {
		t.Fatalf("video channel is not wired to GOP buffer")
	}
	if dest.GetAudioChan() != dest.gopBuffer.AudioFrameChan {
		t.Fatalf("audio channel is not wired to GOP buffer")
	}

	dest.Start()

	// Non-keyframe arrives before first keyframe; GOPBuffer should drop it.
	dest.GetVideoChan() <- &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x41, 0x9a, 0x22}}, // non-IDR NAL
		IsKeyFrame: false,
		PTS:        100 * time.Millisecond,
		DTS:        100 * time.Millisecond,
		SequenceID: 1,
	}

	time.Sleep(120 * time.Millisecond)
	if _, statErr := os.Stat(filepath.Join(outDir, "stream.m3u8")); !os.IsNotExist(statErr) {
		t.Fatalf("playlist should not exist before first keyframe, statErr=%v", statErr)
	}

	// First keyframe should pass GOPBuffer and initialize first segment + playlist.
	dest.GetVideoChan() <- &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        2 * time.Second,
		DTS:        2 * time.Second,
		SequenceID: 2,
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statErr := os.Stat(filepath.Join(outDir, "stream.m3u8")); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("playlist was not created after first keyframe")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHLSFileDestination_SwitchInputIDTimestampReset_DoesNotDropByDestination(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls")
	outFolder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	stream, err := NewHLSLiveDestination("hls-switch-test", outFolder)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}

	dest, ok := stream.(*hlsLive)
	if !ok {
		t.Fatalf("unexpected stream type %T", stream)
	}
	defer dest.Close()

	dest.Start()

	videoAKey := &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        2 * time.Second,
		DTS:        2 * time.Second,
		SequenceID: 1,
	}
	videoAInter := &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x41, 0x9a, 0x22}},
		IsKeyFrame: false,
		PTS:        2033 * time.Millisecond,
		DTS:        2033 * time.Millisecond,
		SequenceID: 2,
	}
	// Simulate switch to a new input whose timeline restarts near zero.
	videoBNonKey := &shared.Frame{
		InputID:    "input-b",
		Codec:      "h264",
		Payload:    [][]byte{{0x41, 0x9a, 0x23}},
		IsKeyFrame: false,
		PTS:        100 * time.Millisecond,
		DTS:        100 * time.Millisecond,
		SequenceID: 3,
	}
	videoBKey := &shared.Frame{
		InputID:    "input-b",
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        200 * time.Millisecond,
		DTS:        200 * time.Millisecond,
		SequenceID: 4,
	}

	dest.GetVideoChan() <- videoAKey
	dest.GetVideoChan() <- videoAInter
	dest.GetVideoChan() <- videoBNonKey
	dest.GetVideoChan() <- videoBKey

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if dest.TotalVideoFrames >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if dest.TotalVideoFrames < 3 {
		t.Fatalf("expected destination to process switched video frames, got total=%d", dest.TotalVideoFrames)
	}
	if dest.DroppedVideoFrames != 0 {
		t.Fatalf("expected no destination-side drops on input switch timestamp reset, got drops=%v", dest.DroppedVideoFrames)
	}

	if _, statErr := os.Stat(filepath.Join(outDir, "stream.m3u8")); statErr != nil {
		t.Fatalf("expected playlist after switched input frames, got err=%v", statErr)
	}
}

func TestHLSFileDestination_DoesNotStartSegmentFromAudioOnly(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls")
	outFolder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	stream, err := NewHLSLiveDestination("hls-audio-gate-test", outFolder)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}

	dest, ok := stream.(*hlsLive)
	if !ok {
		t.Fatalf("unexpected stream type %T", stream)
	}
	defer dest.Close()

	dest.Start()

	dest.GetAudioChan() <- &shared.Frame{
		InputID:    "input-a",
		Codec:      "aac",
		Payload:    [][]byte{{0x11, 0x22, 0x33}},
		IsKeyFrame: true,
		PTS:        100 * time.Millisecond,
		DTS:        100 * time.Millisecond,
		SequenceID: 1,
	}

	time.Sleep(150 * time.Millisecond)
	if _, statErr := os.Stat(filepath.Join(outDir, "stream.m3u8")); !os.IsNotExist(statErr) {
		t.Fatalf("playlist should not exist before first video keyframe, statErr=%v", statErr)
	}

	dest.GetVideoChan() <- &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        2 * time.Second,
		DTS:        2 * time.Second,
		SequenceID: 2,
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statErr := os.Stat(filepath.Join(outDir, "stream.m3u8")); statErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("playlist was not created after first video keyframe")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForHLSArtifacts(t *testing.T, outDir string, timeout time.Duration, minSegments int) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	playlistPath := filepath.Join(outDir, "stream.m3u8")

	for time.Now().Before(deadline) {
		playlistExists := false
		if _, err := os.Stat(playlistPath); err == nil {
			playlistExists = true
		}

		segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
		if err != nil {
			t.Fatalf("glob segments failed: %v", err)
		}

		if playlistExists && len(segmentFiles) >= minSegments {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	segmentFiles, _ := filepath.Glob(filepath.Join(outDir, "*.ts"))
	t.Fatalf("timed out waiting for HLS output in %s (segments=%d)", outDir, len(segmentFiles))
}

func assertHLSPlaylistLooksValid(t *testing.T, playlistPath string) {
	t.Helper()

	content, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("failed reading playlist %s: %v", playlistPath, err)
	}

	text := string(content)
	if !strings.Contains(text, "#EXTM3U") {
		t.Fatalf("playlist %s is missing #EXTM3U", playlistPath)
	}
	if !strings.Contains(text, "#EXTINF") {
		t.Fatalf("playlist %s has no segments", playlistPath)
	}
}

func assertHLSPlayableWithFFmpeg(t *testing.T, playlistPath string) {
	t.Helper()

	info, err := test_tools.ProbeStream(playlistPath)
	if err != nil {
		t.Fatalf("ffprobe failed on %s: %v", playlistPath, err)
	}

	format := strings.ToLower(info.Format)
	if !strings.Contains(format, "hls") && !strings.Contains(format, "applehttp") {
		t.Fatalf("ffprobe format %q is not recognized as HLS", info.Format)
	}

	videoCodec := strings.ToLower(info.VideoCodec)
	audioCodec := strings.ToLower(info.AudioCodec)
	if videoCodec == "" || !strings.Contains(videoCodec, "h264") {
		t.Fatalf("ffprobe video codec %q is not recognized as h264", info.VideoCodec)
	}
	if audioCodec == "" || !strings.Contains(audioCodec, "aac") {
		t.Fatalf("ffprobe audio codec %q is not recognized as AAC", info.AudioCodec)
	}

	cmd := exec.Command(
		"ffmpeg",
		"-v", "error",
		"-i", playlistPath,
		"-t", "6",
		"-map", "0",
		"-f", "null",
		"-",
	)
	runCmdEnsureNoStderrWithTimeout(t, cmd, "ffmpeg verify HLS playback", 60*time.Second)
}

func runCmdEnsureNoStderrWithTimeout(t *testing.T, cmd *exec.Cmd, label string, timeout time.Duration) {
	t.Helper()

	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("%s: failed to start: %v", label, err)
	}

	waitErrChan := make(chan error, 1)
	go func() {
		waitErrChan <- cmd.Wait()
	}()

	select {
	case waitErr := <-waitErrChan:
		stderrText := strings.TrimSpace(stderr.String())
		if stderrText != "" {
			t.Logf("ffmpeg stderr: %s", stderrText)
		}
		if waitErr != nil {
			t.Fatalf("%s failed: %v\n%s", label, waitErr, stderrText)
		}
		if stderrText != "" {
			t.Fatalf("%s produced stderr output; treat as failure:\n%s", label, stderrText)
		}
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		t.Fatalf("%s timed out after %s", label, timeout)
	}
}

func assertHLSPlaylistHasDiscontinuities(t *testing.T, playlistPath string) {
	t.Helper()

	content, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("failed reading playlist %s: %v", playlistPath, err)
	}

	if !strings.Contains(string(content), "#EXT-X-DISCONTINUITY") {
		t.Fatalf("playlist %s should contain discontinuities between standalone TS segments", playlistPath)
	}
}

func assertHLSSegmentTimelineIsContinuous(t *testing.T, outDir string) {
	t.Helper()

	segmentFiles, err := filepath.Glob(filepath.Join(outDir, "*.ts"))
	if err != nil {
		t.Fatalf("glob segments failed: %v", err)
	}
	if len(segmentFiles) < 2 {
		t.Fatalf("need at least 2 segments to validate timeline continuity, got %d", len(segmentFiles))
	}

	sort.Strings(segmentFiles)
	firstStart := firstPacketPTSSeconds(t, segmentFiles[0])
	secondStart := firstPacketPTSSeconds(t, segmentFiles[1])

	if secondStart <= firstStart {
		t.Fatalf("expected second segment to continue timeline after first: first_start=%f second_start=%f", firstStart, secondStart)
	}
}

func firstPacketPTSSeconds(t *testing.T, segmentPath string) float64 {
	t.Helper()

	cmd := exec.Command(
		"ffprobe",
		"-v", "error",
		"-show_entries", "packet=pts_time",
		"-of", "csv=p=0",
		"-read_intervals", "%+#1",
		segmentPath,
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ffprobe failed on %s: %v", segmentPath, err)
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		t.Fatalf("ffprobe returned no packet pts for %s", segmentPath)
	}

	for _, field := range strings.Split(line, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		v, parseErr := strconv.ParseFloat(field, 64)
		if parseErr == nil {
			return v
		}
	}

	t.Fatalf("failed to parse first packet pts from %s: %q", segmentPath, line)
	return 0
}

func TestHLSDestination_RecordMode_KeepsAllEntriesInPlaylist(t *testing.T) {
	outDir := t.TempDir()
	outFolderRaw := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir},
	}).RecordingsRoot()
	outFolder, err := shared.AdaptFolder(outFolderRaw)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}

	// Record mode (isLive=false): entries should never be trimmed regardless of playlistSize.
	dest := &hlsLive{
		id:              "hls-record-test",
		outputFolder:    outFolder,
		segmentDuration: time.Second,
		playlistSize:    2,
		targetDuration:  1,
		isLive:          false,
		events:          shared.NewEventEmitter(32),
	}

	for i := 0; i < 5; i++ {
		if err := dest.openSegmentLocked(time.Duration(i) * time.Second); err != nil {
			t.Fatalf("openSegmentLocked %d failed: %v", i, err)
		}
		if err := dest.closeCurrentSegmentLocked(false); err != nil {
			t.Fatalf("closeCurrentSegmentLocked %d failed: %v", i, err)
		}
	}

	if len(dest.entries) != 5 {
		t.Fatalf("record mode: expected all 5 entries, got %d", len(dest.entries))
	}
}

func TestHLSDestination_LiveMode_TrimsEntriesAtPlaylistSize(t *testing.T) {
	outDir := t.TempDir()
	outFolderRaw := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir},
	}).RecordingsRoot()
	outFolder, err := shared.AdaptFolder(outFolderRaw)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}

	dest := &hlsLive{
		id:              "hls-live-trim-test",
		outputFolder:    outFolder,
		segmentDuration: time.Second,
		playlistSize:    3,
		targetDuration:  1,
		isLive:          true,
		events:          shared.NewEventEmitter(32),
	}

	for i := 0; i < 6; i++ {
		if err := dest.openSegmentLocked(time.Duration(i) * time.Second); err != nil {
			t.Fatalf("openSegmentLocked %d failed: %v", i, err)
		}
		if err := dest.closeCurrentSegmentLocked(false); err != nil {
			t.Fatalf("closeCurrentSegmentLocked %d failed: %v", i, err)
		}
	}

	if len(dest.entries) != 3 {
		t.Fatalf("live mode: expected entries trimmed to playlistSize=3, got %d", len(dest.entries))
	}
	if dest.entries[0].Seq != 3 {
		t.Fatalf("live mode: expected first kept entry to be seq 3 (oldest in window), got seq %d", dest.entries[0].Seq)
	}
}

func TestHLSDestination_LiveMode_MediaSequenceAdvancesWithWindow(t *testing.T) {
	outDir := t.TempDir()
	outFolderRaw := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir},
	}).RecordingsRoot()
	outFolder, err := shared.AdaptFolder(outFolderRaw)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}

	dest := &hlsLive{
		id:              "hls-live-seq-test",
		outputFolder:    outFolder,
		segmentDuration: time.Second,
		playlistSize:    2,
		targetDuration:  1,
		isLive:          true,
		events:          shared.NewEventEmitter(32),
	}

	for i := 0; i < 4; i++ {
		if err := dest.openSegmentLocked(time.Duration(i) * time.Second); err != nil {
			t.Fatalf("openSegmentLocked %d failed: %v", i, err)
		}
		if err := dest.closeCurrentSegmentLocked(false); err != nil {
			t.Fatalf("closeCurrentSegmentLocked %d failed: %v", i, err)
		}
	}

	data, err := os.ReadFile(filepath.Join(outDir, "stream.m3u8"))
	if err != nil {
		t.Fatalf("read playlist: %v", err)
	}
	text := string(data)

	if !strings.Contains(text, "#EXT-X-MEDIA-SEQUENCE:2") {
		t.Fatalf("expected #EXT-X-MEDIA-SEQUENCE:2 after 4 segments with playlistSize=2, playlist:\n%s", text)
	}
	if strings.Contains(text, "seg_000000.ts") {
		t.Fatalf("expected evicted seg_000000.ts not to appear in live playlist, but it does:\n%s", text)
	}
}

func TestHLSDestination_RecordMode_PlaylistEndsWithEndList(t *testing.T) {
	outDir := t.TempDir()
	outFolderRaw := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir},
	}).RecordingsRoot()
	outFolder, err := shared.AdaptFolder(outFolderRaw)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}

	dest := &hlsLive{
		id:              "hls-record-endlist-test",
		outputFolder:    outFolder,
		segmentDuration: time.Second,
		playlistSize:    6,
		targetDuration:  1,
		isLive:          false,
		events:          shared.NewEventEmitter(32),
	}

	if err := dest.openSegmentLocked(0); err != nil {
		t.Fatalf("openSegmentLocked failed: %v", err)
	}
	if err := dest.closeCurrentSegmentLocked(true); err != nil {
		t.Fatalf("closeCurrentSegmentLocked(endList=true) failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "stream.m3u8"))
	if err != nil {
		t.Fatalf("read playlist: %v", err)
	}
	if !strings.Contains(string(data), "#EXT-X-ENDLIST") {
		t.Fatalf("expected #EXT-X-ENDLIST in record-mode playlist, got:\n%s", string(data))
	}
}

func TestHLSDestination_WithHLSLiveMode_SetsField(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls")
	outFolder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	stream, err := NewHLSLiveDestination("hls-opts-test", outFolder, WithHLSLiveMode(), WithHLSCleanInterval(5*time.Second))
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}
	dest := stream.(*hlsLive)
	defer dest.Close()

	if !dest.isLive {
		t.Fatalf("expected isLive=true after WithHLSLiveMode()")
	}
	if dest.cleanInterval != 5*time.Second {
		t.Fatalf("expected cleanInterval=5s, got %v", dest.cleanInterval)
	}
}

func TestHLSDestination_WithoutLiveMode_DefaultsToRecord(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls")
	outFolder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	stream, err := NewHLSLiveDestination("hls-record-default-test", outFolder)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}
	dest := stream.(*hlsLive)
	defer dest.Close()

	if dest.isLive {
		t.Fatalf("expected isLive=false by default (record mode)")
	}
}

func TestHLSDestination_SegmentAndPlaylistOptionsApplied(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls")
	outFolder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	stream, err := NewHLSLiveDestination("hls-custom-opts-test", outFolder,
		WithHLSSegmentDuration(4*time.Second),
		WithHLSPlaylistSize(8),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}
	dest := stream.(*hlsLive)
	defer dest.Close()

	if dest.segmentDuration != 4*time.Second {
		t.Fatalf("expected segmentDuration=4s, got %v", dest.segmentDuration)
	}
	if dest.playlistSize != 8 {
		t.Fatalf("expected playlistSize=8, got %d", dest.playlistSize)
	}
}

func TestHLSDestination_H264SPSPPSPresent(t *testing.T) {
	spsNALU := []byte{0x67, 0x42, 0x00, 0x1f}
	ppsNALU := []byte{0x68, 0xce, 0x38, 0x80}
	idrNALU := []byte{0x65, 0x88, 0x84}

	hasSPS, hasPPS := h264SPSPPSPresent([][]byte{spsNALU, ppsNALU, idrNALU})
	if !hasSPS || !hasPPS {
		t.Fatalf("expected hasSPS=true hasPPS=true, got hasSPS=%v hasPPS=%v", hasSPS, hasPPS)
	}

	hasSPS, hasPPS = h264SPSPPSPresent([][]byte{idrNALU})
	if hasSPS || hasPPS {
		t.Fatalf("expected hasSPS=false hasPPS=false for IDR-only slice")
	}
}

func TestHLSDestination_EnsureSPSPPSOnKeyFrame_InjectsWhenMissing(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	idr := []byte{0x65, 0x88, 0x84}

	dest := &hlsLive{cachedSPS: sps, cachedPPS: pps}
	frame := &shared.Frame{IsKeyFrame: true, Payload: [][]byte{idr}}
	out := dest.ensureSPSPPSOnKeyFrame(frame)

	if len(out) != 3 {
		t.Fatalf("expected 3 NALUs after SPS/PPS injection, got %d", len(out))
	}
	hasSPS, hasPPS := h264SPSPPSPresent(out)
	if !hasSPS || !hasPPS {
		t.Fatalf("expected SPS and PPS present after injection")
	}
}

func TestHLSDestination_EnsureSPSPPSOnKeyFrame_NoOpForNonKeyFrame(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	inter := []byte{0x41, 0x9a, 0x22}

	dest := &hlsLive{cachedSPS: sps, cachedPPS: pps}
	frame := &shared.Frame{IsKeyFrame: false, Payload: [][]byte{inter}}
	out := dest.ensureSPSPPSOnKeyFrame(frame)

	if len(out) != 1 {
		t.Fatalf("expected 1 NALU unchanged for non-keyframe, got %d", len(out))
	}
}

func TestHLSDestination_CacheH264ParameterSets(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	dest := &hlsLive{}

	dest.cacheH264ParameterSets([][]byte{sps, pps})

	if string(dest.cachedSPS) != string(sps) {
		t.Fatalf("expected cachedSPS to be updated")
	}
	if string(dest.cachedPPS) != string(pps) {
		t.Fatalf("expected cachedPPS to be updated")
	}
}

func TestHLSNALTypeFromUnit_RejectsEmpty(t *testing.T) {
	if h264NALTypeFromUnit(nil) != 0 {
		t.Fatalf("expected 0 for nil NALU")
	}
	if h264NALTypeFromUnit([]byte{}) != 0 {
		t.Fatalf("expected 0 for empty NALU")
	}
}

func TestHLSNALTypeFromUnit_StripAnnexB(t *testing.T) {
	withStartCode := []byte{0x00, 0x00, 0x01, 0x67, 0x42}
	if h264NALTypeFromUnit(withStartCode) != 7 {
		t.Fatalf("expected NAL type 7 (SPS) after stripping 3-byte start code")
	}

	withLongStartCode := []byte{0x00, 0x00, 0x00, 0x01, 0x68, 0xce}
	if h264NALTypeFromUnit(withLongStartCode) != 8 {
		t.Fatalf("expected NAL type 8 (PPS) after stripping 4-byte start code")
	}
}

func TestHLSDestination_CloseWritesEndList(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls")
	outFolder := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	stream, err := NewHLSLiveDestination("hls-close-endlist-test", outFolder,
		WithHLSSegmentDuration(time.Second),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}
	dest := stream.(*hlsLive)

	dest.Start()

	dest.GetVideoChan() <- &shared.Frame{
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        time.Second,
		DTS:        time.Second,
		SequenceID: 1,
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(outDir, "stream.m3u8")); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	dest.Close()

	data, err := os.ReadFile(filepath.Join(outDir, "stream.m3u8"))
	if err != nil {
		t.Fatalf("read playlist after close: %v", err)
	}
	if !strings.Contains(string(data), "#EXT-X-ENDLIST") {
		t.Fatalf("expected #EXT-X-ENDLIST after Close(), got:\n%s", string(data))
	}
}

func TestHLSFileDestination_NormalizeAudioTimestamp90k_StrictlyMonotonic(t *testing.T) {
	dest := &hlsLive{}

	first := dest.normalizeAudioTimestamp90k(89655)
	second := dest.normalizeAudioTimestamp90k(89655)
	third := dest.normalizeAudioTimestamp90k(89650)

	if !(first < second && second < third) {
		t.Fatalf("expected strictly increasing audio PTS, got %d, %d, %d", first, second, third)
	}
}

func TestHLSFileDestination_NormalizeVideoTimestamps90k_StrictlyMonotonic(t *testing.T) {
	dest := &hlsLive{}

	pts1, dts1 := dest.normalizeVideoTimestamps90k(1000, 1000)
	pts2, dts2 := dest.normalizeVideoTimestamps90k(1000, 1000)
	pts3, dts3 := dest.normalizeVideoTimestamps90k(999, 998)

	if !(dts1 < dts2 && dts2 < dts3) {
		t.Fatalf("expected strictly increasing video DTS, got %d, %d, %d", dts1, dts2, dts3)
	}
	if !(pts1 < pts2 && pts2 < pts3) {
		t.Fatalf("expected strictly increasing video PTS, got %d, %d, %d", pts1, pts2, pts3)
	}
	if pts1 < dts1 || pts2 < dts2 || pts3 < dts3 {
		t.Fatalf("expected video PTS >= DTS, got (%d,%d), (%d,%d), (%d,%d)", pts1, dts1, pts2, dts2, pts3, dts3)
	}
}

func TestHLSFileDestination_ComputeTargetDuration_UsesLongestSegment(t *testing.T) {
	dest := &hlsLive{
		targetDuration: 2,
		entries: []hlsSegmentEntry{
			{Duration: 1.2},
			{Duration: 15.1},
			{Duration: 15.9},
		},
	}

	got := dest.computeTargetDuration()
	if got != 16 {
		t.Fatalf("expected target duration 16, got %d", got)
	}
}

func TestDurationTo90k_UsesExactIntegerMath(t *testing.T) {
	pts := durationTo90k(1001 * time.Millisecond)
	if pts != 90090 {
		t.Fatalf("expected 90090, got %d", pts)
	}

	back := ticks90kToDuration(pts)
	if back <= 0 {
		t.Fatalf("expected positive duration from ticks, got %v", back)
	}
}

func TestHLSFileDestination_PlaylistTargetDurationReflectsSegments(t *testing.T) {
	outDir := t.TempDir()
	outFolderRaw := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir},
	}).RecordingsRoot()
	outFolder, err := shared.AdaptFolder(outFolderRaw)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}
	dest := &hlsLive{
		url:            outDir,
		outputFolder:   outFolder,
		targetDuration: 2,
		pathPrefix:     "/v1/restream/hls/channel-a/program-a",
		entries: []hlsSegmentEntry{
			{Seq: 5, FileName: "seg_000005.ts", Duration: 16.391},
			{Seq: 6, FileName: "seg_000006.ts", Duration: 15.248},
		},
	}

	if err := dest.writePlaylistLocked(false); err != nil {
		t.Fatalf("writePlaylistLocked failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "stream.m3u8"))
	if err != nil {
		t.Fatalf("failed reading playlist: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	target := -1
	for _, line := range lines {
		if strings.HasPrefix(line, "#EXT-X-TARGETDURATION:") {
			n, parseErr := strconv.Atoi(strings.TrimPrefix(line, "#EXT-X-TARGETDURATION:"))
			if parseErr != nil {
				t.Fatalf("invalid target duration line %q: %v", line, parseErr)
			}
			target = n
			break
		}
	}

	if target != 17 {
		t.Fatalf("expected target duration 17, got %d", target)
	}

	if strings.Contains(string(data), "#EXT-X-DISCONTINUITY") {
		t.Fatalf("contiguous segments (seq 5,6) must NOT have #EXT-X-DISCONTINUITY")
	}
	wantFirst := "/v1/restream/hls/channel-a/program-a/seg_000005.ts"
	wantSecond := "/v1/restream/hls/channel-a/program-a/seg_000006.ts"
	if !strings.Contains(string(data), wantFirst) {
		t.Fatalf("expected storage-backed segment URI %q, got: %s", wantFirst, string(data))
	}
	if !strings.Contains(string(data), wantSecond) {
		t.Fatalf("expected storage-backed segment URI %q, got: %s", wantSecond, string(data))
	}
}

func TestWritePlaylistLocked_DiscontinuityOnlyOnGap(t *testing.T) {
	outDir := t.TempDir()
	outFolderRaw := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir},
	}).RecordingsRoot()
	outFolder, err := shared.AdaptFolder(outFolderRaw)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}

	contiguous := &hlsLive{
		url:            outDir,
		outputFolder:   outFolder,
		targetDuration: 2,
		entries: []hlsSegmentEntry{
			{Seq: 3, FileName: "seg_000003.ts", Duration: 2.0},
			{Seq: 4, FileName: "seg_000004.ts", Duration: 2.0},
			{Seq: 5, FileName: "seg_000005.ts", Duration: 2.0},
		},
	}
	if err := contiguous.writePlaylistLocked(false); err != nil {
		t.Fatalf("writePlaylistLocked: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(outDir, "stream.m3u8"))
	if strings.Contains(string(data), "#EXT-X-DISCONTINUITY") {
		t.Fatal("contiguous segments must not have #EXT-X-DISCONTINUITY")
	}

	outDir2 := t.TempDir()
	outFolderRaw2 := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir2},
	}).RecordingsRoot()
	outFolder2, err := shared.AdaptFolder(outFolderRaw2)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}
	gapped := &hlsLive{
		url:            outDir2,
		outputFolder:   outFolder2,
		targetDuration: 2,
		entries: []hlsSegmentEntry{
			{Seq: 3, FileName: "seg_000003.ts", Duration: 2.0},
			{Seq: 5, FileName: "seg_000005.ts", Duration: 2.0},
		},
	}
	if err := gapped.writePlaylistLocked(false); err != nil {
		t.Fatalf("writePlaylistLocked: %v", err)
	}
	data2, _ := os.ReadFile(filepath.Join(outDir2, "stream.m3u8"))
	if !strings.Contains(string(data2), "#EXT-X-DISCONTINUITY") {
		t.Fatal("gapped segments (seq 3,5) must have #EXT-X-DISCONTINUITY")
	}
}

func TestHLSFileDestination_EmitsAbsoluteURLsInSegmentAndPlaylistEvents(t *testing.T) {
	outDir := t.TempDir()
	outFolderRaw := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: outDir},
	}).RecordingsRoot()
	outFolder, err := shared.AdaptFolder(outFolderRaw)
	if err != nil {
		t.Fatalf("AdaptFolder failed: %v", err)
	}
	dest := &hlsLive{
		id:              "hls-event-url-test",
		outputFolder:    outFolder,
		segmentDuration: time.Second,
		playlistSize:    3,
		targetDuration:  2,
		pathPrefix:      "https://live-play.tupic.com/v1/restream/hls/channel-a/program-a",
		events:          shared.NewEventEmitter(32),
	}

	if err := dest.openSegmentLocked(0); err != nil {
		t.Fatalf("openSegmentLocked failed: %v", err)
	}
	if err := dest.closeCurrentSegmentLocked(false); err != nil {
		t.Fatalf("closeCurrentSegmentLocked failed: %v", err)
	}

	var segMeta *shared.SegmentGeneratedMeta
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-dest.EventChan():
			switch ev.Type {
			case shared.EventTypeSegmentGenerated:
				meta, ok := ev.Meta.(shared.SegmentGeneratedMeta)
				if ok {
					segMeta = &meta
				}
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
		if segMeta != nil {
			break
		}
	}

	if segMeta == nil {
		t.Fatalf("missing segment_generated event meta")
	}
	if !strings.HasPrefix(segMeta.SegmentURL, "https://live-play.tupic.com/v1/restream/hls/channel-a/program-a/") {
		t.Fatalf("expected fully qualified segment URL, got %q", segMeta.SegmentURL)
	}
	if segMeta.PlaylistURL != "https://live-play.tupic.com/v1/restream/hls/channel-a/program-a/stream.m3u8" {
		t.Fatalf("expected fully qualified playlist URL, got %q", segMeta.PlaylistURL)
	}
}

func requireBinary(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Fatalf("%s not available", name)
	}
}

func requireRTMPPublishing(t *testing.T, rtmpURL string, timeout time.Duration) {
	t.Helper()
	requireBinary(t, "ffprobe")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-i", rtmpURL, "-show_streams")
	if err := cmd.Run(); err != nil {
		t.Fatalf("RTMP not publishing or not reachable: %s (%v)", rtmpURL, err)
	}
}

func getConfiguredRTMPURL(t *testing.T) string {
	t.Helper()

	if url := os.Getenv("RTMP_URL"); url != "" {
		return url
	}

	cfg, err := getTestConfig(t)
	if err != nil || cfg == nil {
		t.Fatalf("failed to load test config for RTMP URL: %v", err)
	}

	rtmpURL := strings.TrimSpace(cfg.TestURLs.RTMPURL)
	if rtmpURL == "" {
		t.Fatalf("test_urls.rtmp_url is empty")
	}
	return rtmpURL
}

func getTestConfig(t *testing.T) (*config.Config, error) {
	t.Helper()

	testConfigOnce.Do(func() {
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		viper.AutomaticEnv()

		viper.SetConfigName("default")
		viper.SetConfigType("json")
		if testdataDir := findTestdataDirForTests(); testdataDir != "" {
			rootDir := filepath.Dir(testdataDir)
			viper.AddConfigPath(filepath.Join(rootDir, "configs"))
		}

		if err := viper.ReadInConfig(); err != nil {
			testConfigErr = err
			return
		}

		testConfig = &config.Config{
			TestURLs: config.TestURLs{
				RTMPURL: viper.GetString("test_urls.rtmp_url"),
			},
		}
	})

	return testConfig, testConfigErr
}

var (
	testConfig    *config.Config
	testConfigErr error
)

func findTestdataDirForTests() string {
	// Try to find testdata directory by walking up from current directory
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	for {
		testdataPath := filepath.Join(dir, "testdata")
		if _, err := os.Stat(testdataPath); err == nil {
			return testdataPath
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	// Try common relative paths
	testPaths := []string{
		"testdata",
		"../../testdata",
		"../testdata",
		"./testdata",
	}

	for _, path := range testPaths {
		if absPath, err := filepath.Abs(path); err == nil {
			if stat, err := os.Stat(absPath); err == nil && stat.IsDir() {
				return absPath
			}
		}
	}

	return ""
}

// isRTMPURLAvailable checks if an RTMP URL is available by attempting to connect
func isRTMPURLAvailable(rtmpURL string) bool {
	u, err := url.Parse(addDefaultRTMPPort(rtmpURL))
	if err != nil {
		return false
	}

	c := &gortmplib.Client{
		URL:     u,
		Publish: false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = c.Initialize(ctx)
	if err == nil {
		c.Close()
		return true
	}
	return false
}
