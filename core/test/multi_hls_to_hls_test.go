package test

import (
	"context"
	"fmt"
	core "github.com/tupicapp/restreamer/core"
	streaminputs "github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
	testtools "github.com/tupicapp/restreamer/core/test_tools"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
)

// TestMultiHLSToHLS_WindowSwitchesMatchReference verifies deterministic window switching:
// 0-5s from input1, 5-10s from input2, 10-15s from input3.
func TestMultiHLSToHLS_WindowSwitchesMatchReference(t *testing.T) {
	requireBinary(t, "ffmpeg")
	sourcePlaylist := resolveTestFixturePath(testHLSFixtureRelativePath)
	if _, err := os.Stat(sourcePlaylist); err != nil {
		t.Fatalf("source playlist not found: %v", err)
	}

	workDir := t.TempDir()
	input1Dir := filepath.Join(workDir, "input1")
	input2Dir := filepath.Join(workDir, "input2")
	input3Dir := filepath.Join(workDir, "input3")
	refDir := filepath.Join(workDir, "reference")
	outDir := filepath.Join(workDir, "output")
	for _, dir := range []string{input1Dir, input2Dir, input3Dir, refDir, outDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	makeHLSFixture(t, sourcePlaylist, 0, 15*time.Second, input1Dir)
	makeHLSFixture(t, sourcePlaylist, 5*time.Second, 15*time.Second, input2Dir)
	makeHLSFixture(t, sourcePlaylist, 10*time.Second, 15*time.Second, input3Dir)
	makeReferenceWindowHLS(t,
		filepath.Join(input1Dir, "stream.m3u8"),
		filepath.Join(input2Dir, "stream.m3u8"),
		filepath.Join(input3Dir, "stream.m3u8"),
		refDir,
	)

	input1URL := filepath.Join(input1Dir, "stream.m3u8")
	input2URL := filepath.Join(input2Dir, "stream.m3u8")
	input3URL := filepath.Join(input3Dir, "stream.m3u8")

	runStreamerWindowSwitchStreams(t, outDir,
		[]core.Stream{
			streaminputs.NewHLS("hls-input-1", input1URL, streaminputs.OptionWithRealTime(true)),
			streaminputs.NewHLS("hls-input-2", input2URL, streaminputs.OptionWithRealTime(true)),
			streaminputs.NewHLS("hls-input-3", input3URL, streaminputs.OptionWithRealTime(true)),
		},
		[]string{"hls-input-1", "hls-input-2", "hls-input-3"},
		5*time.Second,
	)

	outputURL := filepath.Join(outDir, "stream.m3u8")
	outVideo, outAudio := collectHLSFrames(t, outputURL, "out")
	in1Video, in1Audio := collectHLSFrames(t, input1URL, "in1")
	in2Video, in2Audio := collectHLSFrames(t, input2URL, "in2")
	in3Video, in3Audio := collectHLSFrames(t, input3URL, "in3")

	assertWindowMatches(t, "window1-video", sliceFramesByPTS(outVideo, 0, 5*time.Second), sliceFramesByPTS(in1Video, 0, 5*time.Second))
	assertWindowMatches(t, "window2-video", sliceFramesByPTS(outVideo, 5*time.Second, 10*time.Second), sliceFramesByPTS(in2Video, 0, 5*time.Second))
	assertWindowMatches(t, "window3-video", sliceFramesByPTS(outVideo, 10*time.Second, 15*time.Second), sliceFramesByPTS(in3Video, 0, 5*time.Second))

	assertWindowMatches(t, "window1-audio", sliceFramesByPTS(outAudio, 0, 5*time.Second), sliceFramesByPTS(in1Audio, 0, 5*time.Second))
	assertWindowMatches(t, "window2-audio", sliceFramesByPTS(outAudio, 5*time.Second, 10*time.Second), sliceFramesByPTS(in2Audio, 0, 5*time.Second))
	assertWindowMatches(t, "window3-audio", sliceFramesByPTS(outAudio, 10*time.Second, 15*time.Second), sliceFramesByPTS(in3Audio, 0, 5*time.Second))
}

// TestMultiHLSToHLS_MixedFileAndLiveWindowSwitchesMatchReference verifies a mixed setup:
// first 5s from a frozen live snapshot through NewHLSLive, then 5s from file input 1,
// then 5s from file input 2.
func TestMultiHLSToHLS_MixedFileAndLiveWindowSwitchesMatchReference(t *testing.T) {
	requireBinary(t, "ffmpeg")

	sourcePlaylist := resolveTestFixturePath(testHLSFixtureRelativePath)
	if _, err := os.Stat(sourcePlaylist); err != nil {
		t.Fatalf("source playlist not found: %v", err)
	}

	workDir := t.TempDir()
	input1Dir := filepath.Join(workDir, "input1")
	input2Dir := filepath.Join(workDir, "input2")
	outDir := filepath.Join(workDir, "output")
	for _, dir := range []string{input1Dir, input2Dir, outDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	liveInputURL, liveFixturePath, cleanup := setupDeterministicLiveFixtureServer(t, 15*time.Second)
	defer cleanup()

	makeHLSFixture(t, sourcePlaylist, 0, 15*time.Second, input1Dir)
	makeHLSFixture(t, sourcePlaylist, 5*time.Second, 15*time.Second, input2Dir)

	input1URL := filepath.Join(input1Dir, "stream.m3u8")
	input2URL := filepath.Join(input2Dir, "stream.m3u8")
	outputURL := filepath.Join(outDir, "stream.m3u8")

	runMixedLiveFileWindowSwitch(t, outDir, liveInputURL, input1URL, input2URL)

	outVideo, outAudio := collectHLSFrames(t, outputURL, "out-mixed")
	liveVideo, liveAudio := collectHLSFrames(t, liveFixturePath, "live-ref")
	in1Video, in1Audio := collectHLSFrames(t, input1URL, "file1-ref")
	in2Video, in2Audio := collectHLSFrames(t, input2URL, "file2-ref")

	assertWindowMatches(t, "mixed-window1-video", sliceFramesByPTS(outVideo, 0, 5*time.Second), sliceFramesByPTS(liveVideo, 0, 5*time.Second))
	assertWindowMatches(t, "mixed-window2-video", sliceFramesByPTS(outVideo, 5*time.Second, 10*time.Second), sliceFramesByPTS(in1Video, 0, 5*time.Second))
	assertWindowMatches(t, "mixed-window3-video", sliceFramesByPTS(outVideo, 10*time.Second, 15*time.Second), sliceFramesByPTS(in2Video, 0, 5*time.Second))

	assertWindowMatches(t, "mixed-window1-audio", sliceFramesByPTS(outAudio, 0, 5*time.Second), sliceFramesByPTS(liveAudio, 0, 5*time.Second))
	assertWindowMatches(t, "mixed-window2-audio", sliceFramesByPTS(outAudio, 5*time.Second, 10*time.Second), sliceFramesByPTS(in1Audio, 0, 5*time.Second))
	assertWindowMatches(t, "mixed-window3-audio", sliceFramesByPTS(outAudio, 10*time.Second, 15*time.Second), sliceFramesByPTS(in2Audio, 0, 5*time.Second))
}

func makeHLSFixture(t *testing.T, sourcePlaylist string, start, duration time.Duration, outDir string) {
	t.Helper()

	segmentTemplate := filepath.Join(outDir, "seg_%06d.ts")
	playlistPath := filepath.Join(outDir, "stream.m3u8")

	cmd := exec.Command(
		"ffmpeg",
		"-hide_banner",
		"-v", "error",
		"-y",
		"-ss", fmt.Sprintf("%.3f", start.Seconds()),
		"-i", sourcePlaylist,
		"-t", fmt.Sprintf("%.3f", duration.Seconds()),
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-tune", "zerolatency",
		"-pix_fmt", "yuv420p",
		"-g", "25",
		"-keyint_min", "25",
		"-sc_threshold", "0",
		"-c:a", "aac",
		"-ar", "48000",
		"-ac", "2",
		"-b:a", "128k",
		"-f", "hls",
		"-hls_time", "1",
		"-hls_list_size", "0",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", segmentTemplate,
		playlistPath,
	)

	runCmdEnsureNoStderrWithTimeout(t, cmd, "ffmpeg build hls fixture", 90*time.Second)
}

func makeReferenceWindowHLS(t *testing.T, input1, input2, input3, outDir string) {
	t.Helper()

	segmentTemplate := filepath.Join(outDir, "seg_%06d.ts")
	playlistPath := filepath.Join(outDir, "stream.m3u8")
	filter := "[0:v][0:a][1:v][1:a][2:v][2:a]concat=n=3:v=1:a=1[v][a]"

	cmd := exec.Command(
		"ffmpeg",
		"-hide_banner",
		"-v", "error",
		"-y",
		"-ss", "0",
		"-t", "5",
		"-i", input1,
		"-ss", "0",
		"-t", "5",
		"-i", input2,
		"-ss", "0",
		"-t", "5",
		"-i", input3,
		"-filter_complex", filter,
		"-map", "[v]",
		"-map", "[a]",
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-tune", "zerolatency",
		"-pix_fmt", "yuv420p",
		"-g", "25",
		"-keyint_min", "25",
		"-sc_threshold", "0",
		"-c:a", "aac",
		"-ar", "48000",
		"-ac", "2",
		"-b:a", "128k",
		"-f", "hls",
		"-hls_time", "1",
		"-hls_list_size", "0",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", segmentTemplate,
		playlistPath,
	)

	runCmdEnsureNoStderrWithTimeout(t, cmd, "ffmpeg build reference hls", 90*time.Second)
}

func runStreamerWindowSwitchStreams(t *testing.T, outDir string, inputs []core.Stream, switchIDs []string, window time.Duration) {
	t.Helper()

	if len(inputs) != len(switchIDs) {
		t.Fatalf("inputs (%d) and switchIDs (%d) length mismatch", len(inputs), len(switchIDs))
	}

	outFolder := storage.NewFolder(outDir)
	hlsDest, err := outputs.NewHLSLiveDestination("hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(1*time.Second),
		outputs.WithHLSPlaylistSize(30),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination: %v", err)
	}

	streamer := core.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	if err := streamer.UpdateStreams(inputs, []core.Stream{hlsDest}); err != nil {
		t.Fatalf("UpdateStreams: %v", err)
	}
	streamer.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := hlsDest.WaitForStart(startCtx); err != nil {
		t.Fatalf("hls dest failed to start: %v", err)
	}

	for i, switchID := range switchIDs {
		if ok := streamer.Switch(switchID); !ok {
			t.Fatalf("switch to %s failed", switchID)
		}
		if err := inputs[i].WaitForStart(startCtx); err != nil {
			t.Fatalf("input %s failed to start after switch: %v", inputs[i].GetID(), err)
		}
		time.Sleep(window)
	}

	time.Sleep(1500 * time.Millisecond)
	streamer.Close()
	waitForHLSArtifacts(t, outDir, 20*time.Second, 5)
}

func runMixedLiveFileWindowSwitch(t *testing.T, outDir, liveInputURL, input1URL, input2URL string) {
	t.Helper()

	outFolder := storage.NewFolder(outDir)
	hlsDest, err := outputs.NewHLSLiveDestination("hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(1*time.Second),
		outputs.WithHLSPlaylistSize(30),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination: %v", err)
	}

	liveInput := streaminputs.NewHLSLive("hls-live-1", liveInputURL)
	fileInput1 := streaminputs.NewHLS("hls-file-1", input1URL, streaminputs.OptionWithRealTime(true))
	fileInput2 := streaminputs.NewHLS("hls-file-2", input2URL, streaminputs.OptionWithRealTime(true))

	streamer := core.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	if err := streamer.UpdateStreams([]core.Stream{liveInput}, []core.Stream{hlsDest}); err != nil {
		t.Fatalf("UpdateStreams(initial live): %v", err)
	}
	streamer.Start()

	startCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := hlsDest.WaitForStart(startCtx); err != nil {
		t.Fatalf("hls dest failed to start: %v", err)
	}

	if ok := streamer.Switch(liveInput.GetID()); !ok {
		t.Fatalf("switch to %s failed", liveInput.GetID())
	}
	if err := liveInput.WaitForStart(startCtx); err != nil {
		t.Fatalf("input %s failed to start after switch: %v", liveInput.GetID(), err)
	}

	time.Sleep(5 * time.Second)

	if err := streamer.UpdateStreams([]core.Stream{liveInput, fileInput1, fileInput2}, []core.Stream{hlsDest}); err != nil {
		t.Fatalf("UpdateStreams(add files): %v", err)
	}

	if ok := streamer.Switch(fileInput1.GetID()); !ok {
		t.Fatalf("switch to %s failed", fileInput1.GetID())
	}
	if err := fileInput1.WaitForStart(startCtx); err != nil {
		t.Fatalf("input %s failed to start after switch: %v", fileInput1.GetID(), err)
	}

	time.Sleep(5 * time.Second)

	if ok := streamer.Switch(fileInput2.GetID()); !ok {
		t.Fatalf("switch to %s failed", fileInput2.GetID())
	}
	if err := fileInput2.WaitForStart(startCtx); err != nil {
		t.Fatalf("input %s failed to start after switch: %v", fileInput2.GetID(), err)
	}

	time.Sleep(5 * time.Second)

	time.Sleep(1500 * time.Millisecond)
	streamer.Close()
	waitForHLSArtifacts(t, outDir, 20*time.Second, 5)
}

func collectHLSFrames(t *testing.T, playlistURL, id string) ([]*Frame, []*Frame) {
	t.Helper()

	input := streaminputs.NewHLS(id, playlistURL)
	buf := outputs.NewBuffering(id + "-buf")
	streamer := core.NewStreamer()
	defer streamer.Close()
	streamer.StartLife()

	if err := streamer.UpdateStreams([]core.Stream{input}, []core.Stream{buf}); err != nil {
		t.Fatalf("collect UpdateStreams(%s): %v", id, err)
	}
	streamer.Start()
	if ok := streamer.Switch(id); !ok {
		t.Fatalf("collect switch(%s) failed", id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := input.WaitForStart(ctx); err != nil {
		t.Fatalf("collect input start(%s): %v", id, err)
	}
	if err := buf.WaitForStart(ctx); err != nil {
		t.Fatalf("collect buffer start(%s): %v", id, err)
	}

	lastFrameTime := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		state := input.State()
		if state != nil && state.LastIO.After(lastFrameTime) {
			lastFrameTime = state.LastIO
		}
		bufState := buf.State()
		if bufState != nil && bufState.LastIO.After(lastFrameTime) {
			lastFrameTime = bufState.LastIO
		}
		if time.Since(lastFrameTime) > 2500*time.Millisecond {
			break
		}
	}

	streamer.Close()
	time.Sleep(300 * time.Millisecond)
	return buf.GetVideoFrames(), buf.GetAudioFrames()
}

func sliceFramesByPTS(frames []*Frame, start, end time.Duration) []*Frame {
	var out []*Frame
	for _, f := range frames {
		if f == nil {
			continue
		}
		if f.PTS < start || f.PTS >= end {
			continue
		}
		clone := *f
		clone.PTS -= start
		clone.DTS -= start
		out = append(out, &clone)
	}
	return out
}

func assertWindowMatches(t *testing.T, label string, got, want []*Frame) {
	t.Helper()

	gotCount := countNonNilFrames(got)
	wantCount := countNonNilFrames(want)
	if wantCount == 0 {
		t.Fatalf("%s: reference window is empty", label)
	}
	if gotCount == 0 {
		t.Fatalf("%s: output window is empty", label)
	}

	gotStart, wantStart, matched := longestPayloadRun(got, want)
	if matched == 0 {
		t.Fatalf("%s: no matching payload subsequence found", label)
	}

	codec := firstCodec(want)
	wantDuration := frameSpan(want)
	gotDuration := frameSpan(got)
	frameCoveragePercent := float64(gotCount) / float64(wantCount) * 100
	durationCoveragePercent := 100.0
	if wantDuration > 0 {
		durationCoveragePercent = float64(gotDuration) / float64(wantDuration) * 100
	}

	payloadMatchPercent := float64(matched) / float64(wantCount) * 100
	minPayloadMatchPercent := 95.0
	minFrameCoveragePercent := 95.0
	minDurationCoveragePercent := 95.0
	ptsTolerance := 2 * time.Millisecond
	if codec == "aac" {
		minPayloadMatchPercent = 30.0
		minFrameCoveragePercent = 75.0
		minDurationCoveragePercent = 80.0
		ptsTolerance = 15 * time.Millisecond
	}
	if payloadMatchPercent < minPayloadMatchPercent {
		t.Fatalf("%s: payload match %.2f%% < %.2f%%", label, payloadMatchPercent, minPayloadMatchPercent)
	}
	if frameCoveragePercent < minFrameCoveragePercent {
		t.Fatalf("%s: frame coverage %.2f%% < %.2f%%", label, frameCoveragePercent, minFrameCoveragePercent)
	}
	if durationCoveragePercent < minDurationCoveragePercent {
		t.Fatalf("%s: duration coverage %.2f%% < %.2f%%", label, durationCoveragePercent, minDurationCoveragePercent)
	}

	gotBase := got[gotStart].PTS
	wantBase := want[wantStart].PTS
	ptsMatched := 0
	for i := 0; i < matched; i++ {
		gotDelta := got[gotStart+i].PTS - gotBase
		wantDelta := want[wantStart+i].PTS - wantBase
		diff := gotDelta - wantDelta
		if diff < 0 {
			diff = -diff
		}
		if diff <= ptsTolerance {
			ptsMatched++
		}
	}

	ptsMatchPercent := float64(ptsMatched) / float64(matched) * 100
	if ptsMatchPercent < 95 {
		t.Fatalf("%s: relative pts match %.2f%% < 95%%", label, ptsMatchPercent)
	}

	t.Logf("%s: matched %d/%d frames starting at output index %d (payload %.2f%%, frame coverage %.2f%%, duration coverage %.2f%%, relative pts %.2f%%)",
		label, matched, wantCount, gotStart, payloadMatchPercent, frameCoveragePercent, durationCoveragePercent, ptsMatchPercent)
}

func countNonNilFrames(frames []*Frame) int {
	count := 0
	for _, frame := range frames {
		if frame != nil {
			count++
		}
	}
	return count
}

func firstCodec(frames []*Frame) string {
	for _, frame := range frames {
		if frame != nil {
			return frame.Codec
		}
	}
	return ""
}

func frameSpan(frames []*Frame) time.Duration {
	first := -1
	last := -1
	for i, frame := range frames {
		if frame == nil {
			continue
		}
		if first == -1 {
			first = i
		}
		last = i
	}
	if first == -1 || last == -1 || first == last {
		return 0
	}
	return frames[last].PTS - frames[first].PTS
}

func longestPayloadRun(got, want []*Frame) (bestGotStart int, bestWantStart int, bestLen int) {
	if len(got) == 0 || len(want) == 0 {
		return 0, 0, 0
	}

	for gotStart := range got {
		if got[gotStart] == nil {
			continue
		}
		gotHash := testtools.FrameHash(got[gotStart])
		for wantStart := range want {
			if want[wantStart] == nil || testtools.FrameHash(want[wantStart]) != gotHash {
				continue
			}

			matched := 0
			for i := 0; gotStart+i < len(got) && wantStart+i < len(want); i++ {
				if got[gotStart+i] == nil || want[wantStart+i] == nil {
					break
				}
				if testtools.FrameHash(got[gotStart+i]) != testtools.FrameHash(want[wantStart+i]) {
					break
				}
				matched++
			}

			if matched > bestLen {
				bestGotStart = gotStart
				bestWantStart = wantStart
				bestLen = matched
			}
		}
	}

	return bestGotStart, bestWantStart, bestLen
}

func snapshotLiveHLSFixture(t *testing.T, sourceURL string, minDuration time.Duration, outDir string) string {
	t.Helper()

	media, mediaURL := resolveMediaPlaylistForSnapshot(t, sourceURL)
	selectedSegments := selectSegmentsForDuration(t, media.Segments, minDuration)
	logSnapshotSegments(t, sourceURL, selectedSegments)

	snapshot := *media
	snapshot.MediaSequence = 0
	snapshot.PlaylistType = playlistTypePtr(playlist.MediaPlaylistTypeVOD)
	snapshot.Endlist = true
	snapshot.ServerControl = nil
	snapshot.PartInf = nil
	snapshot.Parts = nil
	snapshot.PreloadHint = nil
	snapshot.RenditionReport = nil
	snapshot.Skip = nil
	snapshot.Start = nil
	snapshot.Segments = nil

	if media.Map != nil {
		clonedMap := *media.Map
		mapURL := resolveSnapshotURL(mediaURL, media.Map.URI)
		mapName := "init" + filepath.Ext(urlPathForName(mapURL))
		if filepath.Ext(mapName) == "." || filepath.Ext(mapName) == "" {
			mapName = "init.mp4"
		}
		writeSnapshotAsset(t, outDir, mapName, mapURL)
		clonedMap.URI = mapName
		snapshot.Map = &clonedMap
	}

	for i, segment := range selectedSegments {
		cloned := *segment
		segmentURL := resolveSnapshotURL(mediaURL, segment.URI)
		segmentName := fmt.Sprintf("seg_%06d%s", i, extOrDefault(urlPathForName(segmentURL), ".ts"))
		writeSnapshotAsset(t, outDir, segmentName, segmentURL)
		cloned.URI = segmentName
		cloned.Parts = nil
		snapshot.Segments = append(snapshot.Segments, &cloned)
	}

	playlistBytes, err := snapshot.Marshal()
	if err != nil {
		t.Fatalf("marshal live snapshot playlist: %v", err)
	}

	playlistPath := filepath.Join(outDir, "stream.m3u8")
	if err := os.WriteFile(playlistPath, playlistBytes, 0o644); err != nil {
		t.Fatalf("write live snapshot playlist: %v", err)
	}

	return playlistPath
}

func resolveMediaPlaylistForSnapshot(t *testing.T, sourceURL string) (*playlist.Media, string) {
	t.Helper()

	body := mustFetchURL(t, sourceURL)
	pl, err := playlist.Unmarshal(body)
	if err != nil {
		t.Fatalf("unmarshal live playlist %s: %v", sourceURL, err)
	}

	switch typed := pl.(type) {
	case *playlist.Media:
		return typed, sourceURL
	case *playlist.Multivariant:
		if len(typed.Variants) == 0 {
			t.Fatalf("multivariant playlist %s has no variants", sourceURL)
		}
		best := typed.Variants[0]
		for _, variant := range typed.Variants[1:] {
			if variant.Bandwidth < best.Bandwidth {
				best = variant
			}
		}
		return resolveMediaPlaylistForSnapshot(t, resolveSnapshotURL(sourceURL, best.URI))
	default:
		t.Fatalf("unsupported playlist type %T for %s", pl, sourceURL)
		return nil, ""
	}
}

func selectSegmentsForDuration(t *testing.T, segments []*playlist.MediaSegment, minDuration time.Duration) []*playlist.MediaSegment {
	t.Helper()

	var out []*playlist.MediaSegment
	var total time.Duration
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		out = append(out, segment)
		total += segment.Duration
		if total >= minDuration {
			break
		}
	}

	if total < minDuration {
		t.Fatalf("live snapshot segments total %v < required %v", total, minDuration)
	}

	return out
}

func logSnapshotSegments(t *testing.T, sourceURL string, segments []*playlist.MediaSegment) {
	t.Helper()

	total := time.Duration(0)
	uris := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		total += segment.Duration
		uris = append(uris, segment.URI)
	}

	t.Logf("live snapshot source: %s", sourceURL)
	t.Logf("live snapshot selected %d segments covering %v", len(uris), total)
	for i, uri := range uris {
		t.Logf("live snapshot segment[%d]: %s", i, uri)
	}
}

func writeSnapshotAsset(t *testing.T, outDir, fileName, sourceURL string) {
	t.Helper()

	data := mustFetchURL(t, sourceURL)
	if err := os.WriteFile(filepath.Join(outDir, fileName), data, 0o644); err != nil {
		t.Fatalf("write snapshot asset %s: %v", fileName, err)
	}
}

func mustFetchURL(t *testing.T, rawURL string) []byte {
	t.Helper()

	resp, err := http.Get(rawURL) //nolint:gosec
	if err != nil {
		t.Fatalf("fetch %s: %v", rawURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		t.Fatalf("fetch %s: status %d", rawURL, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", rawURL, err)
	}
	return data
}

func resolveSnapshotURL(baseURL, relativeURI string) string {
	if relativeURI == "" {
		return baseURL
	}
	ref, err := url.Parse(relativeURI)
	if err == nil && ref.IsAbs() {
		return ref.String()
	}

	base, err := url.Parse(baseURL)
	if err != nil {
		return relativeURI
	}

	return base.ResolveReference(ref).String()
}

func extOrDefault(pathValue, fallback string) string {
	ext := filepath.Ext(pathValue)
	if ext == "" {
		return fallback
	}
	return ext
}

func urlPathForName(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return parsed.Path
}

func playlistTypePtr(v playlist.MediaPlaylistType) *playlist.MediaPlaylistType {
	return &v
}

func setupDeterministicLiveFixtureServer(t *testing.T, duration time.Duration) (string, string, func()) {
	t.Helper()

	sourcePlaylist := resolveTestFixturePath(testHLSFixtureRelativePath)
	if _, err := os.Stat(sourcePlaylist); err != nil {
		t.Fatalf("source playlist not found: %v", err)
	}

	workDir := t.TempDir()
	liveDir := filepath.Join(workDir, "live")
	if err := os.MkdirAll(liveDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", liveDir, err)
	}

	makeHLSFixture(t, sourcePlaylist, 0, duration, liveDir)

	fileServer := httptest.NewServer(http.FileServer(http.Dir(workDir)))
	cleanup := func() {
		fileServer.Close()
	}

	return fileServer.URL + "/live/stream.m3u8", filepath.Join(liveDir, "stream.m3u8"), cleanup
}
