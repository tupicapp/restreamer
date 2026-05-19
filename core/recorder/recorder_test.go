package recorder

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tupicapp/restreamer/core/config"
	shared "github.com/tupicapp/restreamer/core/shared"
	"github.com/tupicapp/restreamer/core/storage"
)

func TestRecorder_WiresGOPBufferAndSessionNaming(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	rec, err := New("record_channel-a_program-a", root, withNowFunc(func() time.Time {
		return time.Unix(1712345678, 0)
	}))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer rec.Close()

	if !rec.gopBuffer.IsRebase() {
		t.Fatalf("expected recorder GOP buffer to enable timeline rebasing")
	}
	if rec.GetVideoChan() != rec.gopBuffer.VideoFrameChan {
		t.Fatalf("video channel is not wired to GOP buffer")
	}
	if rec.GetAudioChan() != rec.gopBuffer.AudioFrameChan {
		t.Fatalf("audio channel is not wired to GOP buffer")
	}

	rec.Start()
	if rec.sessionID != "record_channel-a_program-a_1712345678" {
		t.Fatalf("unexpected session id: %s", rec.sessionID)
	}
	wantURL := "file://" + filepath.ToSlash(filepath.Join(outDir, "record_channel-a_program-a_1712345678", "stream.m3u8"))
	if got := rec.State().Url; got != wantURL {
		t.Fatalf("unexpected state url: %s", got)
	}

	rec.handleAudioFrame(&shared.Frame{
		InputID:    "input-a",
		Codec:      "aac",
		Payload:    [][]byte{{0x11, 0x22, 0x33}},
		IsKeyFrame: true,
		PTS:        100 * time.Millisecond,
		DTS:        100 * time.Millisecond,
		SequenceID: 1,
	})

	sessionDir := filepath.Join(outDir, "record_channel-a_program-a_1712345678")
	playlistPath := filepath.Join(sessionDir, "stream.m3u8")
	if _, statErr := os.Stat(playlistPath); !os.IsNotExist(statErr) {
		t.Fatalf("playlist should not exist before first video frame, statErr=%v", statErr)
	}
	if rec.DroppedAudioFrames != 1 {
		t.Fatalf("expected audio-only start to be dropped, got %v", rec.DroppedAudioFrames)
	}

	rec.handleVideoFrame(&shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        2 * time.Second,
		DTS:        2 * time.Second,
		SequenceID: 2,
	})

	if _, err := os.Stat(playlistPath); err != nil {
		t.Fatalf("expected playlist after first keyframe: %v", err)
	}
}

func TestRecorder_WritesCompletePlaylistAndKeepsAllSegments(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	rec, err := New(
		"record_channel-a_program-a",
		root,
		WithSegmentDuration(time.Second),
		withNowFunc(func() time.Time { return time.Unix(1712345679, 0) }),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer rec.Close()

	rec.Start()

	rec.handleVideoFrame(videoFrame(1, 0, true))
	rec.handleVideoFrame(videoFrame(2, 500*time.Millisecond, false))
	rec.handleVideoFrame(videoFrame(3, 1200*time.Millisecond, true))
	rec.handleVideoFrame(videoFrame(4, 1500*time.Millisecond, false))

	rec.Stop()

	sessionDir := filepath.Join(outDir, "record_channel-a_program-a_1712345679")
	playlistPath := filepath.Join(sessionDir, "stream.m3u8")

	data, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	playlist := string(data)

	if !strings.Contains(playlist, "#EXT-X-ENDLIST") {
		t.Fatalf("playlist must be finalized on stop: %s", playlist)
	}
	if strings.Count(playlist, "#EXTINF:") != 2 {
		t.Fatalf("expected full playlist with two segments, got: %s", playlist)
	}
	wantFirst := "file://" + filepath.ToSlash(filepath.Join(sessionDir, "seg_000000.ts"))
	wantSecond := "file://" + filepath.ToSlash(filepath.Join(sessionDir, "seg_000001.ts"))
	if !strings.Contains(playlist, wantFirst) || !strings.Contains(playlist, wantSecond) {
		t.Fatalf("playlist should contain all written segments: %s", playlist)
	}

	for _, name := range []string{"seg_000000.ts", "seg_000001.ts"} {
		if _, err := os.Stat(filepath.Join(sessionDir, name)); err != nil {
			t.Fatalf("expected segment %s to remain on disk: %v", name, err)
		}
	}
}

func TestRecorder_WritesVODPlaylistWhileSessionIsActive(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	rec, err := New(
		"record_channel-a_program-a",
		root,
		WithSegmentDuration(time.Second),
		withNowFunc(func() time.Time { return time.Unix(1712345679, 0) }),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer rec.Close()

	rec.Start()
	rec.handleVideoFrame(videoFrame(1, 0, true))

	playlistPath := filepath.Join(outDir, "record_channel-a_program-a_1712345679", "stream.m3u8")
	data, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	playlist := string(data)
	if !strings.Contains(playlist, "#EXT-X-PLAYLIST-TYPE:VOD") {
		t.Fatalf("expected active playlist to be marked as VOD: %s", playlist)
	}
	if !strings.Contains(playlist, "#EXT-X-ENDLIST") {
		t.Fatalf("active playlist must contain ENDLIST for VOD playback: %s", playlist)
	}
}

func TestRecorder_StartAfterStopCreatesNewSessionFolder(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	var (
		mu    sync.Mutex
		times = []time.Time{time.Unix(1712345680, 0), time.Unix(1712345690, 0)}
		idx   int
	)

	rec, err := New("record_channel-a_program-a", root, withNowFunc(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		if idx >= len(times) {
			return times[len(times)-1]
		}
		v := times[idx]
		idx++
		return v
	}))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer rec.Close()

	rec.Start()
	rec.handleVideoFrame(videoFrame(1, 0, true))
	rec.Stop()

	rec.Start()
	rec.handleVideoFrame(videoFrame(2, 0, true))
	rec.Stop()

	firstDir := filepath.Join(outDir, "record_channel-a_program-a_1712345680")
	secondDir := filepath.Join(outDir, "record_channel-a_program-a_1712345690")

	for _, playlistPath := range []string{
		filepath.Join(firstDir, "stream.m3u8"),
		filepath.Join(secondDir, "stream.m3u8"),
	} {
		if _, err := os.Stat(playlistPath); err != nil {
			t.Fatalf("expected playlist %s: %v", playlistPath, err)
		}
	}

	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected two preserved recording folders, got %d", len(entries))
	}
}

func TestRecorder_EmitsRecordStartedOnEverySessionStart(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	var (
		mu    sync.Mutex
		times = []time.Time{time.Unix(1712345700, 0), time.Unix(1712345710, 0)}
		idx   int
	)
	rec, err := New("record_channel-a_program-a", root, withNowFunc(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		if idx >= len(times) {
			return times[len(times)-1]
		}
		v := times[idx]
		idx++
		return v
	}))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer rec.Close()

	rec.Start()
	first := requireRecordStartedEvent(t, rec.EventChan())
	if first.SessionID != "record_channel-a_program-a_1712345700" {
		t.Fatalf("unexpected first session id: %s", first.SessionID)
	}
	if !strings.HasPrefix(first.PlaylistURL, "file://") {
		t.Fatalf("expected fully qualified playlist URL, got %q", first.PlaylistURL)
	}
	if first.SegmentCount != 0 {
		t.Fatalf("expected no segments at session start, got %d", first.SegmentCount)
	}
	if len(first.SegmentURLs) != 0 {
		t.Fatalf("expected no segment URLs at session start, got %#v", first.SegmentURLs)
	}

	rec.handleVideoFrame(videoFrame(1, 0, true))
	rec.handleVideoFrame(videoFrame(2, 1200*time.Millisecond, true))
	rec.Stop()

	rec.Start()

	second := requireRecordStartedEvent(t, rec.EventChan())
	if second.SessionID != "record_channel-a_program-a_1712345710" {
		t.Fatalf("unexpected restarted session id: %s", second.SessionID)
	}
	if !strings.HasPrefix(second.PlaylistURL, "file://") {
		t.Fatalf("expected fully qualified playlist URL, got %q", second.PlaylistURL)
	}
	if second.SegmentCount != 0 {
		t.Fatalf("expected no segments at restarted session start, got %d", second.SegmentCount)
	}
	if len(second.SegmentURLs) != 0 {
		t.Fatalf("expected no segment URLs at restarted session start, got %#v", second.SegmentURLs)
	}
}

func TestRecorder_UsesConfiguredPathPrefixForPlaylistAndSegments(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	rec, err := New(
		"record_channel-a_program-a",
		root,
		WithSegmentDuration(time.Second),
		WithPathPrefix("https://live-play.tupic.com/v1/restream/records/channel-a/program-a"),
		withNowFunc(func() time.Time { return time.Unix(1712345679, 0) }),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer rec.Close()

	rec.Start()
	rec.handleVideoFrame(videoFrame(1, 0, true))
	rec.handleVideoFrame(videoFrame(2, 1200*time.Millisecond, true))
	rec.Stop()

	sessionID := "record_channel-a_program-a_1712345679"
	sessionDir := filepath.Join(outDir, sessionID)
	playlistPath := filepath.Join(sessionDir, "stream.m3u8")

	data, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	playlist := string(data)

	wantFirst := "https://live-play.tupic.com/v1/restream/records/channel-a/program-a/" + sessionID + "/seg_000000.ts"
	wantSecond := "https://live-play.tupic.com/v1/restream/records/channel-a/program-a/" + sessionID + "/seg_000001.ts"
	if !strings.Contains(playlist, wantFirst) || !strings.Contains(playlist, wantSecond) {
		t.Fatalf("playlist should contain configured public URLs: %s", playlist)
	}
	if got := rec.State().Url; got != "https://live-play.tupic.com/v1/restream/records/channel-a/program-a/"+sessionID+"/stream.m3u8" {
		t.Fatalf("unexpected state url: %s", got)
	}
}

func TestRecorder_CloseDrainsBufferedFramesBeforeFinalizing(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	rec, err := New(
		"record_channel-a_program-a",
		root,
		WithSegmentDuration(time.Second),
		withNowFunc(func() time.Time { return time.Unix(1712345720, 0) }),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	rec.Start()
	rec.GetVideoChan() <- videoFrame(1, 0, true)
	rec.GetVideoChan() <- videoFrame(2, 1200*time.Millisecond, true)
	rec.Close()

	sessionDir := filepath.Join(outDir, "record_channel-a_program-a_1712345720")
	playlistPath := filepath.Join(sessionDir, "stream.m3u8")

	data, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	playlist := string(data)

	if !strings.Contains(playlist, "#EXT-X-ENDLIST") {
		t.Fatalf("playlist must be finalized on close: %s", playlist)
	}
	if strings.Count(playlist, "#EXTINF:") != 2 {
		t.Fatalf("expected buffered frames to produce two finalized segments, got: %s", playlist)
	}
}

func TestRecorder_StartFinalizesStaleSessionPlaylist(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "recordings", "channel-a")
	root := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))

	staleDir := filepath.Join(outDir, "record_channel-a_program-a_1712345600")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	stalePlaylist := strings.Join([]string{
		"#EXTM3U",
		"#EXT-X-VERSION:3",
		"#EXT-X-PLAYLIST-TYPE:EVENT",
		"#EXT-X-TARGETDURATION:2",
		"#EXTINF:1.000,",
		"seg_000000.ts",
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(staleDir, "stream.m3u8"), []byte(stalePlaylist), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	rec, err := New(
		"record_channel-a_program-a",
		root,
		withNowFunc(func() time.Time { return time.Unix(1712345725, 0) }),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer rec.Close()

	rec.Start()

	data, err := os.ReadFile(filepath.Join(staleDir, "stream.m3u8"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !strings.Contains(string(data), "#EXT-X-ENDLIST") {
		t.Fatalf("expected stale playlist to be finalized, got: %s", string(data))
	}
}

func videoFrame(sequence int64, pts time.Duration, key bool) *shared.Frame {
	payload := [][]byte{{0x41, 0x9a, 0x22}}
	if key {
		payload = [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}}
	}

	return &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    payload,
		IsKeyFrame: key,
		PTS:        pts,
		DTS:        pts,
		SequenceID: sequence,
	}
}

func requireRecordStartedEvent(t *testing.T, events chan shared.Event) *shared.RecordStartedMeta {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-events:
			if ev.Type != shared.EventTypeRecordStarted {
				continue
			}
			meta, ok := ev.Meta.(shared.RecordStartedMeta)
			if !ok {
				t.Fatalf("unexpected record_started meta type %T", ev.Meta)
			}
			return &meta
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	t.Fatal("expected record_started event")
	return nil
}
