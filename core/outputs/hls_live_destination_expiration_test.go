package outputs

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tupicapp/restreamer/core/config"
	"github.com/tupicapp/restreamer/core/shared"
	"github.com/tupicapp/restreamer/core/storage"
)

type recordingFolder struct {
	base *storage.Folder

	mu                sync.Mutex
	createExpirations map[string]*time.Time
}

func newRecordingFolder(base *storage.Folder) *recordingFolder {
	return &recordingFolder{
		base:              base,
		createExpirations: make(map[string]*time.Time),
	}
}

func (f *recordingFolder) Folder(path string) shared.Folder {
	return newRecordingFolder(f.base.Folder(path))
}

func (f *recordingFolder) Open(path string) (io.ReadCloser, error) {
	return f.base.Open(path)
}

func (f *recordingFolder) ReadDir() ([]fs.DirEntry, error) {
	return f.base.ReadDir()
}

func (f *recordingFolder) Stat(path string) (fs.FileInfo, error) {
	return f.base.Stat(path)
}

func (f *recordingFolder) Create(path string, expirationTime *time.Time) (io.WriteCloser, error) {
	f.mu.Lock()
	if expirationTime != nil {
		expiresAt := *expirationTime
		f.createExpirations[path] = &expiresAt
	} else {
		f.createExpirations[path] = nil
	}
	f.mu.Unlock()
	return f.base.Create(path, expirationTime)
}

func (f *recordingFolder) WriteFile(path string, data []byte) error {
	return f.base.WriteFile(path, data)
}

func (f *recordingFolder) Rename(oldPath, newPath string) error {
	return f.base.Rename(oldPath, newPath)
}

func (f *recordingFolder) Remove(path string) error {
	return f.base.Remove(path)
}

func (f *recordingFolder) RemoveAll() error {
	return f.base.RemoveAll()
}

func (f *recordingFolder) StartCleaner(interval time.Duration, ttl time.Duration) error {
	return f.base.StartCleaner(interval, ttl)
}

func (f *recordingFolder) Clean(formerThan time.Time, limit int64) (removed int64, err error) {
	return f.base.Clean(formerThan, limit)
}

func (f *recordingFolder) ObjectURL(path string) (string, error) {
	return f.base.ObjectURL(path)
}

func (f *recordingFolder) LocalPath() string {
	return f.base.LocalPath()
}

func (f *recordingFolder) expirationFor(path string) (*time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	expiration, ok := f.createExpirations[path]
	return expiration, ok
}

func waitForHLSVideoFrame(t *testing.T, dest *hlsLiveAsync) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dest.TotalVideoFrames > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for HLS destination to process video")
}

func TestHLSLiveDestination_LiveSegmentsUseExpirationHint(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls-live")
	base := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))
	folder := newRecordingFolder(base)

	stream, err := NewHLSLiveDestination("hls-live-expiration-test", folder, WithHLSLiveMode())
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}

	dest, ok := stream.(*hlsLiveAsync)
	if !ok {
		t.Fatalf("unexpected stream type %T", stream)
	}

	dest.Start()
	dest.GetVideoChan() <- &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        2 * time.Second,
		DTS:        2 * time.Second,
		SequenceID: 1,
	}
	waitForHLSVideoFrame(t, dest)
	dest.Close()

	expirationTime, ok := folder.expirationFor("seg_000000.ts")
	if !ok {
		t.Fatal("expected live segment create call to be recorded")
	}
	if expirationTime == nil {
		t.Fatal("expected live segment expiration hint")
	}

	remaining := time.Until(*expirationTime)
	if remaining < 45*time.Second || remaining > 75*time.Second {
		t.Fatalf("unexpected live segment expiration window: %v", remaining)
	}

	if _, err := os.Stat(filepath.Join(outDir, "seg_000000.ts")); err != nil {
		t.Fatalf("expected segment file to exist: %v", err)
	}
}

func TestHLSLiveDestination_VODSegmentsDoNotUseExpirationHint(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "hls-vod")
	base := storage.NewLocal(&config.Config{
		Storage: config.Storage{RecordingsRoot: filepath.Dir(outDir)},
	}).RecordingsRoot().Folder(filepath.Base(outDir))
	folder := newRecordingFolder(base)

	stream, err := NewHLSLiveDestination("hls-vod-expiration-test", folder)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination failed: %v", err)
	}

	dest, ok := stream.(*hlsLiveAsync)
	if !ok {
		t.Fatalf("unexpected stream type %T", stream)
	}

	dest.Start()
	dest.GetVideoChan() <- &shared.Frame{
		InputID:    "input-a",
		Codec:      "h264",
		Payload:    [][]byte{{0x67, 0x42, 0x00, 0x1f}, {0x68, 0xce, 0x38, 0x80}, {0x65, 0x88, 0x84}},
		IsKeyFrame: true,
		PTS:        2 * time.Second,
		DTS:        2 * time.Second,
		SequenceID: 1,
	}
	waitForHLSVideoFrame(t, dest)
	dest.Close()

	expirationTime, ok := folder.expirationFor("seg_000000.ts")
	if !ok {
		t.Fatal("expected vod segment create call to be recorded")
	}
	if expirationTime != nil {
		t.Fatalf("expected no expiration hint for vod segment, got %v", expirationTime)
	}
}
