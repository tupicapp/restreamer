package storage

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tupicapp/restreamer/core/config"
	"github.com/tupicapp/restreamer/core/shared"
)

type Local struct {
	rootPath string
}

func NewLocal(cfg *config.Config) *Local {
	return &Local{
		rootPath: cfg.Storage.RecordingsRoot,
	}
}

type FolderOption func(*Folder)

func WithPublicBaseURL(baseURL string) FolderOption {
	return func(f *Folder) {
		f.publicBaseURL = strings.TrimSpace(strings.TrimRight(baseURL, "/"))
	}
}

func NewFolder(path string, opts ...FolderOption) *Folder {
	folder := &Folder{basePath: path}
	for _, opt := range opts {
		if opt != nil {
			opt(folder)
		}
	}
	return folder
}

func (l *Local) RecordingsRoot() *Folder {
	return &Folder{
		basePath: l.rootPath,
	}
}

type Folder struct {
	basePath      string
	publicBaseURL string
}

func (f *Folder) Folder(path string) *Folder {
	child := &Folder{
		basePath: filepath.Join(f.basePath, path),
	}
	if trimmed := strings.TrimSpace(path); trimmed != "" && f.publicBaseURL != "" {
		child.publicBaseURL = shared.JoinURLPrefix(f.publicBaseURL, filepath.ToSlash(trimmed))
	} else {
		child.publicBaseURL = f.publicBaseURL
	}
	return child
}

func (f *Folder) Open(path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(f.basePath, path)
	return os.Open(fullPath)
}

func (f *Folder) ReadDir() ([]fs.DirEntry, error) {
	return os.ReadDir(f.basePath)
}

func (f *Folder) Stat(path string) (fs.FileInfo, error) {
	fullPath := filepath.Join(f.basePath, path)
	return os.Stat(fullPath)
}

func (f *Folder) Create(path string, expirationTime *time.Time) (io.WriteCloser, error) {
	fullPath := filepath.Join(f.basePath, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return nil, err
	}
	_ = expirationTime
	return os.Create(fullPath)
}

func (f *Folder) WriteFile(path string, data []byte) error {
	fullPath := filepath.Join(f.basePath, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}
	return os.WriteFile(fullPath, data, 0644)
}

func (f *Folder) Rename(oldPath, newPath string) error {
	oldFullPath := filepath.Join(f.basePath, oldPath)
	newFullPath := filepath.Join(f.basePath, newPath)
	return os.Rename(oldFullPath, newFullPath)
}

func (f *Folder) Remove(path string) error {
	fullPath := filepath.Join(f.basePath, path)
	return os.Remove(fullPath)
}

func (f *Folder) RemoveAll() error {
	return os.RemoveAll(f.basePath)
}

func (f *Folder) StartCleaner(interval time.Duration, ttl time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("clean interval must be positive")
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = f.Clean(time.Now().Add(-ttl), 0)
		}
	}()
	return nil
}

func (f *Folder) Clean(formerThan time.Time, limit int64) (removed int64, err error) {
	entries, err := os.ReadDir(f.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if shouldPreserveDuringClean(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(formerThan) {
			fullPath := filepath.Join(f.basePath, entry.Name())
			if err := os.Remove(fullPath); err == nil {
				removed++
				if limit > 0 && removed >= limit {
					break
				}
			}
		}
	}
	return removed, nil
}

func shouldPreserveDuringClean(name string) bool {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(name)))
	return ext == ".m3u8"
}

func (f *Folder) ObjectURL(path string) (string, error) {
	if f.publicBaseURL != "" {
		return shared.JoinURLPrefix(f.publicBaseURL, filepath.ToSlash(strings.TrimSpace(path))), nil
	}
	fullPath := filepath.Join(f.basePath, path)
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", err
	}
	return "file://" + filepath.ToSlash(absPath), nil
}

func (f *Folder) LocalPath() string {
	return f.basePath
}
