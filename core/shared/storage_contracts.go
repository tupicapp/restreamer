package shared

import (
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"time"
)

// Folder is an object-oriented view rooted at a single directory namespace.
// All path arguments are relative to this folder.
type Folder interface {
	Folder(path string) Folder
	Open(path string) (io.ReadCloser, error)
	ReadDir() ([]fs.DirEntry, error)
	Stat(path string) (fs.FileInfo, error)

	Create(path string, expirationTime *time.Time) (io.WriteCloser, error)
	WriteFile(path string, data []byte) error
	Rename(oldPath, newPath string) error

	Remove(path string) error
	RemoveAll() error
	StartCleaner(interval time.Duration, TTL time.Duration) error
	Clean(formerThan time.Time, limit int64) (removed int64, err error)
}

type ObjectURLProvider interface {
	ObjectURL(path string) (string, error)
}

type LocalPathProvider interface {
	LocalPath() string
}

func ResolveObjectURL(folder Folder, path string) (string, error) {
	if folder == nil {
		return "", fmt.Errorf("nil folder")
	}
	if provider, ok := folder.(ObjectURLProvider); ok {
		return provider.ObjectURL(path)
	}
	if adapter, ok := folder.(folderAdapter); ok {
		return adapter.objectURL(path)
	}
	return "", fmt.Errorf("folder does not support object urls")
}

func ResolveLocalPath(folder Folder) (string, error) {
	if folder == nil {
		return "", fmt.Errorf("nil folder")
	}
	if provider, ok := folder.(LocalPathProvider); ok {
		path := provider.LocalPath()
		if path == "" {
			return "", fmt.Errorf("folder returned empty local path")
		}
		return path, nil
	}
	if adapter, ok := folder.(folderAdapter); ok {
		return adapter.localPath()
	}
	return "", fmt.Errorf("folder does not support local paths")
}

func AdaptFolder(v any) (Folder, error) {
	if v == nil {
		return nil, fmt.Errorf("nil folder")
	}
	if f, ok := v.(Folder); ok {
		return f, nil
	}
	return folderAdapter{value: reflect.ValueOf(v)}, nil
}

type folderAdapter struct {
	value reflect.Value
}

func (a folderAdapter) call(name string, args ...any) []reflect.Value {
	method := a.value.MethodByName(name)
	if !method.IsValid() {
		panic(fmt.Sprintf("folder adapter: missing method %s", name))
	}
	in := make([]reflect.Value, 0, len(args))
	for _, arg := range args {
		in = append(in, reflect.ValueOf(arg))
	}
	return method.Call(in)
}

func (a folderAdapter) objectURL(path string) (string, error) {
	method := a.value.MethodByName("ObjectURL")
	if !method.IsValid() {
		return "", fmt.Errorf("folder adapter: missing method ObjectURL")
	}
	out := method.Call([]reflect.Value{reflect.ValueOf(path)})
	if len(out) != 2 {
		return "", fmt.Errorf("folder adapter: invalid ObjectURL signature")
	}
	urlValue := out[0].Interface()
	urlText, _ := urlValue.(string)
	return urlText, toError(out[1].Interface())
}

func (a folderAdapter) localPath() (string, error) {
	method := a.value.MethodByName("LocalPath")
	if !method.IsValid() {
		return "", fmt.Errorf("folder adapter: missing LocalPath")
	}
	out := method.Call(nil)
	if len(out) != 1 {
		return "", fmt.Errorf("folder adapter: invalid LocalPath signature")
	}
	pathValue := out[0].Interface()
	pathText, _ := pathValue.(string)
	return pathText, nil
}

func (a folderAdapter) Folder(path string) Folder {
	out := a.call("Folder", path)
	if len(out) == 0 {
		return nil
	}
	f, err := AdaptFolder(out[0].Interface())
	if err != nil {
		return nil
	}
	return f
}

func (a folderAdapter) Open(path string) (io.ReadCloser, error) {
	out := a.call("Open", path)
	return out[0].Interface().(io.ReadCloser), toError(out[1].Interface())
}

func (a folderAdapter) ReadDir() ([]fs.DirEntry, error) {
	out := a.call("ReadDir")
	return out[0].Interface().([]fs.DirEntry), toError(out[1].Interface())
}

func (a folderAdapter) Stat(path string) (fs.FileInfo, error) {
	out := a.call("Stat", path)
	stat := out[0].Interface()
	if stat == nil {
		return nil, toError(out[1].Interface())
	}
	info, ok := stat.(fs.FileInfo)
	if !ok {
		return nil, fmt.Errorf("folder adapter: Stat returned %T, want fs.FileInfo", stat)
	}
	return info, toError(out[1].Interface())
}

func (a folderAdapter) Create(path string, expirationTime *time.Time) (io.WriteCloser, error) {
	out := a.call("Create", path, expirationTime)
	return out[0].Interface().(io.WriteCloser), toError(out[1].Interface())
}

func (a folderAdapter) WriteFile(path string, data []byte) error {
	out := a.call("WriteFile", path, data)
	return toError(out[0].Interface())
}

func (a folderAdapter) Rename(oldPath, newPath string) error {
	out := a.call("Rename", oldPath, newPath)
	return toError(out[0].Interface())
}

func (a folderAdapter) Remove(path string) error {
	out := a.call("Remove", path)
	return toError(out[0].Interface())
}

func (a folderAdapter) RemoveAll() error {
	out := a.call("RemoveAll")
	return toError(out[0].Interface())
}

func (a folderAdapter) StartCleaner(interval time.Duration, ttl time.Duration) error {
	out := a.call("StartCleaner", interval, ttl)
	return toError(out[0].Interface())
}

func (a folderAdapter) Clean(formerThan time.Time, limit int64) (removed int64, err error) {
	out := a.call("Clean", formerThan, limit)
	removed = out[0].Interface().(int64)
	err = toError(out[1].Interface())
	return
}

func toError(v any) error {
	if v == nil {
		return nil
	}
	return v.(error)
}
