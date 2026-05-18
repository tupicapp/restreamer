package shared

import (
	"fmt"
	"path"
	"strings"
	"time"
)

// WriteFileAtomic writes a file through a temporary sibling object and then
// renames it into place so readers never observe partially-written contents.
func WriteFileAtomic(folder Folder, target string, data []byte) error {
	if folder == nil {
		return fmt.Errorf("folder is required")
	}

	target = strings.TrimSpace(strings.TrimPrefix(target, "/"))
	if target == "" {
		return fmt.Errorf("target path is required")
	}

	dir := path.Dir(target)
	if dir == "." {
		dir = ""
	}
	tmpName := fmt.Sprintf(".%s.tmp-%d", path.Base(target), time.Now().UnixNano())
	tmpPath := tmpName
	if dir != "" {
		tmpPath = path.Join(dir, tmpName)
	}

	if err := folder.WriteFile(tmpPath, data); err != nil {
		return err
	}
	if err := folder.Rename(tmpPath, target); err != nil {
		_ = folder.Remove(tmpPath)
		return err
	}
	return nil
}
