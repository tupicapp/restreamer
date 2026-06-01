package irajstreamer

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	shared "github.com/tupicapp/restreamer/core/shared"
)

func shouldPauseWhenInactive(stream Stream) bool {
	if stream == nil {
		return false
	}
	capable, ok := stream.(pauseWhenInactiveCapable)
	return ok && capable.ShouldPauseWhenInactive()
}

func hashStruct(v any) (string, error) {
	// Convert struct to a deterministic JSON representation
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	// Compute hash
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h[:]), nil
}

func RewriteHLSPlaylist(content, urlPrefix string) string {
	return shared.RewriteHLSPlaylist(content, urlPrefix)
}

func JoinHLSPrefix(prefix string, suffixes ...string) string {
	return shared.JoinURLPrefix(prefix, suffixes...)
}
