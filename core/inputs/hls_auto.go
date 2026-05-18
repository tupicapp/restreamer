package inputs

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
)

// ProbeHLSLive fetches the playlist at uri, resolves any multivariant playlist
// to a media playlist, and returns true when #EXT-X-ENDLIST is absent (live stream).
// Returns false when #EXT-X-ENDLIST is present (VOD/file).
func ProbeHLSLive(uri string) (bool, error) {
	normalized, err := normalizeHLSURI(uri)
	if err != nil {
		return false, fmt.Errorf("probe hls live: %w", err)
	}

	data, err := fetchM3U8(normalized)
	if err != nil {
		return false, err
	}

	return probeIsLive(normalized, data)
}

// probeIsLive inspects playlist bytes rooted at baseURI.
// Multivariant playlists are followed to their lowest-bandwidth variant.
func probeIsLive(baseURI string, data []byte) (bool, error) {
	pl, err := playlist.Unmarshal(data)
	if err != nil {
		// Parsing failed — fall back to raw string check.
		return !bytes.Contains(data, []byte("#EXT-X-ENDLIST")), nil
	}

	switch p := pl.(type) {
	case *playlist.Media:
		return !bytes.Contains(data, []byte("#EXT-X-ENDLIST")), nil

	case *playlist.Multivariant:
		variantURI := lowestBandwidthVariantURI(p.Variants)
		if variantURI == "" {
			return false, fmt.Errorf("multivariant playlist has no usable variants")
		}
		resolved := resolveURL(baseURI, variantURI)
		variantData, err := fetchM3U8(resolved)
		if err != nil {
			return false, fmt.Errorf("fetch variant %q: %w", resolved, err)
		}
		return !bytes.Contains(variantData, []byte("#EXT-X-ENDLIST")), nil

	default:
		return false, fmt.Errorf("unsupported playlist type")
	}
}

// NewHLSAuto probes the playlist at uri and returns:
//   - NewHLS  (VOD/file mode) when #EXT-X-ENDLIST is present
//   - NewHLSLive (live mode) when #EXT-X-ENDLIST is absent
//
// opts are forwarded to NewHLS and have no effect on NewHLSLive.
func NewHLSAuto(id, uri string, opts ...HlsOption) (Stream, error) {
	isLive, err := ProbeHLSLive(uri)
	if err != nil {
		return nil, fmt.Errorf("hls auto detect %q: %w", uri, err)
	}

	if isLive {
		return NewHLSLive(id, uri), nil
	}

	s := NewHLS(id, uri, opts...)
	if s == nil {
		return nil, fmt.Errorf("hls auto: failed to create file reader for %q", uri)
	}
	return s, nil
}

// fetchM3U8 reads a playlist from either a file:// URI or an HTTP(S) URL.
func fetchM3U8(uri string) ([]byte, error) {
	if strings.HasPrefix(uri, "file://") {
		path := strings.TrimPrefix(uri, "file://")
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read playlist file %q: %w", path, err)
		}
		return data, nil
	}

	resp, err := http.Get(uri) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("fetch playlist %q: %w", uri, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch playlist %q: status %d", uri, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read playlist body %q: %w", uri, err)
	}
	return data, nil
}
