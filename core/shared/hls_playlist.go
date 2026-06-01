package shared

import (
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
)

func latestRecorderPlaylistURL(folder Folder, playlistName, publicPrefix string) string {
	if folder == nil {
		return ""
	}

	entries, err := folder.ReadDir()
	if err != nil {
		return ""
	}

	latestSession := ""
	for _, entry := range entries {
		if entry == nil || !entry.IsDir() {
			continue
		}

		sessionID := strings.TrimSpace(entry.Name())
		if sessionID == "" {
			continue
		}

		sessionFolder := folder.Folder(sessionID)
		if sessionFolder == nil || !hasFile(sessionFolder, playlistName) {
			continue
		}

		if latestSession == "" || sessionID > latestSession {
			latestSession = sessionID
		}
	}

	if latestSession == "" {
		return ""
	}

	sessionPrefix := ""
	if strings.TrimSpace(publicPrefix) != "" {
		sessionPrefix = JoinURLPrefix(publicPrefix, latestSession)
	}

	return strings.TrimSpace(PreferredURL(
		sessionPrefix,
		folder.Folder(latestSession),
		playlistName,
	))
}

func openHLSFile(folder Folder, playlistName, reqFile string) (io.ReadCloser, string, error) {
	if folder == nil {
		return nil, "", fmt.Errorf("hls folder is not configured")
	}
	if !hasFile(folder, playlistName) {
		return nil, "", fmt.Errorf("missing playlist: %s", playlistName)
	}
	if !hasFile(folder, reqFile) {
		return nil, "", fmt.Errorf("missing hls file: %s", reqFile)
	}

	contentType := ""
	switch {
	case strings.HasSuffix(reqFile, ".m3u8"):
		contentType = "application/vnd.apple.mpegurl"
	case strings.HasSuffix(reqFile, ".ts"):
		contentType = "video/mp2t"
	}

	f, err := folder.Open(reqFile)
	if err != nil {
		return nil, "", err
	}
	return f, contentType, nil
}

func rewrittenPlaylist(folder Folder, playlistName, urlPrefix string) (string, error) {
	f, err := folder.Open(playlistName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return RewriteHLSPlaylist(string(data), urlPrefix), nil
}

func sanitizeHLSRelativePath(rel string) (string, error) {
	rel = strings.TrimSpace(strings.TrimPrefix(rel, "/"))
	if rel == "" {
		return "", fmt.Errorf("empty path")
	}

	clean := path.Clean(rel)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("invalid relative path")
	}
	return clean, nil
}

func RewriteHLSPlaylist(content, urlPrefix string) string {
	var b strings.Builder
	legacyPrefixes := legacyHLSRewritePrefixes(urlPrefix)

	for _, line := range strings.SplitAfter(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			line = rewriteHLSLine(line, trimmed, urlPrefix, legacyPrefixes)
		}
		b.WriteString(line)
	}

	return b.String()
}

func rewriteHLSLine(line, trimmed, urlPrefix string, legacyPrefixes []string) string {
	switch {
	case strings.HasPrefix(trimmed, "http://"), strings.HasPrefix(trimmed, "https://"):
		return line
	case strings.HasPrefix(trimmed, "/"):
		if rewritten, ok := rewriteLegacyHLSURI(trimmed, urlPrefix, legacyPrefixes); ok {
			return rewritten + trailingNewline(line)
		}
		return line
	default:
		return JoinURLPrefix(urlPrefix, strings.TrimLeft(trimmed, "/")) + trailingNewline(line)
	}
}

func rewriteLegacyHLSURI(uri, urlPrefix string, legacyPrefixes []string) (string, bool) {
	for _, prefix := range legacyPrefixes {
		if uri == prefix {
			return strings.TrimRight(urlPrefix, "/"), true
		}
		if strings.HasPrefix(uri, prefix+"/") {
			return JoinURLPrefix(urlPrefix, strings.TrimPrefix(uri, prefix+"/")), true
		}
	}
	return "", false
}

func legacyHLSRewritePrefixes(urlPrefix string) []string {
	basePath := strings.TrimSpace(urlPrefix)
	if basePath == "" {
		return nil
	}
	if strings.HasPrefix(basePath, "http://") || strings.HasPrefix(basePath, "https://") {
		if parsed, err := url.Parse(basePath); err == nil {
			basePath = parsed.Path
		}
	}

	basePath = "/" + strings.Trim(basePath, "/")
	if basePath == "/" {
		return nil
	}

	suffix := strings.Trim(basePath, "/")
	return []string{
		JoinURLPrefix("/hls", suffix),
		JoinURLPrefix("/v1/restream/hls", suffix),
	}
}

func trailingNewline(line string) string {
	if strings.HasSuffix(line, "\n") {
		return "\n"
	}
	return ""
}

func hlsPathPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "/hls"
	}
	return JoinURLPrefix(prefix)
}

func inputHLSPathPrefix(prefix, inputID string) string {
	return JoinURLPrefix(hlsPathPrefix(prefix), "inputs", strings.TrimSpace(inputID))
}

func outputHLSPathPrefix(prefix string) string {
	return JoinURLPrefix(hlsPathPrefix(prefix), "output")
}

func inputRecordPathPrefix(prefix, inputID string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	return JoinURLPrefix(prefix, "inputs", strings.TrimSpace(inputID))
}

func outputRecordPathPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	return JoinURLPrefix(prefix, "output")
}
