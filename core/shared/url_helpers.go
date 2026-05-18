package shared

import (
	"net/url"
	"path"
	"strings"
)

func JoinURLPrefix(prefix string, suffixes ...string) string {
	prefix = strings.TrimSpace(prefix)

	cleanSuffixes := make([]string, 0, len(suffixes))
	for _, part := range suffixes {
		part = strings.TrimSpace(part)
		if part != "" {
			cleanSuffixes = append(cleanSuffixes, strings.Trim(part, "/"))
		}
	}

	if strings.HasPrefix(prefix, "http://") || strings.HasPrefix(prefix, "https://") {
		u, err := url.Parse(prefix)
		if err != nil {
			base := strings.TrimRight(prefix, "/")
			if len(cleanSuffixes) == 0 {
				return base
			}
			return base + "/" + strings.Join(cleanSuffixes, "/")
		}

		parts := make([]string, 0, len(cleanSuffixes)+1)
		basePath := strings.Trim(u.Path, "/")
		if basePath != "" {
			parts = append(parts, basePath)
		}
		parts = append(parts, cleanSuffixes...)

		if len(parts) == 0 {
			u.Path = ""
		} else {
			u.Path = "/" + path.Join(parts...)
		}
		return strings.TrimRight(u.String(), "/")
	}

	if prefix == "" {
		if len(cleanSuffixes) == 0 {
			return ""
		}
		return path.Join(cleanSuffixes...)
	}

	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	prefix = strings.TrimRight(prefix, "/")
	if len(cleanSuffixes) == 0 {
		return prefix
	}

	all := make([]string, 0, len(cleanSuffixes)+1)
	all = append(all, prefix)
	all = append(all, cleanSuffixes...)
	return path.Join(all...)
}

func PreferredURL(prefix string, folder Folder, relPath string) string {
	relPath = strings.TrimSpace(relPath)
	if relPath == "" {
		return ""
	}

	if strings.TrimSpace(prefix) != "" {
		return JoinURLPrefix(prefix, relPath)
	}

	if folder != nil {
		if absoluteURL, err := ResolveObjectURL(folder, relPath); err == nil {
			absoluteURL = strings.TrimSpace(absoluteURL)
			if absoluteURL != "" {
				return absoluteURL
			}
		}
	}

	return relPath
}
