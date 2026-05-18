package inputs

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"go.uber.org/zap"
)

func HlsDownload(m3u8URL, outputDir string) error {
	return processPlaylist(m3u8URL, outputDir)
}

func processPlaylist(m3u8URL, outputDir string) error {
	resp, err := http.Get(m3u8URL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	baseURL, err := url.Parse(m3u8URL)
	if err != nil {
		return err
	}

	var lines []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if isMasterPlaylist(lines) {
		return handleMasterPlaylist(lines, baseURL, outputDir)
	}

	return handleMediaPlaylist(lines, baseURL, outputDir)
}

func handleMasterPlaylist(lines []string, baseURL *url.URL, outputDir string) error {
	playlistPath := path.Join(outputDir, "master.m3u8")
	out, err := os.Create(playlistPath)
	if err != nil {
		return err
	}
	defer out.Close()

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		fmt.Fprintln(out, line)

		if strings.HasPrefix(line, "#") {
			continue
		}

		variantURL, err := baseURL.Parse(line)
		if err != nil {
			return err
		}

		variantName := path.Base(variantURL.Path)
		variantDir := path.Join(outputDir, strings.TrimSuffix(variantName, ".m3u8"))

		getLogger().Debug("processing variant", zap.String("variant", variantName))

		if strings.Contains(variantName, "stream_2.m3u8") {
			if err := processPlaylist(variantURL.String(), variantDir); err != nil {
				return err
			}

			fmt.Fprintln(out, path.Join(path.Base(variantDir), "playlist.m3u8"))
		}
	}

	return nil
}

func handleMediaPlaylist(lines []string, baseURL *url.URL, outputDir string) error {
	playlistPath := path.Join(outputDir, "playlist.m3u8")
	out, err := os.Create(playlistPath)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, line := range lines {
		// Handle #EXT-X-MAP tag with URI
		if strings.HasPrefix(line, "#EXT-X-MAP") {
			fmt.Fprintln(out, line)
			// Extract URI from #EXT-X-MAP:URI="..."
			uriStart := strings.Index(line, `URI="`)
			if uriStart != -1 {
				uriStart += len(`URI="`)
				uriEnd := strings.Index(line[uriStart:], `"`)
				if uriEnd != -1 {
					initURI := line[uriStart : uriStart+uriEnd]
					if err := downloadInitSegment(initURI, baseURL, outputDir); err != nil {
						return fmt.Errorf("failed to download init segment: %w", err)
					}
				}
			}
			continue
		}

		// Handle init file listed directly (without #EXT-X-MAP tag)
		if strings.HasSuffix(strings.ToLower(line), ".mp4") && !strings.HasPrefix(line, "#") {
			fmt.Fprintln(out, line)
			if err := downloadInitSegment(line, baseURL, outputDir); err != nil {
				return fmt.Errorf("failed to download init segment: %w", err)
			}
			continue
		}

		// Handle regular comments and empty lines
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			fmt.Fprintln(out, line)
			continue
		}

		// Handle regular segments
		segURL, err := baseURL.Parse(line)
		if err != nil {
			return err
		}

		segName := path.Base(segURL.Path)
		segPath := path.Join(outputDir, segName)

		getLogger().Debug("downloading segment", zap.String("segment", segName))

		if err := downloadFile(segURL.String(), segPath); err != nil {
			return err
		}

		fmt.Fprintln(out, segName)
	}

	return nil
}

func downloadInitSegment(initURI string, baseURL *url.URL, outputDir string) error {
	initURL, err := baseURL.Parse(initURI)
	if err != nil {
		return err
	}

	initName := path.Base(initURL.Path)
	initPath := path.Join(outputDir, initName)

	getLogger().Debug("downloading init segment", zap.String("segment", initName))

	return downloadFile(initURL.String(), initPath)
}

func isMasterPlaylist(lines []string) bool {
	for _, l := range lines {
		if strings.HasPrefix(l, "#EXT-X-STREAM-INF") {
			return true
		}
	}
	return false
}

func downloadFile(fileURL, filePath string) error {
	info, err := os.Stat(filePath)
	if err == nil && info != nil && info.Name() != "" {
		getLogger().Debug("segment exists", zap.String("path", filePath))
		return nil
	}

	resp, err := http.Get(fileURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
