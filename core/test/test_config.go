package test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/spf13/viper"
)

// TestVideoConfig represents a test video configuration
type TestVideoConfig struct {
	Name        string // Human-readable name for the test video
	FilePath    string // Path to HLS directory (relative to testdata/hls) or RTMP URL
	Description string // Description of the video
	Skip        bool   // Whether to skip this video in tests
}

// TestVideoList holds the list of test videos to use
var TestVideoList = []TestVideoConfig{
	// HLS Videos
	// {
	// 	Name:        "hls_video_1",
	// 	FilePath:    "1",
	// 	Description: "Primary HLS test video",
	// 	Skip:        false,
	// },
	{
		Name:        "hls_video_1",
		FilePath:    "testdata/hls/ts_1/index.m3u8",
		Description: "Primary HLS test video",
		Skip:        false,
	},
	// Add more HLS videos here as they become available
	// {
	// 	Name:        "hls_video_2",
	// 	FilePath:    "2",
	// 	Description: "Secondary HLS test video",
	// 	Skip:        false,
	// },

	// RTMP Videos
	// RTMP videos are configured via environment variable RTMP_URL
	// or fallback URLs. They will be added dynamically in getTestRTMPVideos()
}

// getTestHLSVideos returns a list of HLS video configurations that are available
func getTestHLSVideos(t *testing.T) []TestVideoConfig {
	testdataDir := findTestdataDirForTests()
	if testdataDir == "" {
		return []TestVideoConfig{}
	}

	var availableVideos []TestVideoConfig
	for _, video := range TestVideoList {
		// Skip RTMP URLs (they start with "rtmp://")
		if strings.HasPrefix(video.FilePath, "rtmp://") || video.Skip {
			continue
		}

		hlsDir := filepath.Join(testdataDir, "hls", video.FilePath)
		if _, err := os.Stat(hlsDir); err == nil {
			availableVideos = append(availableVideos, video)
		} else {
			t.Logf("Skipping HLS video '%s' (%s): directory not found: %v", video.Name, video.FilePath, err)
		}
	}

	return availableVideos
}

// getTestRTMPVideos returns a list of RTMP video configurations that are available
func getTestRTMPVideos(t *testing.T) []TestVideoConfig {
	var availableVideos []TestVideoConfig

	// First, try environment variable
	rtmpURL := os.Getenv("RTMP_URL")
	if rtmpURL != "" {
		availableVideos = append(availableVideos, TestVideoConfig{
			Name:        "rtmp_env",
			FilePath:    rtmpURL,
			Description: "RTMP URL from RTMP_URL environment variable",
			Skip:        false,
		})
		return availableVideos
	}

	// Try common test RTMP URLs
	testURLs := []string{
		"rtmp://localhost:1938/live/1",
		"rtmp://127.0.0.1:1938/live/1",
		"rtmp://localhost:1935/live/test",
		"rtmp://127.0.0.1:1935/live/test",
	}

	for _, testURL := range testURLs {
		// Try to connect to verify availability
		if isRTMPURLAvailable(testURL) {
			availableVideos = append(availableVideos, TestVideoConfig{
				Name:        fmt.Sprintf("rtmp_%s", testURL),
				FilePath:    testURL,
				Description: fmt.Sprintf("RTMP URL: %s", testURL),
				Skip:        false,
			})
			// Return first available
			return availableVideos
		}
	}

	return availableVideos
}

// isRTMPURLAvailable checks if an RTMP URL is available by attempting to connect
func isRTMPURLAvailable(rtmpURL string) bool {
	u, err := url.Parse(addDefaultRTMPPort(rtmpURL))
	if err != nil {
		return false
	}

	c := &gortmplib.Client{
		URL:     u,
		Publish: false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = c.Initialize(ctx)
	if err == nil {
		c.Close()
		return true
	}
	return false
}

// setupHLSVideoServer sets up an HTTP server for an HLS video and returns the playlist URI
// video.FilePath should be a full path like "testdata/hls/ts_1/index.m3u8" or relative like "2"
func setupHLSVideoServer(t *testing.T, video TestVideoConfig) (string, *httptest.Server, error) {
	if baseURL := strings.TrimSpace(os.Getenv("HLS_SERVER_URL")); baseURL != "" {
		baseURL = strings.TrimRight(baseURL, "/")

		if strings.HasPrefix(video.FilePath, "http://") || strings.HasPrefix(video.FilePath, "https://") {
			requireHTTPReachable(t, video.FilePath, 5*time.Second)
			return video.FilePath, nil, nil
		}

		if strings.Contains(video.FilePath, "testdata/hls/") {
			relativePath := strings.TrimPrefix(video.FilePath, "/")
			playlistURI := fmt.Sprintf("%s/%s", baseURL, relativePath)
			requireHTTPReachable(t, playlistURI, 5*time.Second)
			return playlistURI, nil, nil
		}

		playlistURI := fmt.Sprintf("%s/testdata/hls/%s/index.m3u8", baseURL, video.FilePath)
		requireHTTPReachable(t, playlistURI, 5*time.Second)
		return playlistURI, nil, nil
	}

	// Check if it's a full path (contains testdata/hls) or relative path
	if strings.Contains(video.FilePath, "testdata/hls/") {
		// Full path like "testdata/hls/ts_1/index.m3u8"
		// First find the testdata directory
		testdataDir := findTestdataDirForTests()
		if testdataDir == "" {
			return "", nil, fmt.Errorf("testdata directory not found")
		}

		// Resolve the full path relative to testdata directory
		// Remove "testdata/" prefix from the path
		relativePath := strings.TrimPrefix(video.FilePath, "testdata/")
		fullPath := filepath.Join(testdataDir, relativePath)

		// Check if file exists
		if _, err := os.Stat(fullPath); err != nil {
			return "", nil, fmt.Errorf("HLS playlist file not found: %v", err)
		}

		// Start HTTP file server for HLS (serve from testdata directory)
		fileServer := httptest.NewServer(http.FileServer(http.Dir(testdataDir)))
		playlistURI := fmt.Sprintf("%s/%s", fileServer.URL, relativePath)

		return playlistURI, fileServer, nil
	} else {
		// Relative path like "2" - old format for backward compatibility
		testdataDir := findTestdataDirForTests()
		if testdataDir == "" {
			return "", nil, fmt.Errorf("testdata/hls directory not found")
		}

		hlsDir := filepath.Join(testdataDir, "hls", video.FilePath)
		if _, err := os.Stat(hlsDir); err != nil {
			return "", nil, fmt.Errorf("HLS directory not found: %v", err)
		}

		// Start HTTP file server for HLS
		fileServer := httptest.NewServer(http.FileServer(http.Dir(filepath.Join(testdataDir, "hls"))))
		playlistURI := fmt.Sprintf("%s/%s/index.m3u8", fileServer.URL, video.FilePath)

		return playlistURI, fileServer, nil
	}
}

// findTestdataDirForTests finds the testdata directory for tests
// This is a wrapper that can be used by test_config.go
func findTestdataDirForTests() string {
	// Try to find testdata directory by walking up from current directory
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	for {
		testdataPath := filepath.Join(dir, "testdata")
		if _, err := os.Stat(testdataPath); err == nil {
			return testdataPath
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	// Try common relative paths
	testPaths := []string{
		"testdata",
		"../../testdata",
		"../testdata",
		"./testdata",
	}

	for _, path := range testPaths {
		if absPath, err := filepath.Abs(path); err == nil {
			if stat, err := os.Stat(absPath); err == nil && stat.IsDir() {
				return absPath
			}
		}
	}

	return ""
}

// getAllTestVideos returns all available test videos (HLS and RTMP)
func getAllTestVideos(t *testing.T) ([]TestVideoConfig, []TestVideoConfig) {
	hlsVideos := getTestHLSVideos(t)
	rtmpVideos := getTestRTMPVideos(t)
	return hlsVideos, rtmpVideos
}

var (
	testConfigOnce sync.Once
	testConfig     *testRuntimeConfig
	testConfigErr  error
)

type testRuntimeConfig struct {
	TestURLs struct {
		RTMPURL    string `mapstructure:"rtmp_url"`
		HLSLiveURL string `mapstructure:"hls_live_url"`
	} `mapstructure:"test_urls"`
}

func getTestConfig(t *testing.T) (*testRuntimeConfig, error) {
	t.Helper()

	testConfigOnce.Do(func() {
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		viper.AutomaticEnv()

		viper.SetConfigName("default")
		viper.SetConfigType("json")
		if testdataDir := findTestdataDirForTests(); testdataDir != "" {
			rootDir := filepath.Dir(testdataDir)
			viper.AddConfigPath(filepath.Join(rootDir, "configs"))
		}

		if err := viper.ReadInConfig(); err != nil {
			testConfigErr = err
			return
		}

		cfg := &testRuntimeConfig{}
		if err := viper.Unmarshal(cfg); err != nil {
			testConfigErr = err
			return
		}
		testConfig = cfg
	})

	return testConfig, testConfigErr
}

func getConfiguredRTMPURL(t *testing.T) string {
	t.Helper()

	if url := os.Getenv("RTMP_URL"); url != "" {
		return url
	}

	cfg, err := getTestConfig(t)
	if err != nil || cfg == nil {
		t.Fatalf("failed to load test config for RTMP URL: %v", err)
	}

	rtmpURL := strings.TrimSpace(cfg.TestURLs.RTMPURL)
	if rtmpURL == "" {
		t.Fatalf("test_urls.rtmp_url is empty")
	}
	return rtmpURL
}

func requireHTTPReachable(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		t.Fatalf("unable to create request for %s: %v", url, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("url %s not reachable: %v", url, err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		t.Fatalf("url %s returned status %d", url, resp.StatusCode)
	}
}

func getConfiguredHLSLiveURL(t *testing.T) string {
	t.Helper()

	if url := os.Getenv("HLS_LIVE_URL"); url != "" {
		return url
	}

	cfg, err := getTestConfig(t)
	if err != nil || cfg == nil {
		t.Fatalf("failed to load test config for HLS live URL: %v", err)
	}

	hlsURL := strings.TrimSpace(cfg.TestURLs.HLSLiveURL)
	if hlsURL == "" {
		t.Fatalf("test_urls.hls_live_url is empty")
	}
	return hlsURL
}

func getRTMPBaseURL(t *testing.T, rtmpURL string) string {
	t.Helper()

	if rtmpURL == "" {
		return ""
	}

	parsed, err := url.Parse(addDefaultRTMPPort(rtmpURL))
	if err != nil {
		return ""
	}

	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""

	if strings.HasSuffix(parsed.Path, "/") {
		return parsed.String()
	}

	lastSlash := strings.LastIndex(parsed.Path, "/")
	if lastSlash == -1 {
		parsed.Path = parsed.Path + "/"
		return parsed.String()
	}

	parsed.Path = parsed.Path[:lastSlash+1]
	return parsed.String()
}

func requireBinary(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Fatalf("%s not available", name)
	}
}

func requireRTMPPublishing(t *testing.T, rtmpURL string, timeout time.Duration) {
	t.Helper()
	requireBinary(t, "ffprobe")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-i", rtmpURL, "-show_streams")
	if err := cmd.Run(); err != nil {
		t.Fatalf("RTMP not publishing or not reachable: %s (%v)", rtmpURL, err)
	}
}

func addDefaultRTMPPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	if !strings.Contains(u.Host, ":") {
		u.Host = u.Host + ":1935"
	}

	return u.String()
}
