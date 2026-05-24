package test

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
		FilePath:    "testdata/stream.m3u8",
		Description: "Primary HLS test fixture",
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

func resolveTestFixturePath(relativePath string) string {
	if relativePath == "" {
		return ""
	}

	repoRoot := testRepoRootDir()
	if repoRoot == "" {
		return ""
	}

	return filepath.Join(repoRoot, filepath.FromSlash(strings.TrimPrefix(relativePath, "/")))
}

func testFixtureRootDir() string {
	fixturePath := resolveTestFixturePath(testHLSFixtureRelativePath)
	if fixturePath == "" {
		return ""
	}

	return filepath.Dir(fixturePath)
}

func testRepoRootDir() string {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}

	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}

func requireTestFixturePathsExist(t *testing.T, relativePaths ...string) {
	t.Helper()

	for _, relativePath := range relativePaths {
		absolutePath := resolveTestFixturePath(relativePath)
		if absolutePath == "" {
			t.Fatalf("unable to resolve fixture path %q", relativePath)
		}
		if _, err := os.Stat(absolutePath); err != nil {
			t.Fatalf("fixture path %q not available at %s: %v", relativePath, absolutePath, err)
		}
	}
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
		if rootDir := testRepoRootDir(); rootDir != "" {
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
	if err == nil && cfg != nil {
		rtmpURL := strings.TrimSpace(cfg.TestURLs.RTMPURL)
		if rtmpURL != "" {
			return rtmpURL
		}
	}

	testURLs := []string{
		"rtmp://127.0.0.1:1938/live/1",
		testRTMPAVURL,
		"rtmp://127.0.0.1:1935/live/test",
		"rtmp://localhost:1935/live/test",
	}
	for _, testURL := range testURLs {
		if isRTMPURLAvailable(testURL) {
			return testURL
		}
	}

	if err != nil {
		t.Logf("RTMP config not available, falling back to default URL: %v", err)
	}

	return testURLs[0]
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
	if err == nil && cfg != nil {
		hlsURL := strings.TrimSpace(cfg.TestURLs.HLSLiveURL)
		if hlsURL != "" {
			return hlsURL
		}
	}

	if err != nil {
		t.Logf("HLS live config not available, falling back to local fixture: %v", err)
	}

	return getConfiguredHLSFixtureURL(testHLSFixtureRelativePath)
}

func getConfiguredHLSFixtureURL(relativePath string) string {
	baseURL := strings.TrimSpace(os.Getenv("HLS_SERVER_URL"))
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8091"
	}

	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(relativePath, "/")
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
