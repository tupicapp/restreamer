package inputs

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
)

type Segment interface {
	Read() error
}

type segmentState struct {
	VideoSequenceID int64
	AudioSequenceID int64

	lastVideoPTS time.Duration
	lastAudioPTS time.Duration

	lastVideoGOPID int64
	lastAudioGOPID int64

	gopMu        sync.RWMutex
	sequenceIDMu sync.Mutex
}

type segmentHost interface {
	streamID() string
	streamURI() string
	enqueuePendingVideo(*Frame)
	enqueuePendingAudio(*Frame)
	incTotalVideoFrames()
	incTotalAudioFrames()
}

type segmentFactory struct {
	host     segmentHost
	client   *http.Client
	mediaMap *playlist.MediaMap
	baseUrl  string
	state    *segmentState
}

func newSegmentFactory(host segmentHost, baseUrl string) *segmentFactory {
	return &segmentFactory{
		host:    host,
		client:  http.DefaultClient,
		baseUrl: baseUrl,
		state:   &segmentState{},
	}
}

func (f *segmentFactory) newSegment(segment *playlist.MediaSegment) (Segment, error) {
	segmentURL := resolveURL(f.baseUrl, segment.URI)
	var data []byte
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		data, err = fetchSegmentData(f.client, segmentURL, segment.ByteRangeStart, segment.ByteRangeLength)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to download segment: %w", err)
	}

	if isFMP4Segment(segment) {
		return NewFmp4(f.baseUrl, data, f.mediaMap, segment.URI, f.host, f.state)
	}

	return NewMpegTs(data, f.host, f.state)
}

func (f *segmentFactory) SetMediaPlayList(mediaMap *playlist.MediaMap) {
	f.mediaMap = mediaMap
}

func unitsToDuration(units uint64, timeScale uint32) time.Duration {
	if timeScale == 0 {
		timeScale = MpegTSTimeScale
	}

	scale := uint64(timeScale)
	whole := units / scale
	rem := units % scale

	return time.Duration(whole)*time.Second + time.Duration(rem)*time.Second/time.Duration(timeScale)
}

type fmp4TrackInfo struct {
	codecLabel string
	isVideo    bool
	timeScale  uint32
}

func applyByteRange(data []byte, start *uint64, length *uint64) ([]byte, error) {
	if start == nil && length == nil {
		return data, nil
	}

	dataLen := uint64(len(data))
	offset := uint64(0)
	if start != nil {
		offset = *start
		if offset > dataLen {
			return nil, fmt.Errorf("byte range start (%d) exceeds available data (%d)", offset, dataLen)
		}
	}

	end := dataLen
	if length != nil {
		if *length > dataLen-offset {
			return nil, fmt.Errorf("byte range length (%d) exceeds available data (%d) from offset %d", *length, dataLen-offset, offset)
		}
		end = offset + *length
	}

	return data[int(offset):int(end)], nil
}

func isFMP4Segment(segment *playlist.MediaSegment) bool {
	uri := segment.URI
	if idx := strings.IndexAny(uri, "?#"); idx >= 0 {
		uri = uri[:idx]
	}
	return strings.HasSuffix(strings.ToLower(uri), ".m4s")
}

func fetchSegmentData(client *http.Client, uri string, start, length *uint64) ([]byte, error) {
	if strings.HasPrefix(uri, "file://") {
		path, err := fileURIToPath(uri)
		if err != nil {
			return nil, err
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		return applyByteRange(data, start, length)
	}

	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("segment download returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return applyByteRange(data, start, length)
}

func fileURIToPath(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme != "file" {
		return "", fmt.Errorf("unsupported file uri scheme %q", u.Scheme)
	}
	if u.Host != "" && u.Host != "localhost" {
		return "", fmt.Errorf("unsupported file uri host %q", u.Host)
	}

	path, err := url.PathUnescape(u.Path)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("empty file uri path")
	}

	return path, nil
}
