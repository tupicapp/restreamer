package inputs

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
)

func ReferenceInput(baseURL string, playlistURI string, timeScale float64) ([]*Frame, []*Frame, error) {
	_ = timeScale

	var videoFrames []*Frame
	var audioFrames []*Frame
	var videoMu sync.Mutex
	var audioMu sync.Mutex

	playlistData, err := downloadReferenceURL(playlistURI)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download playlist: %w", err)
	}

	pl, err := playlist.Unmarshal(playlistData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse playlist: %w", err)
	}

	var mediaPlaylist *playlist.Media
	switch pl := pl.(type) {
	case *playlist.Media:
		mediaPlaylist = pl
		playlistURL, parseErr := url.Parse(playlistURI)
		if parseErr == nil {
			playlistURL.Path = path.Dir(playlistURL.Path) + "/"
			baseURL = playlistURL.String()
		}
	case *playlist.Multivariant:
		if len(pl.Variants) == 0 {
			return nil, nil, fmt.Errorf("multivariant playlist has no variants")
		}
		variantURI := resolveURL(baseURL, pl.Variants[0].URI)
		variantData, downloadErr := downloadReferenceURL(variantURI)
		if downloadErr != nil {
			return nil, nil, fmt.Errorf("failed to download variant: %w", downloadErr)
		}
		variantPl, unmarshalErr := playlist.Unmarshal(variantData)
		if unmarshalErr != nil {
			return nil, nil, fmt.Errorf("failed to parse variant: %w", unmarshalErr)
		}
		mediaPlaylist, _ = variantPl.(*playlist.Media)
		if mediaPlaylist == nil {
			return nil, nil, fmt.Errorf("variant is not a media playlist")
		}
		variantURL, parseErr := url.Parse(variantURI)
		if parseErr == nil {
			variantURL.Path = path.Dir(variantURL.Path) + "/"
			baseURL = variantURL.String()
		} else {
			baseURL = variantURI
		}
	default:
		return nil, nil, fmt.Errorf("no media playlist found")
	}

	refHlsReader := &hlsInput{
		id:              "ref-reader",
		uri:             playlistURI,
		videoChan:       make(chan *Frame, 1000),
		audioChan:       make(chan *Frame, 1000),
		pendingVideoBuf: make([]*Frame, 0),
		pendingAudioBuf: make([]*Frame, 0),
	}

	segmentFactory := newSegmentFactory(refHlsReader, baseURL)
	segmentFactory.SetMediaPlayList(mediaPlaylist.Map)

	for _, segment := range mediaPlaylist.Segments {
		reader, newErr := segmentFactory.newSegment(segment)
		if newErr != nil {
			return nil, nil, fmt.Errorf("failed to create reader %s: %w", segment.URI, newErr)
		}

		for readCount := 0; readCount < 10000; readCount++ {
			readErr := reader.Read()
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				break
			}
		}
	}

	refHlsReader.pendingMu.Lock()
	videoFrames = append(videoFrames, refHlsReader.pendingVideoBuf...)
	audioFrames = append(audioFrames, refHlsReader.pendingAudioBuf...)
	refHlsReader.pendingMu.Unlock()

	videoMu.Lock()
	audioMu.Lock()
	defer videoMu.Unlock()
	defer audioMu.Unlock()

	return videoFrames, audioFrames, nil
}

func downloadReferenceURL(uri string) ([]byte, error) {
	resp, err := http.Get(uri)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}
