package inputs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	shared "restreamer/irajstreamer/core/shared"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
	"go.uber.org/zap"
)

const (
	MpegTSTimeScale    = 90000.0
	MP4TimeScale       = 15360
	SampleRate         = 44100
	TsAACBytePerSample = 1024
	pendingBufferSize  = 100
)

type hlsInput struct {
	id  string
	uri string

	videoChan chan *Frame
	audioChan chan *Frame

	audioMu sync.RWMutex
	videoMu sync.RWMutex

	IsStarted   bool
	IsInitiated bool

	TotalVideoFrames   int64
	TotalAudioFrames   int64
	DroppedVideoFrames int64
	DroppedAudioFrames int64
	LastIO             time.Time
	RunnerDetails      string

	done      chan struct{}
	closeOnce sync.Once

	started chan struct{}

	baseURL *url.URL

	// Pending frame buffers - preserve insertion order before sorting/forwarding
	pendingVideoBuf []*Frame
	pendingAudioBuf []*Frame
	pendingMu       sync.Mutex

	segmentsChan chan *playlist.MediaSegment

	segmentsMap map[string]struct{}

	segmentFactory *segmentFactory
	realTime       bool
	events         *shared.EventEmitter
}

type HlsOption func(*hlsInput)

func OptionWithRealTime(realTime bool) HlsOption {
	return func(h *hlsInput) { h.realTime = realTime }
}

// NewHLS returns a Stream implementation that reads from an HLS playlist.
func NewHLS(id, uri string, opts ...HlsOption) Stream {
	// Pending channels should be larger to buffer frames before sending to output
	normalizedURI, err := normalizeHLSURI(uri)
	if err != nil {
		getLogger().Error("hls reader: invalid uri", zap.String("stream_id", id), zap.String("uri", uri), zap.Error(err))
		return nil
	}

	baseURL, err := url.Parse(normalizedURI)
	if err != nil {
		getLogger().Error("hls reader: failed to parse uri", zap.String("stream_id", id), zap.String("uri", normalizedURI), zap.Error(err))
		return nil
	}

	h := &hlsInput{
		id:           id,
		uri:          uri,
		baseURL:      baseURL,
		videoChan:    make(chan *Frame, 10),
		audioChan:    make(chan *Frame, 10),
		done:         make(chan struct{}),
		started:      make(chan struct{}),
		segmentsChan: make(chan *playlist.MediaSegment, 1000),
		segmentsMap:  make(map[string]struct{}),
		events:       shared.NewEventEmitter(128),
	}

	for _, opt := range opts {
		opt(h)
	}

	h.segmentFactory = newSegmentFactory(h, baseURL.String())

	return h
}

func (r *hlsInput) GetVideoChan() chan *Frame      { return r.videoChan }
func (r *hlsInput) RestartInterval() time.Duration { return 10 * time.Second }

func (r *hlsInput) GetAudioChan() chan *Frame { return r.audioChan }
func (r *hlsInput) GetID() string             { return r.id }
func (r *hlsInput) Type() string              { return "reader" }
func (r *hlsInput) AudioLock() *sync.RWMutex  { return &r.audioMu }
func (r *hlsInput) VideoLock() *sync.RWMutex  { return &r.videoMu }
func (r *hlsInput) IsRestartable() bool       { return false }
func (r *hlsInput) Stop() {
	r.IsStarted = false
	r.events.Emit(shared.Event{Type: shared.EventTypeStreamStopped, StreamID: r.id, StreamType: r.Type(), Message: "hls reader stopped"})
}
func (r *hlsInput) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamClosed, StreamID: r.id, StreamType: r.Type(), Message: "hls reader closed"})
		r.events.Close()
	})

	r.Stop()
}

func (r *hlsInput) EventChan() chan shared.Event {
	if r.events == nil {
		return nil
	}
	return r.events.Chan()
}

func (r *hlsInput) State() *State {
	return &State{
		LastIO:             r.LastIO,
		IsStarted:          r.IsStarted,
		StreamID:           r.id,
		Url:                r.uri,
		Type:               r.Type(),
		DroppedAudioFrames: float64(r.DroppedAudioFrames),
		DroppedVideoFrames: float64(r.DroppedVideoFrames),
		TotalVideoFrames:   r.TotalVideoFrames,
		TotalAudioFrames:   r.TotalAudioFrames,
	}
}

func (r *hlsInput) Clone() (Stream, error) {
	return NewHLS(r.id, r.uri), nil
}

func (r *hlsInput) WaitForStart(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")
		case <-r.done:
			return fmt.Errorf("hls reader is closed")
		case <-r.started:
			return nil
		}
	}
}

func (r *hlsInput) Start() {
	if r.IsInitiated {
		r.IsStarted = true
		r.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: r.id, StreamType: r.Type(), Message: "hls reader resumed", Meta: shared.StreamLifecycleMeta{URL: r.uri}})
		return
	}

	r.IsInitiated = true
	r.IsStarted = true
	r.RunnerDetails = "hls reader loop"

	getLogger().Debug("hls reader: started", zap.String("stream_id", r.id), zap.String("uri", r.uri))
	r.events.Emit(shared.Event{Type: shared.EventTypeStreamStarted, StreamID: r.id, StreamType: r.Type(), Message: "hls reader started", Meta: shared.StreamLifecycleMeta{URL: r.uri}})

	go r.run()
	go r.playListUpdater()
	go r.drainAndForwardVideo()
	go r.drainAndForwardAudio()

	close(r.started)
}

// enqueuePendingVideo appends a video frame to the pending buffer.
func (r *hlsInput) enqueuePendingVideo(frame *Frame) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	r.pendingVideoBuf = append(r.pendingVideoBuf, frame)
}

// enqueuePendingAudio appends an audio frame to the pending buffer.
func (r *hlsInput) enqueuePendingAudio(frame *Frame) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	r.pendingAudioBuf = append(r.pendingAudioBuf, frame)
}

func (r *hlsInput) streamID() string {
	return r.id
}

func (r *hlsInput) streamURI() string {
	return r.uri
}

func (r *hlsInput) incTotalVideoFrames() {
	r.TotalVideoFrames++
}

func (r *hlsInput) incTotalAudioFrames() {
	r.TotalAudioFrames++
}

func (r *hlsInput) playListUpdater() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	err := r.updateMediaPlaylist()
	if err != nil {
		getLogger().Error("hls reader: failed to update media playlist", zap.String("stream_id", r.id), zap.Error(err))
	}

	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			err := r.updateMediaPlaylist()
			if err != nil {
				getLogger().Error("hls reader: failed to update media playlist", zap.String("stream_id", r.id), zap.Error(err))
			}
		}
	}
}

// popSortedHalfVideo sorts pending video frames by PTS and returns the first half, removing them from the buffer.
func (r *hlsInput) popSortedVideo(sortSize, readSize int) []*Frame {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	if len(r.pendingVideoBuf) < sortSize {
		return nil
	}

	if readSize > sortSize {
		readSize = sortSize
	}

	sort.Slice(r.pendingVideoBuf, func(i, j int) bool {
		return r.pendingVideoBuf[i].DTS < r.pendingVideoBuf[j].DTS
	})

	batch := make([]*Frame, readSize)
	copy(batch, r.pendingVideoBuf[:readSize])

	remaining := make([]*Frame, len(r.pendingVideoBuf)-readSize)
	copy(remaining, r.pendingVideoBuf[readSize:])
	r.pendingVideoBuf = remaining

	return batch
}

// popSortedHalfAudio sorts pending audio frames by PTS and returns the first half, removing them from the buffer.
func (r *hlsInput) popSortedAudio(sortSize, readSize int) []*Frame {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	if len(r.pendingAudioBuf) < sortSize {
		return nil
	}

	if readSize > sortSize {
		readSize = sortSize
	}

	sort.Slice(r.pendingAudioBuf, func(i, j int) bool {
		return r.pendingAudioBuf[i].DTS < r.pendingAudioBuf[j].DTS
	})

	batch := make([]*Frame, readSize)
	copy(batch, r.pendingAudioBuf[:readSize])

	remaining := make([]*Frame, len(r.pendingAudioBuf)-readSize)
	copy(remaining, r.pendingAudioBuf[readSize:])
	r.pendingAudioBuf = remaining

	return batch
}

func (r *hlsInput) pendingBacklogExceeded() bool {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	videoOver := len(r.pendingVideoBuf) >= pendingBufferSize
	audioOver := len(r.pendingAudioBuf) >= pendingBufferSize
	return videoOver || audioOver
}

func (r *hlsInput) drainAndForwardVideo() {
	lastForwardVideo := time.Now()

	for {

		select {
		case <-r.done:
			return
		default:
		}
		videoSortSize := 100
		videoReadSize := 20

		if time.Since(lastForwardVideo) > 1000*time.Millisecond {
			videoSortSize = len(r.pendingVideoBuf)
			videoReadSize = len(r.pendingVideoBuf)
		}

		videoBuffer := r.popSortedVideo(videoSortSize, videoReadSize)
		if len(videoBuffer) == 0 {
			runtime.Gosched()
			continue
		}

		for _, frame := range videoBuffer {
			select {
			case r.videoChan <- frame:
				if r.realTime {
					time.Sleep(frame.Duration)
				}
			}
		}

		lastForwardVideo = time.Now()
	}
}

func (r *hlsInput) drainAndForwardAudio() {
	lastForwardAudio := time.Now()
	for {

		select {
		case <-r.done:
			return
		default:
		}
		audioSortSize := 100
		audioReadSize := 20

		if time.Since(lastForwardAudio) > 1000*time.Millisecond {
			audioSortSize = len(r.pendingAudioBuf)
			audioReadSize = len(r.pendingAudioBuf)
		}

		audioBuffer := r.popSortedAudio(audioSortSize, audioReadSize)
		if len(audioBuffer) == 0 {
			runtime.Gosched()
			continue
		}

		for _, frame := range audioBuffer {
			select {
			case r.audioChan <- frame:
				if r.realTime {
					time.Sleep(frame.Duration)
				}
			}
		}

		lastForwardAudio = time.Now()
	}
}

func (r *hlsInput) updateMediaPlaylist() error {
	logger := getLogger()

	normalizedURI, err := normalizeHLSURI(r.uri)
	if err != nil {
		logger.Error("hls reader: invalid uri", zap.String("stream_id", r.id), zap.String("uri", r.uri), zap.Error(err))
		return err
	}

	baseURL, err := url.Parse(normalizedURI)
	if err != nil {
		logger.Error("hls reader: failed to parse uri", zap.String("stream_id", r.id), zap.String("uri", normalizedURI), zap.Error(err))
		return err
	}

	r.baseURL = baseURL

	// Download and parse the playlist
	playlistData, err := r.downloadPlaylist(normalizedURI)
	if err != nil {
		logger.Error("hls reader: failed to download playlist", zap.String("stream_id", r.id), zap.Error(err))
		return err
	}

	pl, err := playlist.Unmarshal(playlistData)
	if err != nil {
		logger.Error("hls reader: failed to parse playlist", zap.String("stream_id", r.id), zap.Error(err))
		return err
	}

	mediaPlaylist, resolvedURI, err := r.selectMediaPlaylist(pl, normalizedURI)
	if err != nil {
		logger.Error("hls reader: failed to resolve media playlist", zap.String("stream_id", r.id), zap.Error(err))
		return err
	}

	if resolvedURI != "" {
		if variantURL, err := url.Parse(resolvedURI); err == nil {
			r.baseURL = variantURL
		}
	}

	if mediaPlaylist == nil {
		logger.Error("hls reader: no media playlist found", zap.String("stream_id", r.id))
		return errors.New("no media playlist found")
	}

	r.segmentFactory.SetMediaPlayList(mediaPlaylist.Map)

	for _, segment := range mediaPlaylist.Segments {
		_, ok := r.segmentsMap[segment.URI]
		if !ok {
			r.segmentsMap[segment.URI] = struct{}{}
			select {
			case r.segmentsChan <- segment:
				logger.Debug("hls reader: pushing segment", zap.String("uri", segment.URI))
			default:
			}
		}
	}

	return nil
}

func (r *hlsInput) run() {
	logger := getLogger()

	var reader Segment
	var err error
	var segment *playlist.MediaSegment

	for {
		select {
		case <-r.done:
			return
		default:
			if !r.IsStarted {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// Circuit breaker: pause reading if pending backlog is high
			if r.pendingBacklogExceeded() {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			if reader == nil {
				select {
				case segment = <-r.segmentsChan:
					reader, err = r.segmentFactory.newSegment(segment)
					if err != nil {
						logger.Error("hls reader: failed to initialize reader", zap.String("stream_id", r.id), zap.String("uri", segment.URI), zap.Error(err))
						time.Sleep(time.Millisecond * 5)
						continue
					}
				default:
					time.Sleep(5 * time.Millisecond)
					continue
				}
			}

			err = reader.Read()
			if err == io.EOF {
				logger.Debug("hls reader: error : ", zap.String("stream_id", r.id), zap.Error(err))
				reader = nil
				continue
			}

			if err != nil {
				logger.Debug("hls reader: error : ", zap.String("stream_id", r.id), zap.Error(err))
			}

			r.LastIO = time.Now()
		}
	}
}

func (r *hlsInput) downloadPlaylist(uri string) ([]byte, error) {
	req, err := http.Get(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to download playlist: %w", err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("playlist download returned status %d", req.StatusCode)
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read playlist: %w", err)
	}

	return data, nil
}

func (r *hlsInput) selectMediaPlaylist(pl playlist.Playlist, playlistURI string) (*playlist.Media, string, error) {
	switch pl := pl.(type) {
	case *playlist.Media:
		return pl, playlistURI, nil
	case *playlist.Multivariant:

		getLogger().Debug("hls reader: playlist uri", zap.String("uri", playlistURI))
		variantURI, err := selectVariantURI(pl, playlistURI)
		if err != nil {
			return nil, "", err
		}

		getLogger().Debug("hls reader: variant uri", zap.String("uri", variantURI))

		variantData, err := r.downloadPlaylist(variantURI)
		if err != nil {
			return nil, "", err
		}

		variantPl, err := playlist.Unmarshal(variantData)
		if err != nil {
			return nil, "", err
		}

		return r.selectMediaPlaylist(variantPl, variantURI)
	default:
		return nil, "", errors.New("unsupported playlist type")
	}
}

func selectVariantURI(mv *playlist.Multivariant, playlistURI string) (string, error) {
	if len(mv.Variants) == 0 {
		return "", errors.New("multivariant playlist has no variants")
	}

	if uri := lowestBandwidthVariantURI(mv.Variants); uri != "" {
		getLogger().Debug("hls reader: selected variant uri", zap.String("uri", uri))
		return resolveURL(playlistURI, uri), nil
	}

	return "", errors.New("no variant URI available")
}

func lowestBandwidthVariantURI(variants []*playlist.MultivariantVariant) string {
	var best *playlist.MultivariantVariant

	for _, variant := range variants {
		if variant == nil || variant.URI == "" {
			continue
		}

		if best == nil {
			best = variant
			continue
		}

		if variant.Bandwidth > 0 {
			if best.Bandwidth == 0 || variant.Bandwidth < best.Bandwidth {
				best = variant
			}
		}
	}

	if best == nil {
		return ""
	}

	return best.URI
}

func resolveURL(baseURLStr, relativeURI string) string {
	if strings.HasPrefix(relativeURI, "http://") || strings.HasPrefix(relativeURI, "https://") {
		return relativeURI
	}

	base, err := url.Parse(baseURLStr)
	if err != nil {
		return relativeURI
	}

	resolved, err := base.Parse(relativeURI)
	if err != nil {
		return relativeURI
	}

	return resolved.String()
}
