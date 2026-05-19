package inputs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
	"go.uber.org/zap"
)

const (
	MpegTSTimeScale    = 90000.0
	MP4TimeScale       = 15360
	SampleRate         = 48000
	TsAACBytePerSample = 1024

	// Segment-based buffering: at most maxBufferedSegments segments worth of
	// frames are held in pendingVideoBuf/pendingAudioBuf at any time. Draining
	// starts as soon as at least one full segment is buffered.
	maxBufferedSegments = 2
	minBufferedSegments = 1
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

	// Per-segment frame counts in FIFO order. Each entry is the number of
	// frames accumulated for that segment so far. While a segment is being
	// read its entry is the *last* one in the slice and keeps growing; once
	// the segment hits EOF endSegment() is called and a new slot is appended
	// only when the next segment starts. Drainage only consumes *closed*
	// (non-last, or last when openSegment is false) entries to avoid emitting
	// frames out of order.
	videoSegmentCounts []int
	audioSegmentCounts []int
	openSegment        bool

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
		videoChan:    make(chan *Frame, 300),
		audioChan:    make(chan *Frame, 300),
		done:         make(chan struct{}),
		started:      make(chan struct{}),
		segmentsChan: make(chan *playlist.MediaSegment, 50),
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
func (r *hlsInput) ShouldPauseWhenInactive() bool  { return true }

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
		// Mark any open segment as closed so remaining frames can be drained
		r.endSegment()

		// Wait for pending buffers to drain (with timeout to prevent hanging)
		deadline := time.Now().Add(2 * time.Second)
		for {
			r.pendingMu.Lock()
			videoDrained := len(r.pendingVideoBuf) == 0 && len(r.videoSegmentCounts) == 0
			audioDrained := len(r.pendingAudioBuf) == 0 && len(r.audioSegmentCounts) == 0
			r.pendingMu.Unlock()

			if videoDrained && audioDrained {
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}

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

// enqueuePendingVideo appends a video frame to the pending buffer. The frame
// is attributed to the currently-open segment (the last entry of
// videoSegmentCounts, opened by beginSegment).
func (r *hlsInput) enqueuePendingVideo(frame *Frame) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	r.pendingVideoBuf = append(r.pendingVideoBuf, frame)
	if n := len(r.videoSegmentCounts); n > 0 && r.openSegment {
		r.videoSegmentCounts[n-1]++
	} else {
		// Defensive: no open segment — treat as a one-frame closed segment.
		r.videoSegmentCounts = append(r.videoSegmentCounts, 1)
	}
}

// enqueuePendingAudio appends an audio frame to the pending buffer.
func (r *hlsInput) enqueuePendingAudio(frame *Frame) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	r.pendingAudioBuf = append(r.pendingAudioBuf, frame)
	if n := len(r.audioSegmentCounts); n > 0 && r.openSegment {
		r.audioSegmentCounts[n-1]++
	} else {
		r.audioSegmentCounts = append(r.audioSegmentCounts, 1)
	}
}

// beginSegment opens a new segment slot in the per-stream segment-count FIFOs.
// Frames enqueued after this call are attributed to the new segment until
// endSegment is called.
func (r *hlsInput) beginSegment() {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	r.videoSegmentCounts = append(r.videoSegmentCounts, 0)
	r.audioSegmentCounts = append(r.audioSegmentCounts, 0)
	r.openSegment = true
}

// endSegment marks the currently-open segment as fully read. Frames buffered
// for it become eligible for draining.
func (r *hlsInput) endSegment() {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	r.openSegment = false
}

// bufferedSegmentCount returns the total number of segments (open + closed)
// currently held in pending buffers. Caller must hold pendingMu.
func (r *hlsInput) bufferedSegmentCountLocked() int {
	n := len(r.videoSegmentCounts)
	if len(r.audioSegmentCounts) > n {
		n = len(r.audioSegmentCounts)
	}
	return n
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
	// Poll frequently so we pick up new segments quickly. For a 2-second
	// segment duration, 300ms keeps the inter-segment gap under one frame.
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			if !r.IsStarted {
				continue
			}
			err := r.updateMediaPlaylist()
			if err != nil {
				getLogger().Error("hls reader: failed to update media playlist", zap.String("stream_id", r.id), zap.Error(err))
			}
		}
	}
}

// popReadyVideo returns the frames belonging to the front (oldest) *closed*
// segment, sorted by DTS. Returns nil while the only buffered segment is still
// open (frames may still be appended to it).
func (r *hlsInput) popReadyVideo() []*Frame {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	closed := len(r.videoSegmentCounts)
	if r.openSegment && closed > 0 {
		closed--
	}

	if closed == 0 {
		return nil
	}

	// Skip empty closed segments so they don't block draining.
	// A segment is closed if it's not the last one, or it's the last one and openSegment=false.
	for len(r.videoSegmentCounts) > 0 && r.videoSegmentCounts[0] == 0 {
		isLastSegment := len(r.videoSegmentCounts) == 1
		isLastSegmentClosed := !r.openSegment

		if isLastSegment && isLastSegmentClosed {
			// Last segment is closed and empty, safe to skip
			r.videoSegmentCounts = r.videoSegmentCounts[1:]
		} else if !isLastSegment {
			// Not last segment, so it's closed and empty, safe to skip
			r.videoSegmentCounts = r.videoSegmentCounts[1:]
		} else {
			// Last segment is open and empty, can't skip (frames might still arrive)
			return nil
		}
	}

	if len(r.videoSegmentCounts) == 0 {
		return nil
	}

	n := r.videoSegmentCounts[0]
	if n > len(r.pendingVideoBuf) {
		return nil
	}

	batch := make([]*Frame, n)
	copy(batch, r.pendingVideoBuf[:n])
	sort.Slice(batch, func(i, j int) bool { return batch[i].DTS < batch[j].DTS })

	remaining := make([]*Frame, len(r.pendingVideoBuf)-n)
	copy(remaining, r.pendingVideoBuf[n:])
	r.pendingVideoBuf = remaining
	r.videoSegmentCounts = r.videoSegmentCounts[1:]

	return batch
}

// popReadyAudio mirrors popReadyVideo for the audio buffer.
func (r *hlsInput) popReadyAudio() []*Frame {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	closed := len(r.audioSegmentCounts)
	if r.openSegment && closed > 0 {
		closed--
	}

	if closed == 0 {
		return nil
	}

	// Skip empty closed segments so they don't block draining.
	// A segment is closed if it's not the last one, or it's the last one and openSegment=false.
	for len(r.audioSegmentCounts) > 0 && r.audioSegmentCounts[0] == 0 {
		isLastSegment := len(r.audioSegmentCounts) == 1
		isLastSegmentClosed := !r.openSegment

		if isLastSegment && isLastSegmentClosed {
			// Last segment is closed and empty, safe to skip
			r.audioSegmentCounts = r.audioSegmentCounts[1:]
		} else if !isLastSegment {
			// Not last segment, so it's closed and empty, safe to skip
			r.audioSegmentCounts = r.audioSegmentCounts[1:]
		} else {
			// Last segment is open and empty, can't skip (frames might still arrive)
			return nil
		}
	}

	if len(r.audioSegmentCounts) == 0 {
		return nil
	}

	n := r.audioSegmentCounts[0]
	if n > len(r.pendingAudioBuf) {
		return nil
	}

	batch := make([]*Frame, n)
	copy(batch, r.pendingAudioBuf[:n])
	sort.Slice(batch, func(i, j int) bool { return batch[i].DTS < batch[j].DTS })

	remaining := make([]*Frame, len(r.pendingAudioBuf)-n)
	copy(remaining, r.pendingAudioBuf[n:])
	r.pendingAudioBuf = remaining
	r.audioSegmentCounts = r.audioSegmentCounts[1:]

	return batch
}

// pendingBacklogExceeded blocks the segment fetcher when we already hold
// maxBufferedSegments segments worth of frames on either stream.
func (r *hlsInput) pendingBacklogExceeded() bool {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	return r.bufferedSegmentCountLocked() >= maxBufferedSegments
}

func (r *hlsInput) drainAndForwardVideo() {
	for {
		select {
		case <-r.done:
			return
		default:
		}

		if !r.IsStarted {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		videoBuffer := r.popReadyVideo()
		if len(videoBuffer) == 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		for _, frame := range videoBuffer {
			select {
			case r.videoChan <- frame:
				if r.realTime {
					time.Sleep(frame.Duration)
				}
			case <-r.done:
				return
			}
		}
	}
}

func (r *hlsInput) drainAndForwardAudio() {
	for {
		select {
		case <-r.done:
			return
		default:
		}

		if !r.IsStarted {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		audioBuffer := r.popReadyAudio()
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
			case <-r.done:
				return
			}
		}
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
			case <-r.done:
				return nil
			default:
				logger.Warn("hls reader: segment queue full, dropping segment", zap.String("uri", segment.URI))
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
					r.beginSegment()
				default:
					time.Sleep(5 * time.Millisecond)
					continue
				}
			}

			err = reader.Read()
			if err == io.EOF {
				r.endSegment()
				reader = nil
				continue
			}

			if err != nil {
				logger.Debug("hls reader: read error", zap.String("stream_id", r.id), zap.Error(err))
			}

			r.LastIO = time.Now()
		}
	}
}

func (r *hlsInput) downloadPlaylist(uri string) ([]byte, error) {
	if strings.HasPrefix(uri, "file://") {
		path, err := fileURIToPath(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve playlist file: %w", err)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read playlist file: %w", err)
		}
		return data, nil
	}

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
