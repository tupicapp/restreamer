package irajstreamer

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tupicapp/restreamer/core/logger"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/recorder"
	shared "github.com/tupicapp/restreamer/core/shared"

	"go.uber.org/zap"
)

type Streamer struct {
	IsStarted bool
	inputsMu  *sync.RWMutex
	outputsMu *sync.RWMutex
	inputs    map[string]Stream

	MultiCaster   MultiCaster
	outputs       map[string]Stream
	activeInputID string
	SwitchChan    chan string

	TotalAudioFrames   int64
	TotalVideoFrames   int64
	DroppedAudioFrames float64
	DroppedVideoFrames float64
	stagedInputID      string

	channelID string

	channelLiveFolder   shared.Folder
	channelRecordFolder shared.Folder
	recordRootFolder    shared.Folder
	inputHLSMu          sync.RWMutex
	inputHLSFolders     map[string]shared.Folder
	inputRecordMu       sync.RWMutex
	inputRecordFolders  map[string]shared.Folder
	hlsConfig           HLSConfig
	recorderConfig      RecorderConfig

	playlistMu    sync.Mutex
	playlistState *ChannelPlaylistState
	watcherOnce   sync.Once

	events   *shared.EventEmitter
	listener EventListener

	closeOnce sync.Once
	done      chan struct{}
}

type pauseWhenInactiveCapable interface {
	ShouldPauseWhenInactive() bool
}

type StreamerOption func(*Streamer)

type HLSConfig struct {
	PlaylistName        string
	SegmentDuration     time.Duration
	PlaylistSize        int
	TargetDuration      int
	ChannelPlaylistSize int
	PathPrefix          string
}

type RecorderConfig struct {
	SegmentDuration time.Duration
	TargetDuration  int
	PathPrefix      string
}

func WithChannelID(channelID string) StreamerOption {
	return func(s *Streamer) {
		s.channelID = strings.TrimSpace(channelID)
	}
}

func WithChannelLiveFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		adapted, err := shared.AdaptFolder(folder)
		if err != nil {
			logger.GetLogger().Warn("streamer: invalid channel live folder", zap.Error(err))
			return
		}
		s.channelLiveFolder = adapted
	}
}

func WithChannelRecordFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		adapted, err := shared.AdaptFolder(folder)
		if err != nil {
			logger.GetLogger().Warn("streamer: invalid channel record folder", zap.Error(err))
			return
		}
		s.channelRecordFolder = adapted
	}
}

func WithRecordRootFolder(folder any) StreamerOption {
	return func(s *Streamer) {
		adapted, err := shared.AdaptFolder(folder)
		if err != nil {
			logger.GetLogger().Warn("streamer: invalid record root folder", zap.Error(err))
			return
		}
		s.recordRootFolder = adapted
	}
}

func WithHLSConfig(cfg HLSConfig) StreamerOption {
	return func(s *Streamer) {
		s.hlsConfig = cfg
	}
}

func WithRecorderConfig(cfg RecorderConfig) StreamerOption {
	return func(s *Streamer) {
		s.recorderConfig = cfg
	}
}

func NewStreamer(opts ...StreamerOption) *Streamer {
	multicaster := NewMultiCaster()
	streamer := &Streamer{
		inputs:      make(map[string]Stream),
		outputs:     make(map[string]Stream),
		inputsMu:    &sync.RWMutex{},
		outputsMu:   &sync.RWMutex{},
		done:        make(chan struct{}),
		SwitchChan:  make(chan string, 10),
		MultiCaster: multicaster,
		playlistState: &ChannelPlaylistState{
			LastSegmentByInput: make(map[string]string),
			Segments:           make([]HLSSegmentRef, 0, 64),
			LastPublishedKeys:  make([]string, 0, 32),
		},
		inputHLSFolders:    make(map[string]shared.Folder),
		inputRecordFolders: make(map[string]shared.Folder),
		events:             shared.NewEventEmitter(256),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(streamer)
		}
	}
	multicaster.SetStreamer(streamer)
	return streamer
}

func (s *Streamer) UpdateStreams(inputs []Stream, outputs []Stream) error {
	// ---- Update Inputs and Outputs concurrently ----
	var wg sync.WaitGroup
	wg.Add(2)

	// Inputs goroutine
	go func() {
		defer wg.Done()
		s.inputsMu.Lock()
		defer s.inputsMu.Unlock()

		current := make(map[string]struct{})
		for _, newInput := range inputs {
			current[newInput.GetID()] = struct{}{}
			if err := s.upsertInputLocked(newInput); err != nil {
				logger.GetLogger().Warn("streamer: input upsert failed",
					zap.String("input_id", newInput.GetID()),
					zap.Error(err))
			}
		}

		// remove inputs not in current list
		for id, existing := range s.inputs {
			if _, ok := current[id]; !ok {
				existing.Close()
				s.emitEvent(shared.Event{
					Type:       shared.EventTypeInputRemoved,
					StreamID:   s.channelOrStreamID(),
					StreamType: "streamer",
					Message:    "input removed from streamer",
					Meta: shared.ChildStreamMeta{
						Role:      "input",
						ChildID:   id,
						ChildType: existing.Type(),
						Managed:   true,
						ChannelID: s.channelID,
					},
				})
				delete(s.inputs, id)
			}
		}
	}()

	// Outputs goroutine
	go func() {
		defer wg.Done()
		s.outputsMu.Lock()
		defer s.outputsMu.Unlock()

		current := make(map[string]struct{})
		for _, newOutput := range outputs {
			current[newOutput.GetID()] = struct{}{}
			if err := s.upsertOutputLocked(newOutput); err != nil {
				logger.GetLogger().Warn("streamer: output upsert failed",
					zap.String("output_id", newOutput.GetID()),
					zap.Error(err))
			}
		}

		// remove outputs not in current list
		for id, existing := range s.outputs {
			if _, ok := current[id]; !ok {
				existing.Close()
				s.emitEvent(shared.Event{
					Type:       shared.EventTypeDestinationRemoved,
					StreamID:   s.channelOrStreamID(),
					StreamType: "streamer",
					Message:    "destination removed from streamer",
					Meta: shared.ChildStreamMeta{
						Role:      "destination",
						ChildID:   id,
						ChildType: existing.Type(),
						Managed:   true,
						ChannelID: s.channelID,
					},
				})
				delete(s.outputs, id)
			}
		}
	}()

	wg.Wait()

	return nil
}

func (s *Streamer) AddInput(i Stream) error {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	return s.upsertInputLocked(i)
}

// if it doesnt exist its no-op
func (s *Streamer) RemoveInput(streamID string) {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	s.removeInputLocked(streamID, nil, false)
}

func (s *Streamer) RemoveInputIfSame(streamID string, expected Stream) bool {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	return s.removeInputLocked(streamID, expected, true)
}

func (s *Streamer) AddOutput(o Stream) error {
	// TODO : fix these locks
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()
	return s.upsertOutputLocked(o)
}

func (s *Streamer) RemoveOutput(outputID string) {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	output, ok := s.outputs[outputID]
	if !ok {
		return
	}

	output.Stop()
	output.Close()

	s.emitEvent(shared.Event{
		Type:       shared.EventTypeDestinationRemoved,
		StreamID:   s.channelOrStreamID(),
		StreamType: "streamer",
		Message:    "destination removed from streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "destination",
			ChildID:   outputID,
			ChildType: output.Type(),
			Managed:   true,
			ChannelID: s.channelID,
		},
	})

	delete(s.outputs, outputID)
}

func (s *Streamer) StopOutput(outputID string) bool {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	output, ok := s.outputs[outputID]
	if !ok {
		return false
	}

	output.Stop()
	return true
}

func (s *Streamer) Stop() {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	s.IsStarted = false
	for _, o := range s.outputs {
		o.Stop()
	}
}

func (s *Streamer) Switch(inputID string) bool {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()
	_, ok := s.inputs[inputID]
	if !ok {
		return false
	}

	if inputID == s.stagedInputID {
		return true
	}

	prevInputID := s.activeInputID
	s.stagedInputID = inputID

	select {
	case s.SwitchChan <- inputID:
		s.emitEvent(shared.Event{
			Type:       shared.EventTypeInputSwitched,
			StreamID:   s.channelOrStreamID(),
			StreamType: "streamer",
			Message:    "input switch requested",
			Meta: shared.InputSwitchedMeta{
				PreviousInputID: prevInputID,
				CurrentInputID:  inputID,
			},
		})
	default:
		return false
	}

	return true
}

func (s *Streamer) Start() {
	s.outputsMu.Lock()
	defer s.outputsMu.Unlock()

	s.IsStarted = true
	logger := logger.GetLogger()
	for _, o := range s.outputs {
		o.Start()
		logger.Info("streamer: started output", zap.String("output_id", o.GetID()))
	}
	s.emitEvent(shared.Event{
		Type:       shared.EventTypeStreamStarted,
		StreamID:   s.channelOrStreamID(),
		StreamType: "streamer",
		Message:    "streamer started",
		Meta: shared.StreamLifecycleMeta{
			Restartable: false,
		},
	})
}

func (s *Streamer) startSwitcher() {
	for {
		select {
		case <-s.done:
			return
		case inputID := <-s.SwitchChan:
			s.switchInput(inputID)
		}
	}
}

func (s *Streamer) switchInput(inputID string) {
	s.inputsMu.Lock()
	defer s.inputsMu.Unlock()

	prevInputID := s.activeInputID
	if prevInputID != "" && prevInputID != inputID {
		if prevInput, ok := s.inputs[prevInputID]; ok && shouldPauseWhenInactive(prevInput) {
			prevInput.Stop()
		}
	}
	if nextInput, ok := s.inputs[inputID]; ok && shouldPauseWhenInactive(nextInput) {
		nextInput.Start()
	}
	s.activeInputID = inputID
}

func (s *Streamer) startAudio() {
l1:
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.inputsMu.RLock()
		activeInput, ok := s.inputs[s.activeInputID]
		s.inputsMu.RUnlock()

		if !ok {
			time.Sleep(5 * time.Millisecond)

			continue
		}

		var audioFrame *Frame

		select {
		case audioFrame, ok = <-activeInput.GetAudioChan():
			if !ok || audioFrame == nil {
				continue l1
			}

			// fmt.Println("audio frame : ", audioFrame.PTS, audioFrame.SequenceID)

			s.TotalAudioFrames++
		case <-time.After(5 * time.Millisecond):
			continue
		}

		select {
		case s.MultiCaster.GetAudioChan() <- audioFrame:
		case <-time.After(1000 * time.Millisecond):
			logger.GetLogger().Warn("streamer: dropped audio frame to multicaster",
				zap.String("stream_id", s.activeInputID),
				zap.Int64("sequence_id", audioFrame.SequenceID),
				zap.Duration("pts", audioFrame.PTS),
				zap.String("input_id", audioFrame.InputID))
			s.DroppedAudioFrames++
		}
	}
}

func (s *Streamer) startVideo() {
l1:
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.inputsMu.RLock()
		activeInput, ok := s.inputs[s.activeInputID]
		s.inputsMu.RUnlock()

		if !ok {
			time.Sleep(5 * time.Millisecond)

			continue
		}

		var videoframe *Frame

		select {
		case videoframe, ok = <-activeInput.GetVideoChan():
			if !ok || videoframe == nil {
				continue l1
			}

			// fmt.Println("video frame : ", videoframe.PTS, videoframe.SequenceID, videoframe.PacketType)

			s.TotalVideoFrames++
		case <-time.After(5 * time.Millisecond):
			continue
		}

		select {
		case s.MultiCaster.GetVideoChan() <- videoframe:
		case <-time.After(1000 * time.Millisecond):
			logger.GetLogger().Warn("streamer: dropped video frame to multicaster",
				zap.String("stream_id", s.activeInputID),
				zap.Int64("sequence_id", videoframe.SequenceID),
				zap.Duration("pts", videoframe.PTS),
				zap.String("input_id", videoframe.InputID),
				zap.Bool("is_keyframe", videoframe.IsKeyFrame))
			s.DroppedVideoFrames++
		}
	}
}

func (s *Streamer) StartLife() {
	if s.MultiCaster != nil {
		s.MultiCaster.Start()
	}

	go s.startVideo()
	go s.startAudio()
	go s.startSwitcher()
}

type StreamerState struct {
	IsStarted          bool    `json:"is_started"`
	IsResumable        bool    `json:"is_resumable"`
	CurrentInputID     string  `json:"current_input_id"`
	TotalAudioFrames   int64   `json:"total_audio_frames"`
	TotalVideoFrames   int64   `json:"total_video_frames"`
	DroppedAudioFrames float64 `json:"dropped_audio_frames"`
	DroppedVideoFrames float64 `json:"droppped_video_frames"`
	AudioSuccessRate   float64 `json:"audio_success_rate"`
	VideoSuccessRate   float64 `json:"video_success_rate"`

	StreamInputs  []*State             `json:"inputs"`
	StreamOutputs []*State             `json:"outputs"`
	ChannelHLS    ChannelPlaylistState `json:"channel_hls"`

	AvailableProgramHLSURLs []string `json:"available_program_hls_urls"`
	AvailableChannelHLSURLs []string `json:"available_channel_hls_urls"`
	ProgramRecordHLSURLs    []string `json:"program_record_hls_urls"`
	ChannelRecordHLSURLs    []string `json:"channel_record_hls_urls"`
}

type HLSSegmentRef struct {
	ProgramID       string  `json:"program_id"`
	URI             string  `json:"uri"`
	DurationLine    string  `json:"duration_line"`
	DurationSeconds float64 `json:"duration_seconds"`
	Discontinuity   bool    `json:"discontinuity"`
}

type ChannelPlaylistState struct {
	ActiveProgramID    string            `json:"active_program_id"`
	LastSegmentByInput map[string]string `json:"last_segment_by_input"`
	Segments           []HLSSegmentRef   `json:"segments"`
	MediaSequence      int               `json:"media_sequence"`
	LastPublishedKeys  []string          `json:"last_published_keys"`
	Playlist           string            `json:"playlist"`
}

// TODO : fix this
func (s *Streamer) State() StreamerState {
	s.inputsMu.RLock()
	streamInputs := make([]*State, 0, len(s.inputs))
	for _, val := range s.inputs {
		streamInputs = append(streamInputs, val.State())
	}
	activeInput := s.activeInputID
	s.inputsMu.RUnlock()

	s.outputsMu.RLock()
	streamOutputs := make([]*State, 0, len(s.outputs))
	for _, val := range s.outputs {
		streamOutputs = append(streamOutputs, val.State())
	}
	s.outputsMu.RUnlock()

	s.playlistMu.Lock()
	channelHLS := cloneChannelPlaylistState(s.playlistState)
	s.playlistMu.Unlock()

	availableProgramHLSURLs := s.availableProgramHLSURLs(streamInputs)
	availableChannelHLSURLs := s.availableChannelHLSURLs(s.IsStarted)
	programRecordHLSURLs := s.availableProgramRecordHLSURLs()
	channelRecordHLSURLs := s.availableChannelRecordHLSURLs()

	return StreamerState{
		IsStarted:               s.IsStarted,
		CurrentInputID:          activeInput,
		TotalAudioFrames:        s.TotalAudioFrames,
		TotalVideoFrames:        s.TotalVideoFrames,
		DroppedAudioFrames:      s.DroppedAudioFrames,
		DroppedVideoFrames:      s.DroppedVideoFrames,
		StreamInputs:            streamInputs,
		StreamOutputs:           streamOutputs,
		ChannelHLS:              channelHLS,
		AvailableProgramHLSURLs: availableProgramHLSURLs,
		AvailableChannelHLSURLs: availableChannelHLSURLs,
		ProgramRecordHLSURLs:    programRecordHLSURLs,
		ChannelRecordHLSURLs:    channelRecordHLSURLs,
	}
}

func (s *Streamer) availableProgramHLSURLs(streamInputs []*State) []string {
	programURLs := make([]string, 0, len(streamInputs))
	seen := make(map[string]struct{}, len(streamInputs))
	for _, in := range streamInputs {
		if in == nil {
			continue
		}
		playlistURL := s.inputLiveURL(in.StreamID)
		playlistURL = strings.TrimSpace(playlistURL)
		if playlistURL == "" {
			continue
		}
		if _, ok := seen[playlistURL]; ok {
			continue
		}
		seen[playlistURL] = struct{}{}
		programURLs = append(programURLs, playlistURL)
	}
	sort.Strings(programURLs)
	return programURLs
}

func (s *Streamer) inputLiveURL(inputID string) string {
	folder := s.inputHLSFolder(inputID)
	if folder == nil {
		return ""
	}
	playlistName := s.hlsPlaylistName()
	if _, err := folder.Stat(playlistName); err != nil {
		return ""
	}
	playlistURL := shared.PreferredURL("", folder, playlistName)
	if strings.TrimSpace(s.hlsConfig.PathPrefix) != "" {
		playlistURL = shared.JoinURLPrefix(s.programHLSPathPrefix(s.channelID, normalizeProgramID(s.channelID, inputID)), playlistName)
	}
	return strings.TrimSpace(playlistURL)
}

func (s *Streamer) availableChannelHLSURLs(isStarted bool) []string {
	if !isStarted {
		return []string{}
	}
	playlistName := s.hlsPlaylistName()
	if !s.HasChannelHLSFile(playlistName) {
		return []string{}
	}
	playlistURL := shared.PreferredURL("", s.channelLiveFolder, playlistName)
	if strings.TrimSpace(s.hlsConfig.PathPrefix) != "" {
		playlistURL = shared.JoinURLPrefix(s.channelHLSPathPrefix(s.channelID), playlistName)
	}
	playlistURL = strings.TrimSpace(playlistURL)
	if playlistURL == "" {
		return []string{}
	}
	return []string{playlistURL}
}

func (s *Streamer) availableProgramRecordHLSURLs() []string {
	mappedFolders := s.inputRecordFoldersSnapshot()
	discoveredFolders := s.programRecordFoldersFromRoot()
	recordURLs := make([]string, 0, len(mappedFolders)+len(discoveredFolders))
	seen := make(map[string]struct{}, len(mappedFolders)+len(discoveredFolders))

	collect := func(programID string, folder shared.Folder) {
		if folder == nil {
			return
		}
		playlistURL := latestRecorderPlaylistURL(folder, s.hlsPlaylistName(), s.programRecordPathPrefix(s.channelID, programID))
		if playlistURL == "" {
			return
		}
		if _, ok := seen[playlistURL]; ok {
			return
		}
		seen[playlistURL] = struct{}{}
		recordURLs = append(recordURLs, playlistURL)
	}

	for inputID, folder := range mappedFolders {
		collect(normalizeProgramID(s.channelID, inputID), folder)
	}
	for programID, folder := range discoveredFolders {
		collect(programID, folder)
	}

	sort.Strings(recordURLs)
	return recordURLs
}

func (s *Streamer) availableChannelRecordHLSURLs() []string {
	playlistURL := latestRecorderPlaylistURL(s.channelRecordFolder, s.hlsPlaylistName(), s.channelRecordPathPrefix(s.channelID))
	if playlistURL == "" {
		channelID := strings.TrimSpace(s.channelID)
		if s.recordRootFolder != nil && channelID != "" {
			playlistURL = latestRecorderPlaylistURL(s.recordRootFolder.Folder(channelID), s.hlsPlaylistName(), s.channelRecordPathPrefix(channelID))
		}
	}
	if playlistURL == "" {
		return []string{}
	}
	return []string{playlistURL}
}

func latestRecorderPlaylistURL(folder shared.Folder, playlistName, publicPrefix string) string {
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
		if sessionFolder == nil {
			continue
		}
		if _, err := sessionFolder.Stat(playlistName); err != nil {
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
		sessionPrefix = shared.JoinURLPrefix(publicPrefix, latestSession)
	}
	return strings.TrimSpace(shared.PreferredURL(sessionPrefix, folder.Folder(latestSession), playlistName))
}

func cloneChannelPlaylistState(state *ChannelPlaylistState) ChannelPlaylistState {
	if state == nil {
		return ChannelPlaylistState{
			LastSegmentByInput: make(map[string]string),
			Segments:           make([]HLSSegmentRef, 0),
			LastPublishedKeys:  make([]string, 0),
		}
	}

	cloned := ChannelPlaylistState{
		ActiveProgramID: state.ActiveProgramID,
		Playlist:        state.Playlist,
	}
	return cloned
}

func (s *Streamer) EnsureChannelHLSOutput() error {
	channelID := strings.TrimSpace(s.channelID)
	if channelID == "" {
		return errors.New("channel id is required")
	}

	if s.channelLiveFolder == nil {
		return errors.New("channel live folder is not configured")
	}
	if s.channelRecordFolder == nil {
		return errors.New("channel record folder is not configured")
	}

	state := s.State()
	outputIDs := make(map[string]struct{}, len(state.StreamOutputs))
	for _, out := range state.StreamOutputs {
		if out == nil {
			continue
		}
		outputIDs[out.StreamID] = struct{}{}
	}

	recorderID := fmt.Sprintf("record_%s", channelID)
	if _, ok := outputIDs[recorderID]; !ok {
		recorderOptions := make([]recorder.Option, 0, 3)
		recorderOptions = append(recorderOptions, recorder.WithSegmentDuration(s.recorderConfig.SegmentDuration))
		recorderOptions = append(recorderOptions, recorder.WithTargetDuration(s.recorderConfig.TargetDuration))
		recorderOptions = append(recorderOptions, recorder.WithPathPrefix(s.channelRecordPathPrefix(channelID)))
		recorderWatcher, err := recorder.New(recorderID, s.channelRecordFolder, recorderOptions...)
		if err != nil {
			return fmt.Errorf("create ingest recorder watcher: %w", err)
		}
		if err := s.AddOutput(recorderWatcher); err != nil {
			recorderWatcher.Close()
			return err
		}
	}

	outputID := fmt.Sprintf("%s-hls-output", channelID)
	if _, ok := outputIDs[outputID]; ok {
		return nil
	}

	channelHLSOutput, err := outputs.NewHLSLiveDestination(outputID, s.channelLiveFolder, s.channelHLSDestinationOptions(channelID)...)
	if err != nil {
		return err
	}

	if err := s.AddOutput(channelHLSOutput); err != nil {
		channelHLSOutput.Close()
		return err
	}
	return nil
}

func (s *Streamer) hlsDestinationOptions() []outputs.HLSLiveOption {
	options := make([]outputs.HLSLiveOption, 0, 3)
	options = append(options, outputs.WithHLSSegmentDuration(s.hlsConfig.SegmentDuration))
	options = append(options, outputs.WithHLSPlaylistSize(s.hlsConfig.PlaylistSize))
	options = append(options, outputs.WithHLSTargetDuration(s.hlsConfig.TargetDuration))
	return options
}

func (s *Streamer) programHLSDestinationOptions(channelID, programID string) []outputs.HLSLiveOption {
	options := s.hlsDestinationOptions()
	options = append(options, outputs.WithHLSPlaylistPathPrefix(s.programHLSPathPrefix(channelID, programID)))
	return options
}

func (s *Streamer) channelHLSDestinationOptions(channelID string) []outputs.HLSLiveOption {
	options := s.hlsDestinationOptions()
	options = append(options, outputs.WithHLSPlaylistPathPrefix(s.channelHLSPathPrefix(channelID)))
	return options
}

func (s *Streamer) hlsPathPrefix() string {
	prefix := strings.TrimSpace(s.hlsConfig.PathPrefix)
	if prefix == "" {
		return "/hls"
	}
	return shared.JoinURLPrefix(prefix)
}

func (s *Streamer) programHLSPathPrefix(channelID, programID string) string {
	return shared.JoinURLPrefix(s.hlsPathPrefix(), strings.TrimSpace(channelID), strings.TrimSpace(programID))
}

func (s *Streamer) channelHLSPathPrefix(channelID string) string {
	return shared.JoinURLPrefix(s.hlsPathPrefix(), strings.TrimSpace(channelID))
}

func (s *Streamer) recordPathPrefix() string {
	return strings.TrimSpace(s.recorderConfig.PathPrefix)
}

func (s *Streamer) programRecordPathPrefix(channelID, programID string) string {
	if s.recordPathPrefix() == "" {
		return ""
	}
	return shared.JoinURLPrefix(s.recordPathPrefix(), strings.TrimSpace(channelID), strings.TrimSpace(programID))
}

func (s *Streamer) channelRecordPathPrefix(channelID string) string {
	if s.recordPathPrefix() == "" {
		return ""
	}
	return shared.JoinURLPrefix(s.recordPathPrefix(), strings.TrimSpace(channelID))
}

func normalizeProgramID(channelID, inputID string) string {
	channelID = strings.TrimSpace(channelID)
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return ""
	}

	parts := strings.Split(inputID, "/")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 0 {
		return ""
	}
	if len(filtered) == 1 {
		return filtered[0]
	}
	if filtered[0] == channelID {
		return filtered[1]
	}
	return filtered[len(filtered)-1]
}

func JoinHLSPrefix(prefix string, suffixes ...string) string {
	return shared.JoinURLPrefix(prefix, suffixes...)
}

func (s *Streamer) ChannelHLSPlaylistState() string {
	return s.State().ChannelHLS.Playlist
}

func (s *Streamer) SetInputHLSFolder(inputID string, folder any) error {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return errors.New("input id is required")
	}
	adapted, err := shared.AdaptFolder(folder)
	if err != nil {
		return err
	}
	if adapted == nil {
		return errors.New("input hls folder is required")
	}
	s.inputHLSMu.Lock()
	s.inputHLSFolders[inputID] = adapted
	s.inputHLSMu.Unlock()
	return nil
}

func (s *Streamer) SetInputRecordFolder(inputID string, folder any) error {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return errors.New("input id is required")
	}
	adapted, err := shared.AdaptFolder(folder)
	if err != nil {
		return err
	}
	if adapted == nil {
		return errors.New("input record folder is required")
	}
	s.inputRecordMu.Lock()
	s.inputRecordFolders[inputID] = adapted
	s.inputRecordMu.Unlock()
	return nil
}

func (s *Streamer) SetRecordRootFolder(folder any) error {
	adapted, err := shared.AdaptFolder(folder)
	if err != nil {
		return err
	}
	if adapted == nil {
		return errors.New("record root folder is required")
	}
	s.recordRootFolder = adapted
	return nil
}

func (s *Streamer) InputHLSPlaylist(inputID, urlPrefix string) (string, error) {
	folder := s.inputHLSFolder(inputID)
	if folder == nil {
		return "", errors.New("input hls folder is not configured")
	}
	return s.rewrittenPlaylist(folder, s.hlsPlaylistName(), urlPrefix)
}

func (s *Streamer) ChannelHLSPlaylist(urlPrefix string) (string, error) {
	if s.channelLiveFolder == nil {
		return "", errors.New("channel hls folder is not configured")
	}
	return s.rewrittenPlaylist(s.channelLiveFolder, s.hlsPlaylistName(), urlPrefix)
}

func (s *Streamer) HasInputHLS(inputID string) bool {
	folder := s.inputHLSFolder(inputID)
	if folder == nil {
		return false
	}
	_, err := folder.Stat(s.hlsPlaylistName())
	return err == nil
}

func (s *Streamer) HasChannelHLSFile(reqFile string) bool {
	reqFile, err := sanitizeHLSRelativePath(reqFile)
	if err != nil {
		return false
	}
	if s.channelLiveFolder == nil {
		return false
	}
	_, err = s.channelLiveFolder.Stat(reqFile)
	return err == nil
}

func (s *Streamer) OpenChannelHLSFile(reqFile string) (io.ReadCloser, string, error) {
	reqFile, err := sanitizeHLSRelativePath(reqFile)
	if err != nil {
		return nil, "", err
	}
	return s.openHLSFile(s.channelLiveFolder, reqFile)
}

func (s *Streamer) OpenInputHLSFile(inputID, reqFile string) (io.ReadCloser, string, error) {
	folder := s.inputHLSFolder(inputID)
	if folder == nil {
		return nil, "", errors.New("input hls folder is not configured")
	}
	reqFile, err := sanitizeHLSRelativePath(reqFile)
	if err != nil {
		return nil, "", err
	}
	return s.openHLSFile(folder, reqFile)
}

func (s *Streamer) openHLSFile(folder shared.Folder, reqFile string) (io.ReadCloser, string, error) {
	if folder == nil {
		return nil, "", errors.New("hls folder is not configured")
	}
	if _, err := folder.Stat(s.hlsPlaylistName()); err != nil {
		return nil, "", err
	}
	if _, err := folder.Stat(reqFile); err != nil {
		return nil, "", err
	}
	contentType := ""
	if strings.HasSuffix(reqFile, ".m3u8") {
		contentType = "application/vnd.apple.mpegurl"
	} else if strings.HasSuffix(reqFile, ".ts") {
		contentType = "video/mp2t"
	}
	f, err := folder.Open(reqFile)
	if err != nil {
		return nil, "", err
	}
	return f, contentType, nil
}

func (s *Streamer) rewrittenPlaylist(folder shared.Folder, playlistName, urlPrefix string) (string, error) {
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

func (s *Streamer) inputHLSFolder(inputID string) shared.Folder {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return nil
	}
	s.inputHLSMu.RLock()
	defer s.inputHLSMu.RUnlock()
	return s.inputHLSFolders[inputID]
}

func (s *Streamer) inputRecordFolder(inputID string) shared.Folder {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return nil
	}
	s.inputRecordMu.RLock()
	defer s.inputRecordMu.RUnlock()
	return s.inputRecordFolders[inputID]
}

func (s *Streamer) inputRecordFoldersSnapshot() map[string]shared.Folder {
	s.inputRecordMu.RLock()
	defer s.inputRecordMu.RUnlock()

	folders := make(map[string]shared.Folder, len(s.inputRecordFolders))
	for inputID, folder := range s.inputRecordFolders {
		if folder != nil {
			folders[inputID] = folder
		}
	}
	return folders
}

func (s *Streamer) programRecordFoldersFromRoot() map[string]shared.Folder {
	channelID := strings.TrimSpace(s.channelID)
	if s.recordRootFolder == nil || channelID == "" {
		return nil
	}

	programsRoot := s.recordRootFolder.Folder(path.Join("inputs", channelID))
	if programsRoot == nil {
		return nil
	}

	entries, err := programsRoot.ReadDir()
	if err != nil {
		return nil
	}

	folders := make(map[string]shared.Folder, len(entries))
	for _, entry := range entries {
		if entry == nil || !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		folders[name] = programsRoot.Folder(name)
	}
	return folders
}

func RewriteHLSPlaylist(content, urlPrefix string) string {
	var b strings.Builder
	legacyPrefixes := legacyHLSRewritePrefixes(urlPrefix)
	for _, line := range strings.SplitAfter(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			switch {
			case strings.HasPrefix(trimmed, "http://"), strings.HasPrefix(trimmed, "https://"):
			case strings.HasPrefix(trimmed, "/"):
				if rewritten, ok := rewriteLegacyHLSURI(trimmed, urlPrefix, legacyPrefixes); ok {
					line = rewritten + trailingNewline(line)
				}
			default:
				line = shared.JoinURLPrefix(urlPrefix, strings.TrimLeft(trimmed, "/")) + trailingNewline(line)
			}
		}
		b.WriteString(line)
	}
	return b.String()
}

func rewriteLegacyHLSURI(uri, urlPrefix string, legacyPrefixes []string) (string, bool) {
	for _, prefix := range legacyPrefixes {
		if uri == prefix {
			return strings.TrimRight(urlPrefix, "/"), true
		}
		if strings.HasPrefix(uri, prefix+"/") {
			return shared.JoinURLPrefix(urlPrefix, strings.TrimPrefix(uri, prefix+"/")), true
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

	basePath = "/" + strings.Trim(strings.TrimSpace(basePath), "/")
	if basePath == "/" {
		return nil
	}

	parts := strings.Split(strings.Trim(basePath, "/"), "/")
	if len(parts) == 0 {
		return nil
	}

	suffix := strings.Join(parts, "/")
	return []string{
		shared.JoinURLPrefix("/hls", suffix),
		shared.JoinURLPrefix("/v1/restream/hls", suffix),
	}
}

func trailingNewline(line string) string {
	if strings.HasSuffix(line, "\n") {
		return "\n"
	}
	return ""
}

func (s *Streamer) hlsPlaylistName() string {
	name := strings.TrimSpace(s.hlsConfig.PlaylistName)
	if name == "" {
		return "stream.m3u8"
	}
	return name
}

func sanitizeHLSRelativePath(rel string) (string, error) {
	rel = strings.TrimSpace(strings.TrimPrefix(rel, "/"))
	if rel == "" {
		return "", fmt.Errorf("empty path")
	}
	clean := path.Clean(rel)
	if clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", fmt.Errorf("invalid relative path")
	}
	return clean, nil
}

func (s *Streamer) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.IsStarted = false

		if s.MultiCaster != nil {
			s.MultiCaster.Close()
		}

		for _, v := range s.inputs {
			v.Close()
		}

		for _, v := range s.outputs {
			v.Close()
		}

		s.emitEvent(shared.Event{
			Type:       shared.EventTypeStreamClosed,
			StreamID:   s.channelOrStreamID(),
			StreamType: "streamer",
			Message:    "streamer closed",
		})
		s.events.Close()
	})
}

func (s *Streamer) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *Streamer) AttachEventListener(listener EventListener) {
	if listener == nil {
		return
	}
	s.listener = listener
	s.listener.Watch(s)
}

func (s *Streamer) emitEvent(event shared.Event) {
	if s.events == nil {
		return
	}
	if event.StreamID == "" {
		event.StreamID = s.channelOrStreamID()
	}
	if event.StreamType == "" {
		event.StreamType = "streamer"
	}
	s.events.Emit(event)
}

func (s *Streamer) EmitEvent(event shared.Event) {
	s.emitEvent(event)
}

func (s *Streamer) watchStream(stream Stream) {
	if s.listener == nil || stream == nil {
		return
	}
	s.listener.Watch(stream)
}

func (s *Streamer) channelOrStreamID() string {
	if strings.TrimSpace(s.channelID) != "" {
		return strings.TrimSpace(s.channelID)
	}
	return "streamer"
}

func hashStruct(v any) (string, error) {
	// Convert struct to a deterministic JSON representation
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	// Compute hash
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h[:]), nil
}

func (s *Streamer) upsertInputLocked(newInput Stream) error {
	if newInput == nil {
		return errors.New("nil input is not accepted")
	}

	oldInput, exists := s.inputs[newInput.GetID()]
	if exists {
		oldHash, _ := hashStruct(oldInput.State().Url)
		newHash, _ := hashStruct(newInput.State().Url)
		if newHash == oldHash {
			return nil
		}

		logger.GetLogger().Info("streamer: input updated", zap.String("input_id", newInput.GetID()))
		managed := Manage(newInput)
		if s.shouldStartInputLocked(managed, newInput.GetID()) {
			managed.Start()
		}
		s.inputs[newInput.GetID()] = managed
		s.watchStream(managed)
		s.emitEvent(shared.Event{
			Type:       shared.EventTypeInputAdded,
			StreamID:   s.channelOrStreamID(),
			StreamType: "streamer",
			Message:    "input updated in streamer",
			Meta: shared.ChildStreamMeta{
				Role:      "input",
				ChildID:   newInput.GetID(),
				ChildType: newInput.Type(),
				ChildURL:  newInput.State().Url,
				Managed:   true,
				Replaced:  true,
				ChannelID: s.channelID,
			},
		})
		oldInput.Close()
		return nil
	}

	logger.GetLogger().Info("streamer: input added", zap.String("input_id", newInput.GetID()))
	managed := Manage(newInput)
	if s.shouldStartInputLocked(managed, newInput.GetID()) {
		managed.Start()
	}
	s.inputs[newInput.GetID()] = managed
	s.watchStream(managed)
	s.emitEvent(shared.Event{
		Type:       shared.EventTypeInputAdded,
		StreamID:   s.channelOrStreamID(),
		StreamType: "streamer",
		Message:    "input added to streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "input",
			ChildID:   newInput.GetID(),
			ChildType: newInput.Type(),
			ChildURL:  newInput.State().Url,
			Managed:   true,
			ChannelID: s.channelID,
		},
	})
	return nil
}

func (s *Streamer) shouldStartInputLocked(stream Stream, inputID string) bool {
	if !shouldPauseWhenInactive(stream) {
		return true
	}
	return inputID == s.activeInputID || inputID == s.stagedInputID
}

func shouldPauseWhenInactive(stream Stream) bool {
	if stream == nil {
		return false
	}
	capable, ok := stream.(pauseWhenInactiveCapable)
	return ok && capable.ShouldPauseWhenInactive()
}

func (s *Streamer) upsertOutputLocked(newOutput Stream) error {
	if newOutput == nil {
		return errors.New("nil output is not accepted")
	}

	oldOutput, exists := s.outputs[newOutput.GetID()]
	if exists {
		oldHash, _ := hashStruct(oldOutput.State().Url)
		newHash, _ := hashStruct(newOutput.State().Url)
		if newHash == oldHash {
			return nil
		}

		logger.GetLogger().Info("streamer: output updated", zap.String("output_id", newOutput.GetID()))
		managed := Manage(newOutput)
		managed.Start()
		s.outputs[newOutput.GetID()] = managed
		s.watchStream(managed)
		s.emitEvent(shared.Event{
			Type:       shared.EventTypeDestinationAdded,
			StreamID:   s.channelOrStreamID(),
			StreamType: "streamer",
			Message:    "destination updated in streamer",
			Meta: shared.ChildStreamMeta{
				Role:      "destination",
				ChildID:   newOutput.GetID(),
				ChildType: newOutput.Type(),
				ChildURL:  newOutput.State().Url,
				Managed:   true,
				Replaced:  true,
				ChannelID: s.channelID,
			},
		})
		oldOutput.Close()
		return nil
	}

	logger.GetLogger().Info("streamer: output added", zap.String("output_id", newOutput.GetID()))
	managed := Manage(newOutput)
	managed.Start()
	s.outputs[newOutput.GetID()] = managed
	s.watchStream(managed)
	s.emitEvent(shared.Event{
		Type:       shared.EventTypeDestinationAdded,
		StreamID:   s.channelOrStreamID(),
		StreamType: "streamer",
		Message:    "destination added to streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "destination",
			ChildID:   newOutput.GetID(),
			ChildType: newOutput.Type(),
			ChildURL:  newOutput.State().Url,
			Managed:   true,
			ChannelID: s.channelID,
		},
	})
	return nil
}

func (s *Streamer) removeInputLocked(streamID string, expected Stream, matchExpected bool) bool {
	i, ok := s.inputs[streamID]
	if !ok {
		return false
	}
	if matchExpected && i != expected {
		return false
	}

	i.Stop()
	i.Close()

	if s.activeInputID == streamID {
		s.activeInputID = ""
	}
	if s.stagedInputID == streamID {
		s.stagedInputID = ""
	}

	s.emitEvent(shared.Event{
		Type:       shared.EventTypeInputRemoved,
		StreamID:   s.channelOrStreamID(),
		StreamType: "streamer",
		Message:    "input removed from streamer",
		Meta: shared.ChildStreamMeta{
			Role:      "input",
			ChildID:   streamID,
			ChildType: i.Type(),
			Managed:   true,
			ChannelID: s.channelID,
		},
	})

	delete(s.inputs, streamID)
	s.inputHLSMu.Lock()
	delete(s.inputHLSFolders, streamID)
	s.inputHLSMu.Unlock()
	s.inputRecordMu.Lock()
	delete(s.inputRecordFolders, streamID)
	s.inputRecordMu.Unlock()
	return true
}
