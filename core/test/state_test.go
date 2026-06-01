package test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corehelpers "github.com/tupicapp/restreamer/core"
	"github.com/tupicapp/restreamer/core/storage"
)

type stateMockStream struct {
	id         string
	url        string
	streamType string

	videoChan chan *Frame
	audioChan chan *Frame
	events    chan Event

	mu        sync.Mutex
	started   bool
	stopped   int
	closed    int
	cloned    int
	lastIO    time.Time
	resumable bool
}

func newStateMockStream(id, url, streamType string) *stateMockStream {
	return &stateMockStream{
		id:         id,
		url:        url,
		streamType: streamType,
		videoChan:  make(chan *Frame, 1),
		audioChan:  make(chan *Frame, 1),
		events:     make(chan Event, 8),
		lastIO:     time.Now(),
		resumable:  true,
	}
}

func (m *stateMockStream) GetVideoChan() chan *Frame { return m.videoChan }
func (m *stateMockStream) GetAudioChan() chan *Frame { return m.audioChan }
func (m *stateMockStream) GetID() string             { return m.id }
func (m *stateMockStream) EventChan() chan Event     { return m.events }
func (m *stateMockStream) Type() string              { return m.streamType }
func (m *stateMockStream) IsRestartable() bool       { return false }
func (m *stateMockStream) RestartInterval() time.Duration {
	return time.Second
}

func (m *stateMockStream) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = true
}

func (m *stateMockStream) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = false
	m.stopped++
}

func (m *stateMockStream) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = false
	m.closed++
}

func (m *stateMockStream) State() *State {
	return &State{
		IsStarted:   m.started,
		IsResumable: m.resumable,
		LastIO:      m.lastIO,
		StreamID:    m.id,
		Type:        m.streamType,
		Url:         m.url,
	}
}

func (m *stateMockStream) Clone() (Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cloned++
	return newStateMockStream(m.id, m.url, m.streamType), nil
}

func (m *stateMockStream) WaitForStart(context.Context) error { return nil }

func TestStreamer_StateLifecycle_MixedInputs(t *testing.T) {
	streamer := corehelpers.NewStreamer(corehelpers.WithStreamerID("state-ch"))
	defer streamer.Close()
	streamer.StartLife()

	output := newStateMockStream("state-out", "rtmp://state/out", "rtmp-output")
	if err := streamer.AddOutput(output); err != nil {
		t.Fatalf("AddOutput() error = %v", err)
	}
	streamer.Start()

	inputs := []struct {
		id  string
		url string
		typ string
	}{
		{id: "hls", url: "http://source/hls/stream.m3u8", typ: "hls"},
		{id: "hls-live", url: "http://source/hls-live/stream.m3u8", typ: "hlslive"},
		{id: "rtmp-av", url: "rtmp://source/live/av", typ: "rtmp"},
		{id: "rtmp-audio-less", url: "rtmp://source/live/audio-less", typ: "rtmp"},
		{id: "rtmp-video-less", url: "rtmp://source/live/video-less", typ: "rtmp"},
	}

	for _, in := range inputs {
		if err := streamer.AddInput(newStateMockStream(in.id, in.url, in.typ)); err != nil {
			t.Fatalf("AddInput(%q) error = %v", in.id, err)
		}
		configureStateTestFolders(t, streamer, in.id)
	}

	state := streamer.State()
	if got, want := len(state.StreamInputs), len(inputs); got != want {
		t.Fatalf("input count mismatch: got=%d want=%d", got, want)
	}
	assertStateHasInputIDs(t, state, []string{"hls", "hls-live", "rtmp-av", "rtmp-audio-less", "rtmp-video-less"})
	assertURLListHasIDs(t, state.AvailableInputHLSURLs, []string{"hls", "hls-live", "rtmp-av", "rtmp-audio-less", "rtmp-video-less"})
	assertURLListHasIDs(t, state.InputRecordHLSURLs, []string{"hls", "hls-live", "rtmp-av", "rtmp-audio-less", "rtmp-video-less"})

	switchOrder := []string{"hls", "hls-live", "rtmp-av", "rtmp-audio-less", "rtmp-video-less"}
	for _, inputID := range switchOrder {
		if ok := streamer.Switch(inputID); !ok {
			t.Fatalf("Switch(%q) returned false", inputID)
		}
		waitForCurrentInputState(t, streamer, inputID, 2*time.Second)
	}

	beforeFailedSwitch := streamer.State().CurrentInputID
	if ok := streamer.Switch("missing-input"); ok {
		t.Fatal("Switch(missing-input) returned true")
	}
	waitForCurrentInputState(t, streamer, beforeFailedSwitch, 500*time.Millisecond)

	remaining := []string{"hls", "hls-live", "rtmp-av", "rtmp-audio-less", "rtmp-video-less"}
	for _, removeID := range []string{"rtmp-video-less", "rtmp-audio-less", "rtmp-av", "hls-live", "hls"} {
		if ok := streamer.Switch(removeID); !ok {
			t.Fatalf("Switch(%q) before remove returned false", removeID)
		}
		waitForCurrentInputState(t, streamer, removeID, 2*time.Second)

		streamer.RemoveInput(removeID)
		waitForCurrentInputState(t, streamer, "", 2*time.Second)

		remaining = removeIDFromList(remaining, removeID)
		cur := streamer.State()
		assertStateHasInputIDs(t, cur, remaining)
		assertURLListHasIDs(t, cur.AvailableInputHLSURLs, remaining)
		assertURLListHasIDs(t, cur.InputRecordHLSURLs, remaining)
	}

	final := streamer.State()
	if final.CurrentInputID != "" {
		t.Fatalf("expected empty CurrentInputID after all removals, got %q", final.CurrentInputID)
	}
	if len(final.StreamInputs) != 0 {
		t.Fatalf("expected no inputs after removals, got %d", len(final.StreamInputs))
	}

	streamer.Close()
	closedState := streamer.State()
	if closedState.IsStarted {
		t.Fatalf("expected streamer state not started after close, got IsStarted=%v", closedState.IsStarted)
	}
	if len(closedState.StreamOutputs) != 1 {
		t.Fatalf("expected output state to remain observable, got %d outputs", len(closedState.StreamOutputs))
	}
	if closedState.StreamOutputs[0].IsStarted {
		t.Fatal("expected output started=false after close")
	}
}

func TestStreamer_StateInputUpsertCases(t *testing.T) {
	streamer := corehelpers.NewStreamer()
	defer streamer.Close()

	original := newStateMockStream("upsert-input", "rtmp://source/live/a", "rtmp")
	if err := streamer.AddInput(original); err != nil {
		t.Fatalf("AddInput(original) error = %v", err)
	}

	sameIDSameURL := newStateMockStream("upsert-input", "rtmp://source/live/a", "rtmp")
	if err := streamer.AddInput(sameIDSameURL); err != nil {
		t.Fatalf("AddInput(same id+url) error = %v", err)
	}
	state := streamer.State()
	if got := len(state.StreamInputs); got != 1 {
		t.Fatalf("same id+url should keep one input, got %d", got)
	}
	if got := original.closed; got != 0 {
		t.Fatalf("same id+url should not close old input, got closed=%d", got)
	}

	sameIDDifferentURL := newStateMockStream("upsert-input", "rtmp://source/live/b", "rtmp")
	if err := streamer.AddInput(sameIDDifferentURL); err != nil {
		t.Fatalf("AddInput(same id+different url) error = %v", err)
	}
	state = streamer.State()
	if got := len(state.StreamInputs); got != 1 {
		t.Fatalf("same id+different url should replace in place, got %d entries", got)
	}
	inputState := findInputState(state, "upsert-input")
	if inputState == nil {
		t.Fatal("expected upsert-input state to exist after replace")
	}
	if inputState.Url != "rtmp://source/live/b" {
		t.Fatalf("expected replaced url to be %q, got %q", "rtmp://source/live/b", inputState.Url)
	}
	if got := original.closed; got != 1 {
		t.Fatalf("replace should close old input once, got closed=%d", got)
	}

	streamer.RemoveInput("does-not-exist")
	if got := len(streamer.State().StreamInputs); got != 1 {
		t.Fatalf("RemoveInput on missing id should be no-op, got inputs=%d", got)
	}
}

func TestStreamer_State_MultiInputsWithoutSwitch_ProgramURLsPresent(t *testing.T) {
	streamer := corehelpers.NewStreamer(corehelpers.WithStreamerID("state-no-switch"))
	defer streamer.Close()
	streamer.StartLife()

	inputIDs := []string{"rtmp-av", "rtmp-audio-less", "rtmp-video-less"}
	for _, inputID := range inputIDs {
		if err := streamer.AddInput(newStateMockStream(inputID, "rtmp://source/live/"+inputID, "rtmp")); err != nil {
			t.Fatalf("AddInput(%q) error = %v", inputID, err)
		}
		configureStateTestFolders(t, streamer, inputID)
	}

	state := streamer.State()
	if state.CurrentInputID != "" {
		t.Fatalf("expected CurrentInputID to stay empty when no switch happened, got %q", state.CurrentInputID)
	}
	assertStateHasInputIDs(t, state, inputIDs)
	assertURLListHasIDs(t, state.AvailableInputHLSURLs, inputIDs)
	assertURLListHasIDs(t, state.InputRecordHLSURLs, inputIDs)
}

func TestStreamer_UpdateStreams_RemovesDetachedInputFolders(t *testing.T) {
	streamer := corehelpers.NewStreamer(corehelpers.WithStreamerID("state-update"))
	defer streamer.Close()

	inputA := newStateMockStream("input-a", "rtmp://source/live/input-a", "rtmp")
	inputB := newStateMockStream("input-b", "rtmp://source/live/input-b", "rtmp")
	if err := streamer.UpdateStreams([]Stream{inputA, inputB}, nil); err != nil {
		t.Fatalf("UpdateStreams(add) error = %v", err)
	}
	configureStateTestFolders(t, streamer, inputA.GetID())
	configureStateTestFolders(t, streamer, inputB.GetID())

	initial := streamer.State()
	assertURLListHasIDs(t, initial.AvailableInputHLSURLs, []string{"input-a", "input-b"})
	assertURLListHasIDs(t, initial.InputRecordHLSURLs, []string{"input-a", "input-b"})

	replacementA := newStateMockStream("input-a", "rtmp://source/live/input-a-2", "rtmp")
	if err := streamer.UpdateStreams([]Stream{replacementA}, nil); err != nil {
		t.Fatalf("UpdateStreams(remove input-b) error = %v", err)
	}

	state := streamer.State()
	assertStateHasInputIDs(t, state, []string{"input-a"})
	assertURLListHasIDs(t, state.AvailableInputHLSURLs, []string{"input-a"})
	assertURLListHasIDs(t, state.InputRecordHLSURLs, []string{"input-a"})
}

func configureStateTestFolders(t *testing.T, streamer *corehelpers.Streamer, inputID string) {
	t.Helper()

	liveDir := filepath.Join(t.TempDir(), "live-"+inputID)
	if err := os.MkdirAll(liveDir, 0755); err != nil {
		t.Fatalf("MkdirAll(liveDir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(liveDir, "stream.m3u8"), []byte("#EXTM3U\n"), 0644); err != nil {
		t.Fatalf("WriteFile(live playlist) error = %v", err)
	}
	if err := streamer.SetInputHLSFolder(inputID, storage.NewFolder(liveDir)); err != nil {
		t.Fatalf("SetInputHLSFolder(%q) error = %v", inputID, err)
	}

	recordDir := filepath.Join(t.TempDir(), "record-"+inputID)
	sessionDir := filepath.Join(recordDir, "0001")
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("MkdirAll(record session) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "stream.m3u8"), []byte("#EXTM3U\n"), 0644); err != nil {
		t.Fatalf("WriteFile(record playlist) error = %v", err)
	}
	if err := streamer.SetInputRecordFolder(inputID, storage.NewFolder(recordDir)); err != nil {
		t.Fatalf("SetInputRecordFolder(%q) error = %v", inputID, err)
	}
}

func waitForCurrentInputState(t *testing.T, streamer *corehelpers.Streamer, expected string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if streamer.State().CurrentInputID == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("CurrentInputID mismatch: got=%q want=%q", streamer.State().CurrentInputID, expected)
}

func assertStateHasInputIDs(t *testing.T, state corehelpers.StreamerState, expectedIDs []string) {
	t.Helper()

	gotIDs := make([]string, 0, len(state.StreamInputs))
	for _, input := range state.StreamInputs {
		if input == nil {
			continue
		}
		gotIDs = append(gotIDs, input.StreamID)
	}
	slices.Sort(gotIDs)
	want := append([]string(nil), expectedIDs...)
	slices.Sort(want)
	if !slices.Equal(gotIDs, want) {
		t.Fatalf("input ids mismatch: got=%v want=%v", gotIDs, want)
	}
}

func assertURLListHasIDs(t *testing.T, urls []string, expectedIDs []string) {
	t.Helper()

	if got, want := len(urls), len(expectedIDs); got != want {
		t.Fatalf("url count mismatch: got=%d want=%d urls=%v", got, want, urls)
	}
	for _, id := range expectedIDs {
		found := false
		for _, u := range urls {
			if strings.Contains(u, id) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected a URL containing input id %q, urls=%v", id, urls)
		}
	}
}

func removeIDFromList(values []string, removeID string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == removeID {
			continue
		}
		out = append(out, v)
	}
	return out
}

func findInputState(state corehelpers.StreamerState, inputID string) *State {
	for _, input := range state.StreamInputs {
		if input != nil && input.StreamID == inputID {
			return input
		}
	}
	return nil
}
