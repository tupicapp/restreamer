package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	irajstreamer "github.com/tupicapp/restreamer/core"
	manifestpkg "github.com/tupicapp/restreamer/core/manifest"
)

type Server struct {
	runners   *RunnerService
	studios   *StudioRegistry
	workspace *WorkspaceService
	server    *http.Server
	hlsRoot   string
}

func NewServer(addr string) *Server {
	s := &Server{
		runners: NewRunnerService(),
		studios: NewStudioRegistry(),
		hlsRoot: irajstreamer.DefaultTimelineHLSRoot(),
	}
	s.workspace = NewWorkspaceService(s.studios, s.runners)
	_ = os.MkdirAll(s.hlsRoot, 0o755)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.Handle("/hls/", http.StripPrefix("/hls/", http.FileServer(http.Dir(s.hlsRoot))))
	mux.HandleFunc("/api/channels/", s.handleChannels)
	mux.HandleFunc("/api/studio/state", s.handleStudioState)
	mux.HandleFunc("/api/studio/select", s.handleStudioSelect)
	mux.HandleFunc("/api/studio/open", s.handleStudioOpen)
	mux.HandleFunc("/api/studio/stop", s.handleStudioStop)
	mux.HandleFunc("/api/studio/timeline-items", s.handleStudioTimelineItems)
	mux.HandleFunc("/api/runners/", s.handleRunner)

	s.server = &http.Server{
		Addr:              addr,
		Handler:           requestLogMiddleware(corsMiddleware(mux)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) ListenAndServe() error {
	if s == nil || s.server == nil {
		return fmt.Errorf("api server is not initialized")
	}
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStudioState(w http.ResponseWriter, r *http.Request) {
	s.handleStudioStateForChannel(w, r, "default")
}

func (s *Server) handleStudioStateForChannel(w http.ResponseWriter, r *http.Request, channelID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, s.studios.ForChannel(channelID).State())
}

func (s *Server) handleStudioSelect(w http.ResponseWriter, r *http.Request) {
	s.handleStudioSelectForChannel(w, r, "default")
}

func (s *Server) handleStudioSelectForChannel(w http.ResponseWriter, r *http.Request, channelID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	var input SelectStreamInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	state, err := s.studios.ForChannel(channelID).SelectStream(strings.TrimSpace(input.StreamID))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleStudioOpen(w http.ResponseWriter, r *http.Request) {
	s.handleStudioOpenForChannel(w, r, "default")
}

func (s *Server) handleStudioOpenForChannel(w http.ResponseWriter, r *http.Request, channelID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	var input OpenStreamInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	state, err := s.studios.ForChannel(channelID).OpenStream(strings.TrimSpace(input.StreamID))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleStudioStop(w http.ResponseWriter, r *http.Request) {
	s.handleStudioStopForChannel(w, r, "default")
}

func (s *Server) handleStudioStopForChannel(w http.ResponseWriter, r *http.Request, channelID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if _, ok := s.runners.Get(channelID); ok {
		if err := s.runners.Stop(channelID); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, s.studios.ForChannel(channelID).StopBroadcast())
}

func (s *Server) handleStudioTimelineItems(w http.ResponseWriter, r *http.Request) {
	s.handleStudioTimelineItemsForChannel(w, r, "default")
}

func (s *Server) handleStudioTimelineItemsForChannel(w http.ResponseWriter, r *http.Request, channelID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	var input AddTimelineItemInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	state, err := s.studios.ForChannel(channelID).AddTimelineItem(input)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/channels/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		writeError(w, http.StatusNotFound, fmt.Errorf("channel route is incomplete"))
		return
	}

	channelID := normalizeChannelID(parts[0])
	section := parts[1]
	action := ""
	if len(parts) > 2 {
		action = parts[2]
	}

	switch {
	case section == "workspace":
		s.handleWorkspaceForChannel(w, r, channelID)
	case section == "manifest":
		s.handleManifestForChannel(w, r, channelID)
	case section == "studio" && action == "state":
		s.handleStudioStateForChannel(w, r, channelID)
	case section == "studio" && action == "select":
		s.handleStudioSelectForChannel(w, r, channelID)
	case section == "studio" && action == "open":
		s.handleStudioOpenForChannel(w, r, channelID)
	case section == "studio" && action == "stop":
		s.handleStudioStopForChannel(w, r, channelID)
	case section == "studio" && action == "timeline-items":
		s.handleStudioTimelineItemsForChannel(w, r, channelID)
	case section == "runners":
		s.handleRunnerForChannel(w, r, channelID, parts[2:])
	default:
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown channel route"))
	}
}

func (s *Server) handleWorkspaceForChannel(w http.ResponseWriter, r *http.Request, channelID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, s.externalizeWorkspaceState(r, s.workspace.State(channelID)))
}

func (s *Server) handleManifestForChannel(w http.ResponseWriter, r *http.Request, channelID string) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.externalizeWorkspaceState(r, s.workspace.State(channelID)))
	case http.MethodPut:
		var manifest manifestpkg.Manifest
		if err := decodeJSON(r, &manifest); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		state, err := s.workspace.UpdateManifest(channelID, manifest)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, s.externalizeWorkspaceState(r, state))
	default:
		writeMethodNotAllowed(w)
	}
}

func (s *Server) handleRunner(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/runners/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, fmt.Errorf("runner id is required"))
		return
	}
	s.handleRunnerForChannel(w, r, parts[0], parts[1:])
}

func (s *Server) handleRunnerForChannel(w http.ResponseWriter, r *http.Request, channelID string, parts []string) {
	id := normalizeChannelID(channelID)

	switch {
	case len(parts) == 0 && r.Method == http.MethodPost:
		var manifest manifestpkg.Manifest
		if err := decodeJSON(r, &manifest); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		runner, err := s.runners.Create(id, manifest)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusCreated, s.externalizeManifestRunnerState(r, runner.State()))
	case len(parts) == 0 && r.Method == http.MethodDelete:
		if err := s.runners.Close(id); err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "closed"})
	case len(parts) == 1 && parts[0] == "manifest" && r.Method == http.MethodPut:
		var manifest manifestpkg.Manifest
		if err := decodeJSON(r, &manifest); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := s.runners.Update(id, manifest); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		runner, _ := s.runners.Get(id)
		writeJSON(w, http.StatusOK, s.externalizeManifestRunnerState(r, runner.State()))
	case len(parts) == 1 && parts[0] == "start" && r.Method == http.MethodPost:
		if err := s.runners.Start(id); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		runner, _ := s.runners.Get(id)
		writeJSON(w, http.StatusOK, s.externalizeManifestRunnerState(r, runner.State()))
	case len(parts) == 1 && parts[0] == "stop" && r.Method == http.MethodPost:
		if err := s.runners.Stop(id); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		runner, _ := s.runners.Get(id)
		writeJSON(w, http.StatusOK, s.externalizeManifestRunnerState(r, runner.State()))
	default:
		writeMethodNotAllowed(w)
	}
}

func (s *Server) externalizeWorkspaceState(r *http.Request, state WorkspaceState) WorkspaceState {
	if state.Runner == nil {
		return state
	}
	baseURL := requestBaseURL(r)
	runner := *state.Runner
	runner.PreviewURL = absolutizeURL(baseURL, runner.PreviewURL)
	runner.CurrentInputURL = absolutizeURL(baseURL, runner.CurrentInputURL)
	runner.Outputs = externalizeWorkspaceOutputs(baseURL, runner.Outputs)
	runner.StreamerState = externalizeStreamerState(baseURL, runner.StreamerState)
	state.Runner = &runner
	return state
}

func (s *Server) externalizeManifestRunnerState(r *http.Request, state irajstreamer.ManifestRunnerState) irajstreamer.ManifestRunnerState {
	baseURL := requestBaseURL(r)
	state.PreviewURL = absolutizeURL(baseURL, state.PreviewURL)
	state.CurrentInputURL = absolutizeURL(baseURL, state.CurrentInputURL)
	state.Outputs = externalizeRunnerOutputs(baseURL, state.Outputs)
	state.StreamerState = externalizeStreamerState(baseURL, state.StreamerState)
	return state
}

func externalizeWorkspaceOutputs(baseURL string, outputs []WorkspaceOutputState) []WorkspaceOutputState {
	if len(outputs) == 0 {
		return outputs
	}
	cloned := make([]WorkspaceOutputState, 0, len(outputs))
	for _, output := range outputs {
		next := output
		next.URL = absolutizeURL(baseURL, output.URL)
		cloned = append(cloned, next)
	}
	return cloned
}

func externalizeRunnerOutputs(baseURL string, outputs []irajstreamer.RunnerOutput) []irajstreamer.RunnerOutput {
	if len(outputs) == 0 {
		return outputs
	}
	cloned := make([]irajstreamer.RunnerOutput, 0, len(outputs))
	for _, output := range outputs {
		next := output
		next.URL = absolutizeURL(baseURL, output.URL)
		cloned = append(cloned, next)
	}
	return cloned
}

func externalizeStreamerState(baseURL string, state irajstreamer.StreamerState) irajstreamer.StreamerState {
	cloned := state
	cloned.StreamInputs = cloneAndExternalizeStates(baseURL, state.StreamInputs)
	cloned.StreamOutputs = cloneAndExternalizeStates(baseURL, state.StreamOutputs)
	return cloned
}

func cloneAndExternalizeStates(baseURL string, states []*irajstreamer.State) []*irajstreamer.State {
	if len(states) == 0 {
		return []*irajstreamer.State{}
	}
	cloned := make([]*irajstreamer.State, 0, len(states))
	for _, state := range states {
		if state == nil {
			cloned = append(cloned, nil)
			continue
		}
		next := *state
		next.Url = absolutizeURL(baseURL, state.Url)
		if len(state.Served) > 0 {
			next.Served = make([]irajstreamer.ServedState, 0, len(state.Served))
			for _, served := range state.Served {
				nextServed := served
				nextServed.Url = absolutizeURL(baseURL, served.Url)
				next.Served = append(next.Served, nextServed)
			}
		} else {
			next.Served = nil
		}
		cloned = append(cloned, &next)
	}
	return cloned
}

func requestBaseURL(r *http.Request) string {
	if r == nil {
		return "http://127.0.0.1:8080"
	}
	scheme := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = strings.TrimSpace(r.Host)
	}
	if host == "" {
		host = "127.0.0.1:8080"
	}
	return scheme + "://" + host
}

func absolutizeURL(baseURL, raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return trimmed
	}
	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return trimmed
	}
	parsedRef, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	return parsedBase.ResolveReference(parsedRef).String()
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func requestLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, recorder.status, time.Since(start).Round(time.Millisecond))
	})
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
}
