package irajstreamer

import "io"

func (s *Streamer) SetOutputLiveFolder(folder any) error {
	return s.hlsFolders.SetOutputLiveFolder(folder)
}

func (s *Streamer) SetOutputRecordFolder(folder any) error {
	return s.hlsFolders.SetOutputRecordFolder(folder)
}

func (s *Streamer) SetInputHLSFolder(inputID string, folder any) error {
	return s.hlsFolders.SetInputHLSFolder(inputID, folder)
}

func (s *Streamer) SetInputRecordFolder(inputID string, folder any) error {
	return s.hlsFolders.SetInputRecordFolder(inputID, folder)
}

func (s *Streamer) SetInputRecordRootFolder(folder any) error {
	return s.hlsFolders.SetInputRecordRootFolder(folder)
}

func (s *Streamer) SetOutputRecordRootFolder(folder any) error {
	return s.hlsFolders.SetOutputRecordRootFolder(folder)
}

func (s *Streamer) InputHLSPlaylist(inputID, urlPrefix string) (string, error) {
	return s.hlsFolders.InputHLSPlaylist(inputID, s.hlsPlaylistName(), urlPrefix)
}

func (s *Streamer) OutputHLSPlaylist(urlPrefix string) (string, error) {
	return s.hlsFolders.OutputHLSPlaylist(s.hlsPlaylistName(), urlPrefix)
}

func (s *Streamer) HasInputHLS(inputID string) bool {
	return s.hlsFolders.HasInputHLS(inputID, s.hlsPlaylistName())
}

func (s *Streamer) HasOutputHLSFile(reqFile string) bool {
	return s.hlsFolders.HasOutputHLSFile(reqFile, s.hlsPlaylistName())
}

func (s *Streamer) OpenOutputHLSFile(reqFile string) (io.ReadCloser, string, error) {
	return s.hlsFolders.OpenOutputHLSFile(reqFile, s.hlsPlaylistName())
}

func (s *Streamer) OpenInputHLSFile(inputID, reqFile string) (io.ReadCloser, string, error) {
	return s.hlsFolders.OpenInputHLSFile(inputID, reqFile, s.hlsPlaylistName())
}

func (s *Streamer) availableInputHLSURLs(inputIDs []string) []string {
	return s.hlsFolders.AvailableInputHLSURLs(inputIDs, s.hlsPlaylistName(), s.hlsConfig.PathPrefix)
}

func (s *Streamer) availableOutputHLSURLs() []string {
	return s.hlsFolders.AvailableOutputHLSURLs(s.IsStarted, s.hlsPlaylistName(), s.hlsConfig.PathPrefix)
}

func (s *Streamer) availableInputRecordHLSURLs(inputIDs []string) []string {
	return s.hlsFolders.InputRecordHLSURLs(inputIDs, s.hlsPlaylistName(), s.recorderConfig.PathPrefix)
}

func (s *Streamer) availableOutputRecordHLSURLs() []string {
	return s.hlsFolders.OutputRecordHLSURLs(s.hlsPlaylistName(), s.recorderConfig.PathPrefix)
}

func inputIDsFromStates(states []*State) []string {
	inputIDs := make([]string, 0, len(states))
	for _, state := range states {
		if state == nil {
			continue
		}
		inputIDs = append(inputIDs, state.StreamID)
	}
	return inputIDs
}
