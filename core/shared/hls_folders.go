package shared

import (
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
)

type HLSFolders struct {
	outputLive       Folder
	outputRecord     Folder
	inputRecordRoot  Folder
	outputRecordRoot Folder

	inputLiveMu   sync.RWMutex
	inputLive     map[string]Folder
	inputRecordMu sync.RWMutex
	inputRecord   map[string]Folder
}

func NewHLSFolders() *HLSFolders {
	return &HLSFolders{
		inputLive:   make(map[string]Folder),
		inputRecord: make(map[string]Folder),
	}
}

func (h *HLSFolders) SetOutputLiveFolder(folder any) error {
	adapted, err := adaptFolder(folder)
	if err != nil {
		return err
	}
	h.outputLive = adapted
	return nil
}

func (h *HLSFolders) SetOutputRecordFolder(folder any) error {
	adapted, err := adaptFolder(folder)
	if err != nil {
		return err
	}
	h.outputRecord = adapted
	return nil
}

func (h *HLSFolders) SetInputRecordRootFolder(folder any) error {
	adapted, err := adaptRequiredFolder(folder, "input record root folder is required")
	if err != nil {
		return err
	}
	h.inputRecordRoot = adapted
	return nil
}

func (h *HLSFolders) SetOutputRecordRootFolder(folder any) error {
	adapted, err := adaptRequiredFolder(folder, "output record root folder is required")
	if err != nil {
		return err
	}
	h.outputRecordRoot = adapted
	return nil
}

func (h *HLSFolders) SetInputHLSFolder(inputID string, folder any) error {
	adapted, err := adaptRequiredFolder(folder, "input hls folder is required")
	if err != nil {
		return err
	}
	inputID, err = requiredInputID(inputID)
	if err != nil {
		return err
	}

	h.inputLiveMu.Lock()
	h.inputLive[inputID] = adapted
	h.inputLiveMu.Unlock()
	return nil
}

func (h *HLSFolders) SetInputRecordFolder(inputID string, folder any) error {
	adapted, err := adaptRequiredFolder(folder, "input record folder is required")
	if err != nil {
		return err
	}
	inputID, err = requiredInputID(inputID)
	if err != nil {
		return err
	}

	h.inputRecordMu.Lock()
	h.inputRecord[inputID] = adapted
	h.inputRecordMu.Unlock()
	return nil
}

func (h *HLSFolders) RemoveInput(inputID string) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return
	}

	h.inputLiveMu.Lock()
	delete(h.inputLive, inputID)
	h.inputLiveMu.Unlock()

	h.inputRecordMu.Lock()
	delete(h.inputRecord, inputID)
	h.inputRecordMu.Unlock()
}

func (h *HLSFolders) AvailableInputHLSURLs(inputIDs []string, playlistName, pathPrefix string) []string {
	urls := make([]string, 0, len(inputIDs))
	seen := make(map[string]struct{}, len(inputIDs))

	for _, inputID := range inputIDs {
		url := strings.TrimSpace(h.inputLiveURL(inputID, playlistName, pathPrefix))
		if url == "" {
			continue
		}
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}

	sort.Strings(urls)
	return urls
}

func (h *HLSFolders) AvailableOutputHLSURLs(isStarted bool, playlistName, pathPrefix string) []string {
	if !isStarted {
		return nil
	}

	url := strings.TrimSpace(playlistURL(h.outputLive, playlistName, outputHLSPathPrefix(pathPrefix)))
	if url == "" {
		return nil
	}
	return []string{url}
}

func (h *HLSFolders) InputRecordHLSURLs(inputIDs []string, playlistName, pathPrefix string) []string {
	urls := make([]string, 0, len(inputIDs))
	seen := make(map[string]struct{}, len(inputIDs))
	mapped := h.snapshotInputRecordFolders()
	discovered := childFolders(h.inputRecordRoot)

	for _, inputID := range inputIDs {
		url := latestRecorderPlaylistURL(mapped[inputID], playlistName, inputRecordPathPrefix(pathPrefix, inputID))
		if url == "" {
			url = latestRecorderPlaylistURL(discovered[inputID], playlistName, inputRecordPathPrefix(pathPrefix, inputID))
		}
		if url == "" {
			continue
		}
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}

	sort.Strings(urls)
	return urls
}

func (h *HLSFolders) OutputRecordHLSURLs(playlistName, pathPrefix string) []string {
	url := latestRecorderPlaylistURL(h.outputRecord, playlistName, outputRecordPathPrefix(pathPrefix))
	if url == "" {
		url = latestRecorderPlaylistURL(h.outputRecordRoot, playlistName, outputRecordPathPrefix(pathPrefix))
	}
	if url == "" {
		return nil
	}
	return []string{url}
}

func (h *HLSFolders) InputHLSPlaylist(inputID, playlistName, urlPrefix string) (string, error) {
	folder := h.inputLiveFolder(inputID)
	if folder == nil {
		return "", errors.New("input hls folder is not configured")
	}
	return rewrittenPlaylist(folder, playlistName, urlPrefix)
}

func (h *HLSFolders) OutputHLSPlaylist(playlistName, urlPrefix string) (string, error) {
	if h.outputLive == nil {
		return "", errors.New("output hls folder is not configured")
	}
	return rewrittenPlaylist(h.outputLive, playlistName, urlPrefix)
}

func (h *HLSFolders) HasInputHLS(inputID, playlistName string) bool {
	return hasFile(h.inputLiveFolder(inputID), playlistName)
}

func (h *HLSFolders) HasOutputHLSFile(reqFile, playlistName string) bool {
	reqFile, err := sanitizeHLSRelativePath(reqFile)
	if err != nil {
		return false
	}
	return hasFile(h.outputLive, playlistName) && hasFile(h.outputLive, reqFile)
}

func (h *HLSFolders) OpenOutputHLSFile(reqFile, playlistName string) (io.ReadCloser, string, error) {
	reqFile, err := sanitizeHLSRelativePath(reqFile)
	if err != nil {
		return nil, "", err
	}
	return openHLSFile(h.outputLive, playlistName, reqFile)
}

func (h *HLSFolders) OpenInputHLSFile(inputID, reqFile, playlistName string) (io.ReadCloser, string, error) {
	reqFile, err := sanitizeHLSRelativePath(reqFile)
	if err != nil {
		return nil, "", err
	}

	folder := h.inputLiveFolder(inputID)
	if folder == nil {
		return nil, "", errors.New("input hls folder is not configured")
	}
	return openHLSFile(folder, playlistName, reqFile)
}

func (h *HLSFolders) inputLiveURL(inputID, playlistName, pathPrefix string) string {
	return playlistURL(h.inputLiveFolder(inputID), playlistName, inputHLSPathPrefix(pathPrefix, inputID))
}

func (h *HLSFolders) inputLiveFolder(inputID string) Folder {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return nil
	}

	h.inputLiveMu.RLock()
	defer h.inputLiveMu.RUnlock()
	return h.inputLive[inputID]
}

func (h *HLSFolders) snapshotInputRecordFolders() map[string]Folder {
	h.inputRecordMu.RLock()
	defer h.inputRecordMu.RUnlock()

	out := make(map[string]Folder, len(h.inputRecord))
	for inputID, folder := range h.inputRecord {
		if folder != nil {
			out[inputID] = folder
		}
	}
	return out
}

func requiredInputID(inputID string) (string, error) {
	inputID = strings.TrimSpace(inputID)
	if inputID == "" {
		return "", errors.New("input id is required")
	}
	return inputID, nil
}

func adaptFolder(folder any) (Folder, error) {
	return AdaptFolder(folder)
}

func adaptRequiredFolder(folder any, nilMessage string) (Folder, error) {
	adapted, err := adaptFolder(folder)
	if err != nil {
		return nil, err
	}
	if adapted == nil {
		return nil, errors.New(nilMessage)
	}
	return adapted, nil
}

func hasFile(folder Folder, fileName string) bool {
	if folder == nil {
		return false
	}
	_, err := folder.Stat(fileName)
	return err == nil
}

func playlistURL(folder Folder, playlistName, prefix string) string {
	if !hasFile(folder, playlistName) {
		return ""
	}
	if strings.TrimSpace(prefix) == "" {
		return strings.TrimSpace(PreferredURL("", folder, playlistName))
	}
	return strings.TrimSpace(JoinURLPrefix(prefix, playlistName))
}

func childFolders(root Folder) map[string]Folder {
	if root == nil {
		return nil
	}

	entries, err := root.ReadDir()
	if err != nil {
		return nil
	}

	out := make(map[string]Folder, len(entries))
	for _, entry := range entries {
		if entry == nil || !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		out[name] = root.Folder(name)
	}
	return out
}
