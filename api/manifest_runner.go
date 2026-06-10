package api

import (
	"fmt"
	"sync"

	irajstreamer "github.com/tupicapp/restreamer/core"
	manifestpkg "github.com/tupicapp/restreamer/core/manifest"
)

type RunnerService struct {
	mu      sync.RWMutex
	runners map[string]irajstreamer.ManifestRunner
}

func NewRunnerService() *RunnerService {
	return &RunnerService{
		runners: map[string]irajstreamer.ManifestRunner{},
	}
}

func (s *RunnerService) Create(id string, manifest manifestpkg.Manifest) (irajstreamer.ManifestRunner, error) {
	id = normalizeChannelID(id)
	if manifest.ChannelID == "" {
		manifest.ChannelID = id
	}
	if manifest.ChannelID != id {
		return nil, fmt.Errorf("manifest.channel_id %q does not match runner id %q", manifest.ChannelID, id)
	}

	runner, err := irajstreamer.NewManifestRunner(manifest)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runners[id]; exists {
		return nil, fmt.Errorf("runner %q already exists", id)
	}
	s.runners[id] = runner
	return runner, nil
}

func (s *RunnerService) Apply(id string, manifest manifestpkg.Manifest) (irajstreamer.ManifestRunner, error) {
	id = normalizeChannelID(id)
	if manifest.ChannelID == "" {
		manifest.ChannelID = id
	}
	if manifest.ChannelID != id {
		return nil, fmt.Errorf("manifest.channel_id %q does not match runner id %q", manifest.ChannelID, id)
	}

	runner, ok := s.Get(id)
	if ok {
		if err := runner.UpdateManifest(manifest); err != nil {
			return nil, err
		}
		if err := runner.Start(); err != nil {
			return nil, err
		}
		return runner, nil
	}

	created, err := s.Create(id, manifest)
	if err != nil {
		return nil, err
	}
	if err := created.Start(); err != nil {
		_ = created.Close()
		s.mu.Lock()
		delete(s.runners, id)
		s.mu.Unlock()
		return nil, err
	}
	return created, nil
}

func (s *RunnerService) Get(id string) (irajstreamer.ManifestRunner, bool) {
	id = normalizeChannelID(id)
	s.mu.RLock()
	defer s.mu.RUnlock()
	runner, ok := s.runners[id]
	return runner, ok
}

func (s *RunnerService) Update(id string, manifest manifestpkg.Manifest) error {
	id = normalizeChannelID(id)
	if manifest.ChannelID == "" {
		manifest.ChannelID = id
	}
	if manifest.ChannelID != id {
		return fmt.Errorf("manifest.channel_id %q does not match runner id %q", manifest.ChannelID, id)
	}
	runner, ok := s.Get(id)
	if !ok {
		return fmt.Errorf("runner %q not found", id)
	}
	return runner.UpdateManifest(manifest)
}

func (s *RunnerService) Start(id string) error {
	id = normalizeChannelID(id)
	runner, ok := s.Get(id)
	if !ok {
		return fmt.Errorf("runner %q not found", id)
	}
	return runner.Start()
}

func (s *RunnerService) Stop(id string) error {
	id = normalizeChannelID(id)
	runner, ok := s.Get(id)
	if !ok {
		return fmt.Errorf("runner %q not found", id)
	}
	return runner.Stop()
}

func (s *RunnerService) Close(id string) error {
	id = normalizeChannelID(id)
	s.mu.Lock()
	runner, ok := s.runners[id]
	if ok {
		delete(s.runners, id)
	}
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("runner %q not found", id)
	}
	return runner.Close()
}
