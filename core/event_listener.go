package irajstreamer

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	shared "github.com/tupicapp/restreamer/core/shared"
)

type Queue interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Close() error
}

type EventListener interface {
	Start() error
	Close() error
	Watch(shared.EventSource)
}

type queueEventListener struct {
	queue         Queue
	subjectPrefix string
	publishTTL    time.Duration

	done      chan struct{}
	closeOnce sync.Once

	watchMu sync.Mutex
	watched map[chan shared.Event]struct{}
}

type queueEvent struct {
	Type       shared.EventType `json:"type"`
	Time       time.Time        `json:"time"`
	StreamID   string           `json:"stream_id,omitempty"`
	StreamType string           `json:"stream_type,omitempty"`
	Message    string           `json:"message,omitempty"`
	Error      string           `json:"error,omitempty"`
	Meta       any              `json:"meta,omitempty"`
}

func NewEventListener(queue Queue, subjectPrefix string) EventListener {
	subjectPrefix = strings.TrimSpace(subjectPrefix)
	if subjectPrefix == "" {
		subjectPrefix = "restreamer.events"
	}
	return &queueEventListener{
		queue:         queue,
		subjectPrefix: subjectPrefix,
		publishTTL:    5 * time.Second,
		done:          make(chan struct{}),
		watched:       make(map[chan shared.Event]struct{}),
	}
}

func (l *queueEventListener) Start() error {
	return nil
}

func (l *queueEventListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		close(l.done)
		if l.queue != nil {
			err = l.queue.Close()
		}
	})
	return err
}

func (l *queueEventListener) Watch(source shared.EventSource) {
	if source == nil {
		return
	}
	ch := source.EventChan()
	if ch == nil {
		return
	}

	l.watchMu.Lock()
	if _, ok := l.watched[ch]; ok {
		l.watchMu.Unlock()
		return
	}
	l.watched[ch] = struct{}{}
	l.watchMu.Unlock()

	go l.consume(ch)
}

func (l *queueEventListener) consume(ch chan shared.Event) {
	for {
		select {
		case <-l.done:
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			if event.Type == shared.EventTypeSegmentGenerated {
				continue
			}
			l.publish(event)
		}
	}
}

func (l *queueEventListener) publish(event shared.Event) {
	if l.queue == nil {
		return
	}

	outbound := queueEvent{
		Type:       event.Type,
		Time:       event.Time,
		StreamID:   event.StreamID,
		StreamType: event.StreamType,
		Message:    event.Message,
		Meta:       event.Meta,
	}
	if event.Error != nil {
		outbound.Error = event.Error.Error()
	}

	payload, err := json.Marshal(outbound)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.publishTTL)
	defer cancel()

	_ = l.queue.Publish(ctx, l.subject(string(event.Type)), payload)
}

func (l *queueEventListener) subject(eventType string) string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return l.subjectPrefix
	}
	return l.subjectPrefix + "." + eventType
}
