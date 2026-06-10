package timeline

import (
	"context"
	"sync"
	"time"
)

type Scheduler struct {
	now func() time.Time

	updateCh chan Plan
	done     chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		now:      func() time.Time { return time.Now().UTC() },
		updateCh: make(chan Plan, 1),
		done:     make(chan struct{}),
	}
}

func (s *Scheduler) Start(emit func(Event)) {
	s.startOnce.Do(func() {
		go s.loop(emit)
	})
}

func (s *Scheduler) ReplacePlan(plan Plan) {
	select {
	case s.updateCh <- plan:
	default:
		select {
		case <-s.updateCh:
		default:
		}
		s.updateCh <- plan
	}
}

func (s *Scheduler) Stop() {
	s.closeOnce.Do(func() {
		close(s.done)
	})
}

func (s *Scheduler) loop(emit func(Event)) {
	var (
		current Plan
		index   int
		timer   *time.Timer
		timerCh <-chan time.Time
	)

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerCh = nil
	}

	resetForCurrent := func() {
		stopTimer()
		events := current.NormalizedEvents()
		for index < len(events) {
			now := s.now()
			if events[index].At.After(now) {
				timer = time.NewTimer(events[index].At.Sub(now))
				timerCh = timer.C
				return
			}
			emit(events[index])
			index++
		}
	}

	for {
		select {
		case <-s.done:
			stopTimer()
			return
		case plan := <-s.updateCh:
			current = plan
			index = 0
			resetForCurrent()
		case <-timerCh:
			events := current.NormalizedEvents()
			if index < len(events) {
				emit(events[index])
				index++
			}
			resetForCurrent()
		}
	}
}

func (s *Scheduler) Run(ctx context.Context, plan Plan, emit func(Event)) {
	if ctx == nil {
		return
	}
	s.Start(emit)
	s.ReplacePlan(plan)
	go func() {
		<-ctx.Done()
		s.Stop()
	}()
}
