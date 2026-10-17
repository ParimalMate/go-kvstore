package gossip

import (
	"context"
	"math/rand"
	"time"
)

// OnChange replaces the optional transition observer. It runs without table or
// service locks. Hooks must return promptly (enqueue future repair work) and
// must not call Stop synchronously: Stop waits for the notifying worker too.
func (s *Service) OnChange(hook func(Transition)) {
	s.mu.Lock()
	s.onChange = hook
	s.mu.Unlock()
}

// Start is safe to call repeatedly. A stopped Service is not restartable;
// a new process incarnation gets a new Table and Service.
func (s *Service) Start(parent context.Context) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.stopped || s.cancel != nil || len(s.seeds) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	// Keep one count for the loop until it can no longer launch exchanges. This
	// makes concurrent Add calls safe while Stop is waiting for all workers.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if ctx.Err() != nil {
			return
		}
		s.round(ctx)
		ticker := time.NewTicker(s.table.timing.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				s.round(ctx)
			}
		}
	}()
}

func (s *Service) round(ctx context.Context) {
	s.table.Beat()
	s.refresh()
	seed, ok := s.table.SelectPeer(s.seeds, rand.Intn)
	if !ok || ctx.Err() != nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = s.exchange(ctx, seed)
	}()
}

// Stop cancels requests as well as the ticker and waits for all loop workers.
// It is safe before Start, after cancellation, and from concurrent callers.
func (s *Service) Stop() {
	s.lifecycleMu.Lock()
	s.stopped = true
	if s.cancel != nil {
		s.cancel()
	}
	s.lifecycleMu.Unlock()
	s.wg.Wait()
}
