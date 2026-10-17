package antientropy

import (
	"context"
	"errors"
	"fmt"
	"kvstore/internal/gossip"
	"math/rand"
	"sync"
	"time"
)

// Membership supplies local observations only; explicit repair ignores status.
type Membership interface {
	PeerByAddr(string) (string, gossip.Status, bool)
	AddrOf(string) (string, bool)
	IsSelfAlias(string) bool
	OnChange(func(gossip.Transition))
}

type Options struct {
	Peers        []string
	Gossip       Membership
	Interval     time.Duration
	InitialDelay time.Duration
}

type Totals struct {
	Cycles          uint64 `json:"cycles"`
	KeysPulled      uint64 `json:"keys_pulled"`
	KeysPushed      uint64 `json:"keys_pushed"`
	VersionsApplied uint64 `json:"versions_applied"`
	Collisions      uint64 `json:"collisions"`
	Errors          uint64 `json:"errors"`
}

type Status struct {
	Totals      Totals            `json:"totals"`
	LastReports map[string]Report `json:"last_reports"`
}

type work struct {
	peer   string
	done   chan struct{}
	report Report
}
type workerState struct {
	mu      sync.Mutex
	options Options
	queue   chan *work
	pending map[string]*work
	ctx     context.Context
	cancel  context.CancelFunc
	stopped bool
	wg      sync.WaitGroup
	status  Status
}

var ErrQueueFull = errors.New("anti-entropy queue is full")
var ErrStopped = errors.New("anti-entropy worker is not running")

// Start is idempotent. Even with scheduling disabled, the worker serves manual runs.
func (s *Service) Start(parent context.Context) {
	s.worker.mu.Lock()
	defer s.worker.mu.Unlock()
	if s.worker.cancel != nil || s.worker.stopped {
		return
	}
	s.worker.ctx, s.worker.cancel = context.WithCancel(parent)
	s.worker.wg.Add(1)
	go s.runWorker()
	o := s.worker.options
	if o.Interval == 0 || len(o.Peers) == 0 {
		return
	}
	if o.Gossip != nil {
		o.Gossip.OnChange(s.onTransition)
	}
	s.worker.wg.Add(1)
	go s.runTimer()
}

// Stop cancels HTTP requests before waiting. It is safe to call repeatedly.
func (s *Service) Stop() {
	s.worker.mu.Lock()
	s.worker.stopped = true
	if s.worker.cancel != nil {
		s.worker.cancel()
	}
	s.worker.mu.Unlock()
	s.worker.wg.Wait()
}

func (s *Service) enqueue(peer string) (*work, error) {
	s.worker.mu.Lock()
	defer s.worker.mu.Unlock()
	if s.worker.cancel == nil || s.worker.stopped || s.worker.ctx.Err() != nil {
		return nil, ErrStopped
	}
	if existing := s.worker.pending[peer]; existing != nil {
		return existing, nil
	}
	item := &work{peer: peer, done: make(chan struct{})}
	select {
	case s.worker.queue <- item:
		s.worker.pending[peer] = item
		return item, nil
	default:
		return nil, ErrQueueFull
	}
}

func (s *Service) runWorker() {
	defer s.worker.wg.Done()
	defer func() {
		s.worker.mu.Lock()
		defer s.worker.mu.Unlock()
		for peer, item := range s.worker.pending {
			item.report = Report{Peer: peer, Error: context.Canceled.Error()}
			close(item.done)
			delete(s.worker.pending, peer)
		}
	}()
	for {
		select {
		case <-s.worker.ctx.Done():
			return
		case item := <-s.worker.queue:
			if s.worker.ctx.Err() != nil {
				return
			}
			report, _ := s.SyncWith(s.worker.ctx, item.peer)
			s.worker.mu.Lock()
			item.report = report
			delete(s.worker.pending, item.peer)
			close(item.done)
			s.worker.mu.Unlock()
		}
	}
}

func (s *Service) eligiblePeers() []string {
	o := s.worker.options
	peers := make([]string, 0, len(o.Peers))
	seen := make(map[string]bool)
	for _, peer := range o.Peers {
		if peer == "" || seen[peer] {
			continue
		}
		seen[peer] = true
		if o.Gossip != nil {
			if o.Gossip.IsSelfAlias(peer) {
				continue
			}
			_, status, known := o.Gossip.PeerByAddr(peer)
			if known && (status == gossip.Suspect || status == gossip.Dead) {
				continue
			}
		}
		peers = append(peers, peer)
	}
	return peers
}

func (s *Service) onTransition(t gossip.Transition) {
	if t.From != gossip.Dead || t.To != gossip.Alive {
		return
	}
	if peer, ok := s.worker.options.Gossip.AddrOf(t.ID); ok {
		// Enqueue never waits for queue space or for a cycle to finish.
		_, _ = s.enqueue(peer)
	}
}

func (s *Service) runTimer() {
	defer s.worker.wg.Done()
	timer := time.NewTimer(s.worker.options.InitialDelay)
	defer timer.Stop()
	for {
		select {
		case <-s.worker.ctx.Done():
			return
		case <-timer.C:
			peers := s.eligiblePeers()
			if len(peers) > 0 {
				_, _ = s.enqueue(peers[rand.Intn(len(peers))])
			}
			timer.Reset(time.Duration(float64(s.worker.options.Interval) * (0.8 + rand.Float64()*0.4)))
		}
	}
}

func (s *Service) recordReport(r Report) {
	s.worker.mu.Lock()
	defer s.worker.mu.Unlock()
	t := &s.worker.status.Totals
	t.Cycles++
	t.KeysPulled += uint64(r.KeysPulled)
	t.KeysPushed += uint64(r.KeysPushed)
	t.VersionsApplied += uint64(r.VersionsApplied)
	t.Collisions += uint64(r.Collisions)
	if r.Error != "" {
		t.Errors++
	}
	s.worker.status.LastReports[r.Peer] = r
}

func (s *Service) Status() Status {
	s.worker.mu.Lock()
	defer s.worker.mu.Unlock()
	result := Status{Totals: s.worker.status.Totals, LastReports: make(map[string]Report)}
	for peer, r := range s.worker.status.LastReports {
		result.LastReports[peer] = r
	}
	return result
}

func newWorker(options []Options) (*workerState, error) {
	if len(options) > 1 {
		return nil, fmt.Errorf("at most one options value is allowed")
	}
	var o Options
	if len(options) == 1 {
		o = options[0]
	}
	if o.Interval < 0 || (o.Interval > 0 && (o.Interval < time.Second || o.Interval > time.Hour)) {
		return nil, fmt.Errorf("interval must be 0 or 1s..1h")
	}
	if o.InitialDelay < 0 || (o.Interval > 0 && o.InitialDelay > o.Interval) {
		return nil, fmt.Errorf("invalid initial delay")
	}
	o.Peers = append([]string(nil), o.Peers...)
	return &workerState{options: o, queue: make(chan *work, 16), pending: make(map[string]*work), status: Status{LastReports: make(map[string]Report)}}, nil
}
