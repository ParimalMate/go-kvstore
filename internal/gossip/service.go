package gossip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// Service owns gossip transport and logging. It never participates in quorum
// decisions. The supplied table provides the clock and immutable node identity.
type Service struct {
	table       *Table
	selfID      string
	generation  int64
	seeds       []string
	logger      *log.Logger
	client      *http.Client
	timeout     time.Duration
	mu          sync.Mutex
	reachable   map[string]bool
	aliases     map[string]bool
	conflicts   map[int64]time.Time
	onChange    func(Transition)
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	stopped     bool
	wg          sync.WaitGroup
}

func NewService(table *Table, logger *log.Logger, seeds []string) (*Service, error) {
	if table == nil || logger == nil {
		return nil, fmt.Errorf("gossip table and logger are required")
	}
	timeout := table.timing.Interval
	if timeout < 200*time.Millisecond {
		timeout = 200 * time.Millisecond
	}
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	table.mu.Lock()
	generation := table.entries[table.selfID].generation
	table.mu.Unlock()
	return &Service{
		table: table, selfID: table.selfID, generation: generation,
		seeds: append([]string(nil), seeds...), logger: logger, timeout: timeout,
		client:    &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		reachable: make(map[string]bool), aliases: make(map[string]bool), conflicts: make(map[int64]time.Time),
	}, nil
}

func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/gossip", s.HandleExchange)
	mux.HandleFunc("GET /internal/members", s.HandleMembers)
}

// decodeMessage consumes the entire bounded body, rejecting trailing JSON and
// oversized whitespace as well as malformed messages, before any table mutation.
func decodeMessage(body []byte) (GossipMessage, error) {
	var message GossipMessage
	if err := json.Unmarshal(body, &message); err != nil {
		return message, err
	}
	if err := message.Validate(); err != nil {
		return message, err
	}
	// Every sender includes self in its digest. Requiring it makes identity
	// checks possible rather than guessing whether a same-ID response is an alias.
	for _, member := range message.Members {
		if member.ID == message.From {
			return message, nil
		}
	}
	return message, fmt.Errorf("sender must include its own member entry")
}

func (s *Service) HandleExchange(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			http.Error(w, "gossip body exceeds 64 KiB", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "cannot read gossip body", http.StatusBadRequest)
		}
		return
	}
	message, err := decodeMessage(body)
	if err != nil {
		http.Error(w, "invalid gossip message: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.inspectIdentity(message)
	s.merge(message.Members)
	s.writeJSON(w, GossipMessage{From: s.selfID, Members: s.table.Digest()})
}

type MembersResponse struct {
	Self           string       `json:"self"`
	Generation     int64        `json:"generation"`
	Members        []MemberView `json:"members"`
	UnreachedSeeds []string     `json:"unreached_seeds"`
}

func (s *Service) HandleMembers(w http.ResponseWriter, r *http.Request) {
	s.refresh()
	view := s.table.View(s.seeds)
	// Preserve sorted order for other nodes while putting self first.
	for i, member := range view.Members {
		if member.ID == s.selfID {
			copy(view.Members[1:i+1], view.Members[:i])
			view.Members[0] = member
			break
		}
	}
	s.writeJSON(w, MembersResponse{s.selfID, s.generation, view.Members, view.UnreachedSeeds})
}

func (s *Service) writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		s.logger.Printf("gossip: response write failed: %v", err)
	}
}

func (s *Service) merge(members []WireMember) {
	changes := s.table.Merge(members)
	for _, id := range changes.NewIDs {
		s.logger.Printf("gossip: learned %s via gossip", id)
	}
	for _, restart := range changes.Restarted {
		s.logger.Printf("gossip: %s restarted (generation %d -> %d)", restart.ID, restart.OldGeneration, restart.NewGeneration)
	}
	// Heartbeat advances are normal traffic; logging each would flood the logs.
	s.refresh()
}

func (s *Service) refresh() {
	transitions := s.table.Refresh()
	if len(transitions) == 0 {
		return
	}
	view := s.table.View(nil)
	members := make(map[string]MemberView, len(view.Members))
	for _, member := range view.Members {
		members[member.ID] = member
	}
	for _, transition := range transitions {
		member := members[transition.ID]
		reason := fmt.Sprintf("no heartbeat for %s", time.Duration(member.LastChangeMSAgo)*time.Millisecond)
		if transition.To == Alive {
			reason = "heartbeat resumed"
		}
		s.logger.Printf("gossip: %s %s -> %s (%s, gen=%d, hb=%d)", transition.ID, transition.From, transition.To, reason, member.Generation, member.Heartbeat)
		s.mu.Lock()
		hook := s.onChange
		s.mu.Unlock()
		if hook != nil {
			hook(transition)
		}
	}
}

// inspectIdentity detects relayed higher self generations, and both directions
// of a direct duplicate-ID exchange. It never changes the local generation.
func (s *Service) inspectIdentity(message GossipMessage) {
	for _, member := range message.Members {
		if member.ID == s.selfID && (member.Generation > s.generation || (message.From == s.selfID && member.Generation != s.generation)) {
			s.logConflict(member.Generation)
		}
	}
}

func (s *Service) logConflict(generation int64) {
	now := s.table.now()
	s.mu.Lock()
	previous, seen := s.conflicts[generation]
	report := !seen || now.Sub(previous) >= 30*time.Second
	if report {
		s.conflicts[generation] = now
	}
	s.mu.Unlock()
	if report {
		s.logger.Printf("gossip: ID CONFLICT: peer reports gen=%d for my ID %s (local gen=%d)", generation, s.selfID, s.generation)
	}
}

func (s *Service) seedResult(seed string, err error) {
	s.mu.Lock()
	wasReachable, known := s.reachable[seed]
	reachable := err == nil
	s.reachable[seed] = reachable
	reportFailure := !reachable && (!known || wasReachable)
	reportRecovery := reachable && known && !wasReachable
	s.mu.Unlock()
	if reportFailure {
		s.logger.Printf("gossip: seed %s unreachable: %v", seed, err)
	}
	if reportRecovery {
		s.logger.Printf("gossip: seed %s reachable again", seed)
	}
}
