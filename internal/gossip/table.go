package gossip

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Status string

const (
	Alive   Status = "alive"
	Suspect Status = "suspect"
	Dead    Status = "dead"
)

type Timing struct {
	Interval     time.Duration
	SuspectAfter time.Duration
	DeadAfter    time.Duration
}

func (t Timing) Validate() error {
	if t.Interval < 10*time.Millisecond || t.Interval > time.Minute {
		return fmt.Errorf("gossip interval must be between 10ms and 1m")
	}
	if t.SuspectAfter < 3*t.Interval {
		return fmt.Errorf("suspect threshold must be at least three intervals")
	}
	if t.DeadAfter <= t.SuspectAfter {
		return fmt.Errorf("dead threshold must exceed suspect threshold")
	}
	return nil
}

type entry struct {
	generation, heartbeat int64
	lastChange            time.Time
	status                Status
	via                   string
}

type Table struct {
	mu          sync.Mutex
	selfID      string
	entries     map[string]*entry
	direct      map[string]string // Dialed address -> last responding node ID; never gossiped.
	selfAliases map[string]bool
	timing      Timing
	now         func() time.Time
}

func NewTable(selfID string, generation int64, timing Timing, now func() time.Time) (*Table, error) {
	if err := (WireMember{ID: selfID, Generation: generation}).Validate(); err != nil {
		return nil, err
	}
	if err := timing.Validate(); err != nil {
		return nil, err
	}
	if now == nil {
		return nil, fmt.Errorf("clock function is required")
	}
	t := &Table{selfID: selfID, entries: make(map[string]*entry), direct: make(map[string]string), selfAliases: make(map[string]bool), timing: timing, now: now}
	t.entries[selfID] = &entry{generation: generation, lastChange: t.now(), status: Alive}
	return t, nil
}

type Restart struct {
	ID                           string
	OldGeneration, NewGeneration int64
}

type Changes struct {
	NewIDs      []string
	Restarted   []Restart
	AdvancedIDs []string
	Conflict    bool
}

// Merge accepts evidence only. Duplicate or stale evidence never refreshes age.
// Logging belongs to the caller, after this method releases the lock.
func (t *Table) Merge(members []WireMember) Changes {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	var changes Changes
	for _, incoming := range members {
		if incoming.Validate() != nil {
			continue
		}
		current, known := t.entries[incoming.ID]
		if incoming.ID == t.selfID {
			if incoming.Generation > current.generation {
				changes.Conflict = true
			}
			continue
		}
		if !known {
			if len(t.entries) >= MaxMembers {
				continue
			}
			t.entries[incoming.ID] = &entry{generation: incoming.Generation, heartbeat: incoming.Heartbeat, lastChange: now, status: Alive}
			changes.NewIDs = append(changes.NewIDs, incoming.ID)
			continue
		}
		if incoming.Generation > current.generation {
			changes.Restarted = append(changes.Restarted, Restart{incoming.ID, current.generation, incoming.Generation})
			current.generation = incoming.Generation
			current.heartbeat = incoming.Heartbeat
			current.lastChange = now
		} else if incoming.Generation == current.generation && incoming.Heartbeat > current.heartbeat {
			current.heartbeat = incoming.Heartbeat
			current.lastChange = now
			changes.AdvancedIDs = append(changes.AdvancedIDs, incoming.ID)
		}
	}
	return changes
}

func (t *Table) Beat() {
	t.mu.Lock()
	defer t.mu.Unlock()
	self := t.entries[t.selfID]
	// Never wrap into a negative heartbeat, which would be invalid evidence.
	if self.heartbeat < 1<<63-1 {
		self.heartbeat++
	}
	self.lastChange = t.now()
}

type Transition struct {
	ID       string
	From, To Status
}

func (t *Table) statusAt(id string, e *entry, now time.Time) Status {
	if id == t.selfID {
		return Alive
	}
	age := now.Sub(e.lastChange)
	if age < t.timing.SuspectAfter {
		return Alive
	}
	if age < t.timing.DeadAfter {
		return Suspect
	}
	return Dead
}

// Refresh stores status only to report each transition once. Merge leaves that
// field alone so recovery from dead to alive is also reported here.
func (t *Table) Refresh() []Transition {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	var transitions []Transition
	for id, e := range t.entries {
		status := t.statusAt(id, e, now)
		if status != e.status {
			transitions = append(transitions, Transition{id, e.status, status})
			e.status = status
		}
	}
	sort.Slice(transitions, func(i, j int) bool { return transitions[i].ID < transitions[j].ID })
	return transitions
}

func (t *Table) Digest() []WireMember {
	t.mu.Lock()
	defer t.mu.Unlock()
	members := make([]WireMember, 0, len(t.entries))
	for id, e := range t.entries {
		members = append(members, WireMember{id, e.generation, e.heartbeat})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	return members
}
