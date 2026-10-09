package gossip

import "sort"

// NoteDirect records a validated exchange's responder and the address actually
// dialed (possibly a proxy). It does not refresh heartbeat evidence. Call after
// Merge; unknown IDs can still be matched to seeds when the member table is full.
func (t *Table) NoteDirect(id, addr string) {
	if !ValidID(id) || addr == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	oldID := t.direct[addr]
	if oldID != id {
		if old := t.entries[oldID]; old != nil && old.via == addr {
			old.via = ""
		}
	}
	t.direct[addr] = id
	if id == t.selfID {
		t.selfAliases[addr] = true
		return
	}
	delete(t.selfAliases, addr)
	if e := t.entries[id]; e != nil {
		e.via = addr
	}
}

// MarkSelfAlias excludes an address discovered to lead back to this process.
func (t *Table) MarkSelfAlias(addr string) {
	if addr == "" {
		return
	}
	t.NoteDirect(t.selfID, addr)
}

// SelectPeer chooses uniformly when pick is uniform on [0,n), e.g. rand.Intn.
// Status is deliberately not consulted: dead peers must remain discoverable.
// The injected picker runs outside the table lock and must return a valid index.
func (t *Table) SelectPeer(seeds []string, pick func(int) int) (string, bool) {
	t.mu.Lock()
	eligible := make([]string, 0, len(seeds))
	seen := make(map[string]bool, len(seeds))
	for _, addr := range seeds {
		if addr != "" && !t.selfAliases[addr] && !seen[addr] {
			eligible = append(eligible, addr)
			seen[addr] = true
		}
	}
	t.mu.Unlock()
	if len(eligible) == 0 {
		return "", false
	}
	return eligible[pick(len(eligible))], true
}

type MemberView struct {
	ID              string `json:"id"`
	Status          Status `json:"status"`
	Generation      int64  `json:"generation"`
	Heartbeat       int64  `json:"heartbeat"`
	LastChangeMSAgo int64  `json:"last_change_ms_ago"`
	Via             string `json:"via"`
}

type LocalView struct {
	Members        []MemberView `json:"members"`
	UnreachedSeeds []string     `json:"unreached_seeds"`
}

// View derives current status without consuming the transitions Refresh reports.
// All returned values are copies; local addresses never enter the wire digest.
func (t *Table) View(seeds []string) LocalView {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	view := LocalView{Members: make([]MemberView, 0, len(t.entries)), UnreachedSeeds: make([]string, 0)}
	for id, e := range t.entries {
		age := now.Sub(e.lastChange).Milliseconds()
		if age < 0 {
			age = 0
		}
		view.Members = append(view.Members, MemberView{
			ID: id, Status: t.statusAt(id, e, now), Generation: e.generation,
			Heartbeat: e.heartbeat, LastChangeMSAgo: age, Via: e.via,
		})
	}
	seen := make(map[string]bool, len(seeds))
	for _, addr := range seeds {
		if addr != "" && !seen[addr] && t.direct[addr] == "" && !t.selfAliases[addr] {
			view.UnreachedSeeds = append(view.UnreachedSeeds, addr)
			seen[addr] = true
		}
	}
	sort.Slice(view.Members, func(i, j int) bool { return view.Members[i].ID < view.Members[j].ID })
	sort.Strings(view.UnreachedSeeds)
	return view
}
