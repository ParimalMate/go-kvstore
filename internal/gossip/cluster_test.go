package gossip

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type delegatedHandler struct {
	mu       sync.RWMutex
	handler  http.Handler
	requests atomic.Int64
}

func (d *delegatedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.requests.Add(1)
	d.mu.RLock()
	handler := d.handler
	d.mu.RUnlock()
	if handler == nil {
		http.Error(w, "unavailable", 503)
		return
	}
	handler.ServeHTTP(w, r)
}
func (d *delegatedHandler) set(handler http.Handler) { d.mu.Lock(); d.handler = handler; d.mu.Unlock() }

type clusterEvents struct {
	mu      sync.Mutex
	changes []Transition
	output  bytes.Buffer
}

func (e *clusterEvents) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.output.Write(p)
}
func (e *clusterEvents) record(change Transition) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.changes = append(e.changes, change)
}
func (e *clusterEvents) forID(id string) []Transition {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := []Transition{}
	for _, change := range e.changes {
		if change.ID == id {
			result = append(result, change)
		}
	}
	return result
}
func (e *clusterEvents) logs() string { e.mu.Lock(); defer e.mu.Unlock(); return e.output.String() }

type gossipCluster struct {
	t         *testing.T
	services  []*Service
	delegates []*delegatedHandler
	servers   []*httptest.Server
	events    []*clusterEvents
	topology  [][]int
}

func newGossipCluster(t *testing.T, topology [][]int) *gossipCluster {
	t.Helper()
	c := &gossipCluster{t: t, topology: topology}
	t.Cleanup(func() {
		for _, service := range c.services {
			if service != nil {
				service.Stop()
			}
		}
		for _, server := range c.servers {
			server.Close()
		}
	})
	for range topology {
		delegate := &delegatedHandler{}
		c.delegates = append(c.delegates, delegate)
		c.servers = append(c.servers, httptest.NewServer(delegate))
		c.services = append(c.services, nil)
		c.events = append(c.events, &clusterEvents{})
	}
	// Reserve every HTTP address before configuring any service's seed list.
	for i := range topology {
		c.install(i, int64(100+i))
	}
	for _, service := range c.services {
		service.Start(context.Background())
	}
	return c
}
func (c *gossipCluster) install(index int, generation int64) {
	c.t.Helper()
	seeds := []string{}
	for _, peer := range c.topology[index] {
		seeds = append(seeds, strings.TrimPrefix(c.servers[peer].URL, "http://"))
	}
	table, err := NewTable(fmt.Sprintf("n%d", index+1), generation, Timing{50 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}, time.Now)
	if err != nil {
		c.t.Fatal(err)
	}
	service, err := NewService(table, log.New(c.events[index], "", 0), seeds)
	if err != nil {
		c.t.Fatal(err)
	}
	service.OnChange(c.events[index].record)
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	c.services[index] = service
	c.delegates[index].set(mux)
}
func (c *gossipCluster) member(observer int, id string) (MemberView, bool) {
	for _, member := range c.services[observer].table.View(nil).Members {
		if member.ID == id {
			return member, true
		}
	}
	return MemberView{}, false
}
func (c *gossipCluster) allAlive() bool {
	for _, service := range c.services {
		view := service.table.View(nil)
		if len(view.Members) != len(c.services) {
			return false
		}
		for _, member := range view.Members {
			if member.Status != Alive || member.Heartbeat < 1 {
				return false
			}
		}
	}
	return true
}
func (c *gossipCluster) stop(index int) { c.services[index].Stop(); c.delegates[index].set(nil) }

func TestClusterConvergenceCrashAndRestart(t *testing.T) {
	c := newGossipCluster(t, [][]int{{1, 2}, {0, 2}, {0, 1}})
	waitFor(t, c.allAlive)
	initial, _ := c.member(0, "n2")
	waitFor(t, func() bool {
		for _, observer := range []int{0, 2} {
			member, _ := c.member(observer, "n2")
			if member.Heartbeat <= 20 || member.Heartbeat <= initial.Heartbeat {
				return false
			}
		}
		return true
	})
	old, _ := c.member(0, "n2")
	c.stop(1)
	stoppedAt := time.Now()
	waitFor(t, func() bool {
		for _, observer := range []int{0, 2} {
			member, _ := c.member(observer, "n2")
			if member.Status != Dead || len(c.events[observer].forID("n2")) < 2 {
				return false
			}
		}
		return true
	})
	if age := time.Since(stoppedAt); age > 1500*time.Millisecond {
		t.Fatalf("crash detection too slow: %s", age)
	}
	for _, observer := range []int{0, 2} {
		want := []Transition{{"n2", Alive, Suspect}, {"n2", Suspect, Dead}}
		if got := c.events[observer].forID("n2"); !reflect.DeepEqual(got, want) {
			t.Fatalf("observer %d: %v", observer, got)
		}
	}
	c.install(1, old.Generation+1000)
	c.services[1].Start(context.Background())
	waitFor(t, func() bool {
		if !c.allAlive() {
			return false
		}
		for _, observer := range []int{0, 2} {
			member, _ := c.member(observer, "n2")
			if member.Generation != old.Generation+1000 || len(c.events[observer].forID("n2")) != 3 {
				return false
			}
		}
		return true
	})
	for _, observer := range []int{0, 2} {
		member, _ := c.member(observer, "n2")
		if member.Heartbeat >= old.Heartbeat {
			t.Fatalf("restart test failed to exercise lower heartbeat: old=%d new=%d", old.Heartbeat, member.Heartbeat)
		}
		changes := c.events[observer].forID("n2")
		if changes[2] != (Transition{"n2", Dead, Alive}) {
			t.Fatal(changes)
		}
		if !strings.Contains(c.events[observer].logs(), "n2 restarted") {
			t.Fatal("missing restart log")
		}
	}
}

func TestClusterIndirectLivenessAndHeal(t *testing.T) {
	// n1 and n3 never dial each other; all evidence between them travels via n2.
	c := newGossipCluster(t, [][]int{{1}, {0, 2}, {1}})
	waitFor(t, c.allAlive)
	first, _ := c.member(0, "n3")
	waitFor(t, func() bool { member, _ := c.member(0, "n3"); return member.Heartbeat > first.Heartbeat })
	member, _ := c.member(0, "n3")
	if member.Via != "" {
		t.Fatal("indirect node has a direct address", member)
	}
	c.stop(1)
	waitFor(t, func() bool {
		for _, observer := range []int{0, 2} {
			for _, id := range []string{"n1", "n2", "n3"} {
				if id == c.services[observer].selfID {
					continue
				}
				member, _ := c.member(observer, id)
				// View derives age immediately; wait for the running loop to
				// record the transition before healing so recovery is observable.
				if member.Status != Dead || len(c.events[observer].forID(id)) < 2 {
					return false
				}
			}
		}
		return true
	})
	c.install(1, 2000)
	c.services[1].Start(context.Background())
	waitFor(t, c.allAlive)
	member, _ = c.member(0, "n3")
	if member.Via != "" || member.Heartbeat <= first.Heartbeat {
		t.Fatal(member)
	}
	for _, observer := range []int{0, 2} {
		waitFor(t, func() bool { return len(c.events[observer].forID("n2")) >= 3 })
		want := []Transition{{"n2", Alive, Suspect}, {"n2", Suspect, Dead}, {"n2", Dead, Alive}}
		if got := c.events[observer].forID("n2"); !reflect.DeepEqual(got, want) {
			t.Fatal(got)
		}
	}
}

func TestClusterHealthyIdleNoFalseSuspicions(t *testing.T) {
	c := newGossipCluster(t, [][]int{{1, 2}, {0, 2}, {0, 1}})
	waitFor(t, c.allAlive)
	initial, _ := c.member(0, "n2")
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			final, _ := c.member(0, "n2")
			if final.Heartbeat <= initial.Heartbeat {
				t.Fatal("idle cluster did not advance")
			}
			return
		case <-ticker.C:
			for _, events := range c.events {
				events.mu.Lock()
				for _, change := range events.changes {
					if change.To == Suspect {
						events.mu.Unlock()
						t.Fatalf("false suspicion: %+v", change)
					}
				}
				events.mu.Unlock()
			}
		}
	}
}

func TestClusterSelfAliasSkippedByLoop(t *testing.T) {
	c := newGossipCluster(t, [][]int{{0}})
	addr := strings.TrimPrefix(c.servers[0].URL, "http://")
	waitFor(t, func() bool {
		_, eligible := c.services[0].table.SelectPeer([]string{addr}, func(int) int { return 0 })
		return !eligible
	})
	heartbeat := c.services[0].table.Digest()[0].Heartbeat
	waitFor(t, func() bool { return c.services[0].table.Digest()[0].Heartbeat >= heartbeat+5 })
	if count := c.delegates[0].requests.Load(); count != 1 {
		t.Fatalf("alias contacted %d times", count)
	}
	if count := strings.Count(c.events[0].logs(), "is an alias"); count != 1 {
		t.Fatalf("alias logged %d times", count)
	}
}
