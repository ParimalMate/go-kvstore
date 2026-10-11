package antientropy

import (
	"context"
	"io"
	"kvstore/internal/gossip"
	"kvstore/internal/replication"
	"kvstore/internal/store"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type testApplier struct{ s *store.Store }

func (a testApplier) ApplyVersions(k string, vs []replication.Version) (int, error) {
	for i, v := range vs {
		if _, err := a.s.PutWithClock(k, v.Value, v.VC); err != nil {
			return i, err
		}
	}
	return len(vs), nil
}
func workerService(t *testing.T, o Options) *Service {
	t.Helper()
	st := store.NewStore(filepath.Join(t.TempDir(), "data.wal"))
	s, err := NewService(st, testApplier{st}, log.New(io.Discard, "", 0), 100, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop(); st.Close() })
	return s
}

type membershipStub struct {
	mu     sync.Mutex
	status gossip.Status
	hook   func(gossip.Transition)
	addr   string
	alias  bool
}

func (m *membershipStub) PeerByAddr(string) (string, gossip.Status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return "n2", m.status, m.status != ""
}
func (m *membershipStub) AddrOf(string) (string, bool) { return m.addr, true }
func (m *membershipStub) IsSelfAlias(string) bool      { return m.alias }
func (m *membershipStub) OnChange(f func(gossip.Transition)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hook = f
}
func (m *membershipStub) fire() {
	m.mu.Lock()
	f := m.hook
	m.mu.Unlock()
	if f != nil {
		f(gossip.Transition{ID: "n2", From: gossip.Dead, To: gossip.Alive})
	}
}

func TestWorkerQueueDedupAndStop(t *testing.T) {
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	peer := strings.TrimPrefix(server.URL, "http://")
	s := workerService(t, Options{})
	s.Start(context.Background())
	first, err := s.enqueue(peer)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	duplicate, err := s.enqueue(peer)
	if err != nil || duplicate != first {
		t.Fatal("active peer not deduplicated")
	}
	for i := 0; i < 16; i++ {
		if _, err = s.enqueue(string(rune('a'+i)) + ":1234"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.enqueue("overflow:1234"); err != ErrQueueFull {
		t.Fatalf("full queue: %v", err)
	}
	rr := httptest.NewRecorder()
	s.HandleRun(rr, httptest.NewRequest("POST", "/internal/ae/run", strings.NewReader(`{"peer":"overflow:1234"}`)))
	if rr.Code != 429 {
		t.Fatalf("status %d", rr.Code)
	}
	done := make(chan struct{})
	go func() { s.Stop(); s.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop blocked")
	}
	select {
	case <-first.done:
	case <-time.After(time.Second):
		t.Fatal("waiter stranded")
	}
	if _, err = s.enqueue(peer); err != ErrStopped {
		t.Fatal(err)
	}
}

func TestWorkerGatingManualRecoveryAndStatus(t *testing.T) {
	remote := workerService(t, Options{})
	mux := http.NewServeMux()
	remote.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	peer := strings.TrimPrefix(server.URL, "http://")
	membership := &membershipStub{status: gossip.Suspect, addr: peer}
	s := workerService(t, Options{Peers: []string{peer}, Gossip: membership, Interval: time.Second})
	if len(s.eligiblePeers()) != 0 {
		t.Fatal("suspect eligible")
	}
	s.Start(context.Background())
	rr := httptest.NewRecorder()
	s.HandleRun(rr, httptest.NewRequest("POST", "/internal/ae/run", strings.NewReader(`{"peer":"`+peer+`"}`)))
	if rr.Code != 200 {
		t.Fatalf("explicit run: %d %s", rr.Code, rr.Body.String())
	}
	if s.Status().Totals.Cycles != 1 {
		t.Fatal("manual cycle missing")
	}
	membership.fire()
	deadline := time.Now().Add(500 * time.Millisecond)
	for s.Status().Totals.Cycles < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Status().Totals.Cycles != 2 {
		t.Fatal("recovery did not trigger")
	}
	snapshot := s.Status()
	delete(snapshot.LastReports, peer)
	if len(s.Status().LastReports) != 1 {
		t.Fatal("status aliases live map")
	}
	s.Stop()
	membership.fire() // A retained hook must remain harmless after shutdown.
}

func TestWorkerDisabledAndUnknownPeers(t *testing.T) {
	m := &membershipStub{addr: "localhost:1234"}
	s := workerService(t, Options{Peers: []string{m.addr, m.addr}, Gossip: m})
	if len(s.eligiblePeers()) != 1 {
		t.Fatal("unknown must be eligible and deduplicated")
	}
	s.Start(context.Background())
	m.mu.Lock()
	hasHook := m.hook != nil
	m.mu.Unlock()
	if hasHook {
		t.Fatal("disabled scheduling installed hook")
	}
	standalone := workerService(t, Options{})
	standalone.Start(context.Background())
	rr := httptest.NewRecorder()
	standalone.HandleRun(rr, httptest.NewRequest("POST", "/internal/ae/run", nil))
	if rr.Code != 200 || strings.TrimSpace(rr.Body.String()) != "[]" {
		t.Fatal(rr.Body.String())
	}
}

func TestRunValidation(t *testing.T) {
	s := workerService(t, Options{})
	for _, body := range []string{"null", "[]", "{} trailing", `{"peer":"bad"}`} {
		rr := httptest.NewRecorder()
		s.HandleRun(rr, httptest.NewRequest("POST", "/internal/ae/run", strings.NewReader(body)))
		if rr.Code != 400 {
			t.Fatalf("%q: %d", body, rr.Code)
		}
	}
	rr := httptest.NewRecorder()
	s.HandleRun(rr, httptest.NewRequest("POST", "/internal/ae/run", strings.NewReader(strings.Repeat(" ", 65537))))
	if rr.Code != 413 {
		t.Fatal(rr.Code)
	}
}

func TestWorkerTimerAndSelfAlias(t *testing.T) {
	remote := workerService(t, Options{})
	mux := http.NewServeMux()
	remote.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	peer := strings.TrimPrefix(server.URL, "http://")
	s := workerService(t, Options{Peers: []string{peer}, Interval: time.Second})
	s.Start(context.Background())
	deadline := time.Now().Add(500 * time.Millisecond)
	for s.Status().Totals.Cycles == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Status().Totals.Cycles != 1 {
		t.Fatal("initial timer did not run")
	}
	s.Stop()
	m := &membershipStub{addr: peer, alias: true}
	alias := workerService(t, Options{Peers: []string{peer}, Gossip: m})
	if len(alias.eligiblePeers()) != 0 {
		t.Fatal("self alias eligible")
	}
}
