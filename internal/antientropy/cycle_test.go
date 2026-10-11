package antientropy_test

import (
	"context"
	"fmt"
	"io"
	"kvstore/internal/antientropy"
	"kvstore/internal/api"
	"kvstore/internal/merkle"
	"kvstore/internal/replication"
	"kvstore/internal/store"
	"kvstore/internal/vectorclock"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type delegate struct {
	mu      sync.RWMutex
	handler http.Handler
}

func (d *delegate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	h := d.handler
	d.mu.RUnlock()
	if h == nil {
		http.Error(w, "offline", 503)
		return
	}
	h.ServeHTTP(w, r)
}
func (d *delegate) set(h http.Handler) { d.mu.Lock(); d.handler = h; d.mu.Unlock() }

type node struct {
	s      *store.Store
	h      *api.Handler
	svc    *antientropy.Service
	server *httptest.Server
	d      *delegate
	mux    *http.ServeMux
	wal    string
}

func (n *node) addr() string { return strings.TrimPrefix(n.server.URL, "http://") }
func cluster(t *testing.T, count, budget int) []*node {
	t.Helper()
	nodes := make([]*node, count)
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				n.server.Close()
				n.s.Close()
			}
		}
	})
	for i := range nodes {
		n := &node{wal: filepath.Join(t.TempDir(), "wal"), d: &delegate{}}
		n.s = store.NewStore(n.wal)
		n.server = httptest.NewServer(n.d)
		nodes[i] = n
	}
	for i, n := range nodes {
		peers := []string{}
		for j, p := range nodes {
			if i != j {
				peers = append(peers, p.addr())
			}
		}
		quorum := 1
		if count > 1 {
			quorum = 2
		}
		logger := log.New(io.Discard, "", 0)
		n.h = api.NewHandler(n.s, logger, peers, quorum, quorum, fmt.Sprintf("n%d", i+1))
		var err error
		n.svc, err = antientropy.NewService(n.s, n.h, logger, budget)
		if err != nil {
			t.Fatal(err)
		}
		n.mux = http.NewServeMux()
		n.mux.HandleFunc("PUT /kv/{key}", n.h.PutHandler)
		n.mux.HandleFunc("GET /internal/kv/{key}", n.h.InternalGetHandler)
		n.mux.HandleFunc("PUT /internal/kv/{key}", n.h.InternalPutHandler)
		n.svc.RegisterRoutes(n.mux)
		n.d.set(n.mux)
	}
	return nodes
}
func apply(t *testing.T, n *node, key, value string, vc vectorclock.VectorClock) {
	t.Helper()
	if _, err := n.h.ApplyVersions(key, []replication.Version{{Value: value, VC: vc}}); err != nil {
		t.Fatal(err)
	}
}
func root(t *testing.T, n *node) merkle.Hash {
	t.Helper()
	tree, err := antientropy.NewSnapshotCache(n.s).Tree()
	if err != nil {
		t.Fatal(err)
	}
	return tree.Root()
}
func walSize(t *testing.T, n *node) int64 {
	t.Helper()
	info, err := os.Stat(n.wal)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
func externalPut(t *testing.T, n *node, key, value string) {
	t.Helper()
	r := httptest.NewRequest("PUT", "/kv/"+key, strings.NewReader(value))
	w := httptest.NewRecorder()
	n.mux.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestSyncMissedWritesAndNoOp(t *testing.T) {
	ns := cluster(t, 3, 1000)
	a, b, c := ns[0], ns[1], ns[2]
	c.d.set(nil)
	for i := 0; i < 5; i++ {
		externalPut(t, a, fmt.Sprintf("key-%d", i), "value")
	}
	c.d.set(c.mux)
	report, err := c.svc.SyncWith(context.Background(), a.addr())
	if err != nil || report.KeysPulled != 5 || report.KeysPushed != 0 || report.VersionsApplied != 5 || !report.RootsEqualAfter {
		t.Fatal(report, err)
	}
	if root(t, a) != root(t, b) || root(t, b) != root(t, c) {
		t.Fatal("roots differ")
	}
	beforeA, beforeC := walSize(t, a), walSize(t, c)
	report, err = c.svc.SyncWith(context.Background(), a.addr())
	if err != nil || !report.RootsEqual || !report.RootsEqualAfter || report.HashesFetched != 1 || report.Requests != 1 || report.KeysPulled != 0 || report.KeysPushed != 0 {
		t.Fatal(report, err)
	}
	if beforeA != walSize(t, a) || beforeC != walSize(t, c) {
		t.Fatal("no-op grew WAL")
	}
}

func TestSyncEfficiency(t *testing.T) {
	ns := cluster(t, 2, 1000)
	a, b := ns[0], ns[1]
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("same-%d", i)
		for _, n := range ns {
			apply(t, n, key, "value", vectorclock.VectorClock{"n1": 1})
		}
	}
	apply(t, a, "different", "new", vectorclock.VectorClock{"n1": 1})
	report, err := b.svc.SyncWith(context.Background(), a.addr())
	if err != nil || report.KeysPulled+report.KeysPushed != 1 || report.HashesFetched > 49 || !report.RootsEqualAfter {
		t.Fatal(report, err)
	}
}

func TestSyncSiblingsThenResolve(t *testing.T) {
	ns := cluster(t, 2, 1000)
	a, b := ns[0], ns[1]
	apply(t, a, "city", "Mumbai", vectorclock.VectorClock{"n1": 2})
	apply(t, b, "city", "Pune", vectorclock.VectorClock{"n1": 1, "n2": 1})
	report, err := a.svc.SyncWith(context.Background(), b.addr())
	if err != nil || report.KeysPulled != 1 || report.KeysPushed != 1 || report.VersionsApplied != 1 || !report.RootsEqualAfter {
		t.Fatal(report, err)
	}
	for _, n := range ns {
		v, _ := n.s.GetSiblings("city")
		if len(v) != 2 {
			t.Fatal(v)
		}
	}
	externalPut(t, a, "city", "resolved")
	for _, n := range ns {
		v, _ := n.s.GetSiblings("city")
		if len(v) != 1 || v[0].Value != "resolved" {
			t.Fatal(v)
		}
	}
}

func TestSyncBothDirectionsAndCollision(t *testing.T) {
	ns := cluster(t, 2, 1000)
	a, b := ns[0], ns[1]
	apply(t, a, "only-a", "a", vectorclock.VectorClock{"n1": 1})
	apply(t, b, "only-b", "b", vectorclock.VectorClock{"n2": 1})
	apply(t, a, "collision", "left", vectorclock.VectorClock{"n1": 1})
	apply(t, b, "collision", "right", vectorclock.VectorClock{"n1": 1})
	report, err := a.svc.SyncWith(context.Background(), b.addr())
	if err != nil || report.Collisions != 1 || report.KeysPulled != 2 || report.KeysPushed != 1 || report.VersionsApplied != 1 || report.RootsEqualAfter {
		t.Fatal(report, err)
	}
	for _, n := range ns {
		for _, key := range []string{"only-a", "only-b"} {
			v, _ := n.s.GetSiblings(key)
			if len(v) != 1 {
				t.Fatal(key, v)
			}
		}
	}
	for i, n := range ns {
		got, _, _ := n.s.Get("collision")
		if got != []string{"left", "right"}[i] {
			t.Fatal("collision changed", got)
		}
	}
}

func TestSyncBudget(t *testing.T) {
	ns := cluster(t, 2, 100)
	a, b := ns[0], ns[1]
	for i := 0; i < 300; i++ {
		apply(t, a, fmt.Sprintf("key-%d", i), "value", vectorclock.VectorClock{"n1": 1})
	}
	for i := 0; i < 3; i++ {
		report, err := b.svc.SyncWith(context.Background(), a.addr())
		if err != nil || report.KeysPulled != 100 || report.KeysPushed != 0 || report.Truncated != (i < 2) {
			t.Fatal(i, report, err)
		}
	}
	if root(t, a) != root(t, b) {
		t.Fatal("budgeted cycles did not converge")
	}
}

func TestSyncPerKeyErrorContinues(t *testing.T) {
	ns := cluster(t, 2, 1000)
	a, b := ns[0], ns[1]
	apply(t, b, "bad", "b", vectorclock.VectorClock{"n2": 1})
	apply(t, b, "good", "g", vectorclock.VectorClock{"n2": 1})
	b.d.set(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/internal/kv/bad" {
			http.Error(w, "error", 500)
			return
		}
		b.mux.ServeHTTP(w, r)
	}))
	report, err := a.svc.SyncWith(context.Background(), b.addr())
	if err == nil || report.KeyErrors != 1 || report.VersionsApplied != 1 {
		t.Fatal(report, err)
	}
	if v, _, _ := a.s.Get("good"); v != "g" {
		t.Fatal("later key not repaired")
	}
}

func TestSyncCancellation(t *testing.T) {
	ns := cluster(t, 1, 1000)
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := ns[0].svc.SyncWith(ctx, strings.TrimPrefix(server.URL, "http://")); done <- err }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cycle cancellation too slow")
	}
}

func TestSyncLyingTreeNeverApplies(t *testing.T) {
	for _, mode := range []string{"shape", "index", "changed-listing"} {
		t.Run(mode, func(t *testing.T) {
			ns := cluster(t, 2, 1000)
			a, b := ns[0], ns[1]
			apply(t, b, "key", "remote", vectorclock.VectorClock{"n2": 1})
			b.d.set(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "shape" && r.URL.Path == "/internal/ae/nodes" {
					io.WriteString(w, `{"fanout":2,"levels":3,"hashes":[]}`)
					return
				}
				if r.URL.Path == "/internal/ae/buckets" {
					index := merkle.BucketOf("key")
					if mode == "index" {
						index = 4096
					}
					fmt.Fprintf(w, `{"fanout":16,"levels":3,"buckets":[{"index":%d,"keys":[{"key":"key","hash":"%s"}]}]}`, index, strings.Repeat("0", 64))
					return
				}
				b.mux.ServeHTTP(w, r)
			}))
			before := walSize(t, a)
			report, err := a.svc.SyncWith(context.Background(), b.addr())
			if mode == "changed-listing" {
				if err != nil || report.BucketsSkipped != 1 {
					t.Fatal(report, err)
				}
			} else if err == nil {
				t.Fatal("invalid tree accepted")
			}
			if report.KeysPulled != 0 || report.VersionsApplied != 0 || walSize(t, a) != before {
				t.Fatal("untrusted listing changed state", report)
			}
		})
	}
}
