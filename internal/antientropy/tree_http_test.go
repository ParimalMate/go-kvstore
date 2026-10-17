package antientropy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kvstore/internal/merkle"
	"kvstore/internal/store"
	"kvstore/internal/vectorclock"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countedStore struct {
	*store.Store
	scans atomic.Int64
}

func (s *countedStore) Range(fn func(string, []store.Entry)) { s.scans.Add(1); s.Store.Range(fn) }
func source(t *testing.T) *countedStore {
	t.Helper()
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	t.Cleanup(func() { s.Close() })
	return &countedStore{Store: s}
}
func put(t *testing.T, s *store.Store, key, value string, n int64) {
	t.Helper()
	if _, err := s.PutWithClock(key, value, vectorclock.VectorClock{"n1": n}); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCacheReuseAndCoalescing(t *testing.T) {
	s := source(t)
	put(t, s.Store, "key", "one", 1)
	cache := NewSnapshotCache(s)
	trees := make(chan *merkle.Tree, 50)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tree, err := cache.Tree()
			if err != nil {
				t.Error(err)
				return
			}
			trees <- tree
		}()
	}
	wg.Wait()
	close(trees)
	first := <-trees
	for tree := range trees {
		if tree != first {
			t.Fatal("concurrent requests built separate trees")
		}
	}
	if s.scans.Load() != 1 {
		t.Fatal("more than one rebuild", s.scans.Load())
	}
	put(t, s.Store, "key", "one", 1)
	same, err := cache.Tree()
	if err != nil || same != first {
		t.Fatal("duplicate invalidated cache", err)
	}
	put(t, s.Store, "key", "two", 2)
	next, err := cache.Tree()
	if err != nil || next == first || next.Root() == first.Root() {
		t.Fatal("write did not rebuild", err)
	}
	if s.scans.Load() != 2 {
		t.Fatal(s.scans.Load())
	}
	keys, _ := first.BucketKeys(merkle.BucketOf("key"))
	oldHash, _ := merkle.KeyHash("key", []merkle.Version{{Value: "one", Clock: map[string]int64{"n1": 1}}})
	if keys[0].Hash != oldHash {
		t.Fatal("old snapshot mutated")
	}
}

type writeDuringScan struct {
	*countedStore
	after func()
}

func (s *writeDuringScan) Range(fn func(string, []store.Entry)) {
	s.countedStore.Range(fn)
	if s.after != nil {
		action := s.after
		s.after = nil
		action()
	}
}
func TestSnapshotDoesNotCacheMismatchedRevision(t *testing.T) {
	s := source(t)
	put(t, s.Store, "key", "one", 1)
	changed := &writeDuringScan{countedStore: s, after: func() { put(t, s.Store, "key", "two", 2) }}
	cache := NewSnapshotCache(changed)
	first, err := cache.Tree()
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Tree()
	if err != nil {
		t.Fatal(err)
	}
	if first.Root() == second.Root() || s.scans.Load() != 2 {
		t.Fatal("outdated snapshot cached under a newer revision")
	}
	third, _ := cache.Tree()
	if third != second {
		t.Fatal("stable snapshot not cached")
	}
}

func TestTreeHTTPValidation(t *testing.T) {
	s := source(t)
	mux := http.NewServeMux()
	NewTreeHandler(NewSnapshotCache(s)).RegisterRoutes(mux)
	for _, tc := range []struct {
		name, path, body string
		status           int
	}{
		{"nodes", "nodes", `{"level":0,"indexes":[0]}`, 200},
		{"buckets", "buckets", `{"indexes":[0,4095]}`, 200},
		{"bad json", "nodes", "{", 400},
		{"null", "nodes", "null", 400},
		{"missing level", "nodes", `{"indexes":[0]}`, 400},
		{"high level", "nodes", `{"level":4,"indexes":[0]}`, 400},
		{"negative level", "nodes", `{"level":-1,"indexes":[0]}`, 400},
		{"wrong root index", "nodes", `{"level":0,"indexes":[1]}`, 400},
		{"duplicate", "nodes", `{"level":1,"indexes":[0,0]}`, 400},
		{"negative index", "buckets", `{"indexes":[-1]}`, 400},
		{"high index", "buckets", `{"indexes":[4096]}`, 400},
		{"duplicate bucket", "buckets", `{"indexes":[1,1]}`, 400},
		{"empty", "buckets", `{"indexes":[]}`, 400},
		{"fraction", "nodes", `{"level":1.5,"indexes":[0]}`, 400},
		{"trailing", "nodes", `{"level":0,"indexes":[0]} {}`, 400},
		{"oversize", "nodes", strings.Repeat(" ", MaxRequestBytes+1), 413},
		{"oversize buckets", "buckets", strings.Repeat(" ", MaxRequestBytes+1), 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest("POST", "/internal/ae/"+tc.path, strings.NewReader(tc.body)))
			if rec.Code != tc.status {
				t.Fatal(rec.Code, rec.Body.String())
			}
		})
	}
	for _, tc := range []struct {
		path  string
		count int
	}{{"nodes", 257}, {"buckets", 65}} {
		indexes := make([]int, tc.count)
		for i := range indexes {
			indexes[i] = i
		}
		level := 3
		body, _ := json.Marshal(nodesRequest{&level, indexes})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("POST", "/internal/ae/"+tc.path, strings.NewReader(string(body))))
		if rec.Code != 400 {
			t.Fatal("excess indexes accepted")
		}
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/internal/ae/nodes", nil))
	if rec.Code != 405 {
		t.Fatal(rec.Code)
	}
}

func TestRealHTTPRemoteComparison(t *testing.T) {
	s := source(t)
	put(t, s.Store, "city", "Pune", 1)
	cache := NewSnapshotCache(s)
	mux := http.NewServeMux()
	NewTreeHandler(cache).RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	remote := NewHTTPRemote(context.Background(), strings.TrimPrefix(server.URL, "http://"))
	tree, _ := cache.Tree()
	diff, stats, err := merkle.Compare(tree, remote, merkle.CompareOptions{})
	if err != nil || len(diff.Keys) != 0 || stats.HashesFetched != 1 {
		t.Fatal(diff, stats, err)
	}
	diff, _, err = merkle.Compare((&merkle.Builder{}).Build(), remote, merkle.CompareOptions{})
	if err != nil || !reflect.DeepEqual(diff.Keys, []string{"city"}) {
		t.Fatal(diff, err)
	}
	// Nonascending requested node/bucket order must be preserved by the server.
	response, err := remote.Nodes(3, []int{4095, 0, 22})
	if err != nil {
		t.Fatal(err)
	}
	for i, index := range []int{4095, 0, 22} {
		want, _ := tree.Node(3, index)
		if response.Hashes[i] != want {
			t.Fatal("wrong response order")
		}
	}
	if _, err := remote.Buckets([]int{4095, 0}); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPRemoteRejectsBadResponses(t *testing.T) {
	zero := strings.Repeat("0", 64)
	bucket := merkle.BucketOf("key")
	for _, tc := range []struct {
		name, body string
		status     int
		buckets    bool
	}{
		{"shape", `{"fanout":2,"levels":3,"hashes":["` + zero + `"]}`, 200, false},
		{"hash length", `{"fanout":16,"levels":3,"hashes":["00"]}`, 200, false},
		{"count", `{"fanout":16,"levels":3,"hashes":[]}`, 200, false},
		{"malformed", "{", 200, false},
		{"oversize", strings.Repeat(" ", MaxResponseBytes+1), 200, false},
		{"non200", "unavailable", 503, false},
		{"redirect", "", 302, false},
		{"bucket shape", `{"fanout":2,"levels":3,"buckets":[]}`, 200, true},
		{"index", `{"fanout":16,"levels":3,"buckets":[{"index":4096,"keys":[]}]}`, 200, true},
		{"missing hash", fmt.Sprintf(`{"fanout":16,"levels":3,"buckets":[{"index":%d,"keys":[{"key":"key"}]}]}`, bucket), 200, true},
		{"wrong key", fmt.Sprintf(`{"fanout":16,"levels":3,"buckets":[{"index":%d,"keys":[{"key":"other-key","hash":"%s"}]}]}`, bucket, zero), 200, true},
		{"duplicate keys", fmt.Sprintf(`{"fanout":16,"levels":3,"buckets":[{"index":%d,"keys":[{"key":"key","hash":"%s"},{"key":"key","hash":"%s"}]}]}`, bucket, zero, zero), 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			remote := NewHTTPRemote(context.Background(), strings.TrimPrefix(server.URL, "http://"))
			var err error
			if tc.buckets {
				_, err = remote.Buckets([]int{bucket})
			} else {
				_, err = remote.Nodes(0, []int{0})
			}
			if err == nil {
				t.Fatal("accepted invalid response")
			}
			if tc.status != 200 {
				var typed *HTTPStatusError
				if !errors.As(err, &typed) || typed.StatusCode != tc.status {
					t.Fatal(err)
				}
			} else {
				var typed *InvalidResponseError
				if !errors.As(err, &typed) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRemoteCancellation(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote := NewHTTPRemote(ctx, strings.TrimPrefix(server.URL, "http://"))
	done := make(chan error, 1)
	go func() { _, err := remote.Nodes(0, []int{0}); done <- err }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
	if remote.client.Timeout != 5*time.Second {
		t.Fatal("wrong request timeout")
	}
}
