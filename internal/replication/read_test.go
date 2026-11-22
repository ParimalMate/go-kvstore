package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"kvstore/internal/vectorclock"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func addr(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }

func peerReturning(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestValidateVersion(t *testing.T) {
	bigClock := vectorclock.VectorClock{}
	for i := 0; len(bigClock) < 4000; i++ {
		bigClock[fmt.Sprintf("node-with-a-long-identifier-%06d", i)] = int64(i)
	}
	cases := []struct {
		name string
		v    Version
		ok   bool
	}{
		{"valid", Version{Value: "v", VC: vectorclock.VectorClock{"n1": 1}}, true},
		{"empty clock object", Version{Value: "v", VC: vectorclock.VectorClock{}}, true},
		{"zero counter", Version{Value: "v", VC: vectorclock.VectorClock{"n1": 0}}, true},
		{"value at limit", Version{Value: strings.Repeat("x", MaxValueBytes), VC: vectorclock.VectorClock{"n1": 1}}, true},
		{"nil clock", Version{Value: "v"}, false},
		{"value over limit", Version{Value: strings.Repeat("x", MaxValueBytes+1), VC: vectorclock.VectorClock{"n1": 1}}, false},
		{"empty node id", Version{Value: "v", VC: vectorclock.VectorClock{"": 1}}, false},
		{"negative counter", Version{Value: "v", VC: vectorclock.VectorClock{"n1": -1}}, false},
		{"clock over limit", Version{Value: "v", VC: bigClock}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateVersion(tc.v)
			if (err == nil) != tc.ok {
				t.Fatalf("ValidateVersion ok=%v, want ok=%v (err=%v)", err == nil, tc.ok, err)
			}
		})
	}
}

func TestReadParsesSiblings(t *testing.T) {
	var gotPath string
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.EscapedPath()
		_, _ = io.WriteString(w, `[{"value":"a","vc":{"n1":2}},{"value":"","vc":{"n2":1}}]`)
	}))
	defer peer.Close()

	versions, err := Read(context.Background(), addr(peer), "a/b c")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if gotPath != "/internal/kv/a%2Fb%20c" {
		t.Errorf("key was not path-escaped: %s", gotPath)
	}
	if len(versions) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(versions))
	}
	if versions[0].Value != "a" || !maps.Equal(versions[0].VC, vectorclock.VectorClock{"n1": 2}) {
		t.Errorf("first sibling = %+v", versions[0])
	}
	if versions[1].Value != "" || !maps.Equal(versions[1].VC, vectorclock.VectorClock{"n2": 1}) {
		t.Errorf("empty-string value must be kept as a sibling: %+v", versions[1])
	}
}

func TestReadMissingKeyIsEmptyNotError(t *testing.T) {
	versions, err := Read(context.Background(), addr(peerReturning(t, http.StatusOK, `[]`)), "k")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if versions == nil || len(versions) != 0 {
		t.Fatalf("expected empty non-nil slice, got %#v", versions)
	}
}

func TestReadRejectsBadResponses(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"server error", http.StatusInternalServerError, `[]`, "status"},
		{"not found status", http.StatusNotFound, `[]`, "status"},
		{"malformed json", http.StatusOK, `[{"value":`, ""},
		{"object instead of array", http.StatusOK, `{"value":"a","vc":{}}`, ""},
		{"null instead of array", http.StatusOK, `null`, "not null"},
		{"sibling without value", http.StatusOK, `[{"vc":{"n1":1}}]`, "requires value"},
		{"sibling with null value", http.StatusOK, `[{"value":null,"vc":{"n1":1}}]`, "requires value"},
		{"sibling without clock", http.StatusOK, `[{"value":"a"}]`, "vc is required"},
		{"negative counter", http.StatusOK, `[{"value":"a","vc":{"n1":-1}}]`, "nonnegative"},
		{"empty node id", http.StatusOK, `[{"value":"a","vc":{"":1}}]`, "nonempty"},
		{"one bad sibling poisons the answer", http.StatusOK, `[{"value":"a","vc":{"n1":1}},{"value":"b"}]`, "vc is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			versions, err := Read(context.Background(), addr(peerReturning(t, tc.status, tc.body)), "k")
			if err == nil {
				t.Fatalf("expected an error, got versions %+v", versions)
			}
			if versions != nil {
				t.Errorf("versions must be nil on error, got %+v", versions)
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestReadRejectsOversizedResponse(t *testing.T) {
	chunk := []byte(strings.Repeat(" ", 1<<20))
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "[")
		for written := 1; written <= MaxReadBytes; written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer peer.Close()
	_, err := Read(context.Background(), addr(peer), "k")
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected a size error, got %v", err)
	}
}

func TestReadUnreachablePeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := ln.Addr().String()
	ln.Close() // Nothing listens here any more.
	if _, err := Read(context.Background(), dead, "k"); err == nil {
		t.Fatal("expected a connection error")
	}
}

func TestReadHonoursContext(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer peer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Read(ctx, addr(peer), "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Read ignored the context deadline: took %v", elapsed)
	}
}

func TestReadFanOutReportsEveryPeer(t *testing.T) {
	good := peerReturning(t, http.StatusOK, `[{"value":"v","vc":{"n1":1}}]`)
	empty := peerReturning(t, http.StatusOK, `[]`)
	failing := peerReturning(t, http.StatusInternalServerError, `oops`)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slow.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	got := map[string]ReadResult{}
	for r := range ReadFanOut(ctx, []string{addr(good), addr(empty), addr(failing), addr(slow)}, "k") {
		if _, dup := got[r.Peer]; dup {
			t.Fatalf("duplicate result for %s", r.Peer)
		}
		got[r.Peer] = r
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 results, got %d", len(got))
	}
	if r := got[addr(good)]; r.Err != nil || len(r.Versions) != 1 || r.Versions[0].Value != "v" {
		t.Errorf("good peer result = %+v", r)
	}
	if r := got[addr(empty)]; r.Err != nil || len(r.Versions) != 0 {
		t.Errorf("empty peer result = %+v", r)
	}
	if r := got[addr(failing)]; r.Err == nil {
		t.Errorf("failing peer reported success: %+v", r)
	}
	if r := got[addr(slow)]; !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Errorf("slow peer: expected deadline error, got %+v", r)
	}
}

func TestReadFanOutSlowPeerDoesNotDelayOthers(t *testing.T) {
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer slow.Close()
	defer close(release)
	fast := peerReturning(t, http.StatusOK, `[]`)

	results := ReadFanOut(context.Background(), []string{addr(slow), addr(fast)}, "k")
	select {
	case r := <-results:
		if r.Peer != addr(fast) || r.Err != nil {
			t.Fatalf("first result should be the fast peer, got %+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("a blocked peer delayed the fast peer's answer")
	}
}

func TestReadFanOutNoPeers(t *testing.T) {
	select {
	case _, ok := <-ReadFanOut(context.Background(), nil, "k"):
		if ok {
			t.Fatal("unexpected result without peers")
		}
	case <-time.After(time.Second):
		t.Fatal("empty read fan-out did not close")
	}
}

func TestReplicateRejectsInvalidVersionWithoutContactingPeer(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer peer.Close()
	for _, vc := range []vectorclock.VectorClock{nil, {"n1": -1}, {"": 1}} {
		if err := Replicate(context.Background(), addr(peer), "k", "v", vc); err == nil {
			t.Errorf("clock %v was accepted", vc)
		}
	}
	if err := Replicate(context.Background(), addr(peer), "k", strings.Repeat("x", MaxValueBytes+1), vectorclock.VectorClock{"n1": 1}); err == nil {
		t.Error("oversized value was accepted")
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("invalid writes reached the peer %d times", n)
	}
}

func TestReplicateEscapesKeyAndReportsStatus(t *testing.T) {
	var gotPath string
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		http.Error(w, "full", http.StatusInsufficientStorage)
	}))
	defer peer.Close()
	err := Replicate(context.Background(), addr(peer), "a/b?c", "v", vectorclock.VectorClock{"n1": 1})
	if err == nil || !strings.Contains(err.Error(), "507") {
		t.Fatalf("expected a 507 status error, got %v", err)
	}
	if gotPath != "/internal/kv/a%2Fb%3Fc" {
		t.Errorf("key was not path-escaped: %s", gotPath)
	}
}

func TestFanOutCopiesClockBeforeWorkersStart(t *testing.T) {
	release := make(chan struct{})
	seen := make(chan string, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		body, _ := io.ReadAll(r.Body)
		seen <- string(body)
	}))
	defer peer.Close()
	vc := vectorclock.VectorClock{"n1": 1}
	results := FanOut(context.Background(), []string{addr(peer)}, "k", "v", vc)
	vc["n1"] = 99 // The caller mutates its clock after starting the fan-out.
	close(release)
	for range results {
	}
	if body := <-seen; !strings.Contains(body, `"n1":1`) {
		t.Fatalf("worker observed the caller's later mutation: %s", body)
	}
}
