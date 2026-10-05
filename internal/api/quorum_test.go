package api

import (
	"encoding/json"
	"io"
	"kvstore/internal/store"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type quorumLog chan string

func (l quorumLog) Write(p []byte) (int, error) {
	l <- string(p)
	return len(p), nil
}

func (l quorumLog) waitFor(t *testing.T, text string) {
	t.Helper()
	timer := time.NewTimer(4 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line := <-l:
			if strings.Contains(line, text) {
				return
			}
		case <-timer.C:
			t.Fatalf("missing background log containing %q", text)
		}
	}
}

func TestWriteQuorumReturnsBeforeSlowPeer(t *testing.T) {
	logs := make(quorumLog, 32)
	logger := log.New(logs, "", 0)
	var peers []string
	for _, delay := range []time.Duration{60 * time.Millisecond, 900 * time.Millisecond, 0} {
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if delay == 0 {
				http.Error(w, "peer failure", http.StatusInternalServerError)
				return
			}
			select {
			case <-time.After(delay):
				w.WriteHeader(http.StatusOK)
			case <-r.Context().Done():
			}
		}))
		defer peer.Close()
		peers = append(peers, strings.TrimPrefix(peer.URL, "http://"))
	}
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	// Three peers plus the coordinator means N=4; W=2, R=3 overlap.
	h := NewHandler(s, logger, peers, 2, 3)
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
	response := httptest.NewRecorder()
	start := time.Now()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("Parimal")))
	elapsed := time.Since(start)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if elapsed < 60*time.Millisecond || elapsed >= 700*time.Millisecond {
		t.Fatalf("quorum latency=%s; expected fast peer acknowledgement before the 900ms peer", elapsed)
	}
	t.Logf("W=2 response took %s; slow peer delay is 900ms", elapsed)
	// The slow result must still be received and logged after the response.
	logs.waitFor(t, "-> "+peers[1]+" OK")
}

func TestWriteQuorumOneStillReplicates(t *testing.T) {
	logs := make(quorumLog, 16)
	release := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer peer.Close()
	addr := strings.TrimPrefix(peer.URL, "http://")
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	h := NewHandler(s, log.New(logs, "", 0), []string{addr}, 1, 2)
	req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("Parimal"))
	req.SetPathValue("key", "name")
	response := httptest.NewRecorder()
	start := time.Now()
	h.PutHandler(response, req)
	elapsed := time.Since(start)
	close(release)
	if response.Code != http.StatusOK || elapsed >= time.Second {
		t.Fatalf("W=1 should only wait for local storage: status=%d elapsed=%s", response.Code, elapsed)
	}
	logs.waitFor(t, "-> "+addr+" OK")
}

func TestWriteQuorumInsufficientAcks(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "peer error"
		if timeout {
			name = "peer timeout"
		}
		t.Run(name, func(t *testing.T) {
			logs := make(quorumLog, 16)
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(http.StatusOK)
			}))
			defer good.Close()
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if timeout {
					<-r.Context().Done()
					return
				}
				http.Error(w, "peer failed", http.StatusInternalServerError)
			}))
			defer bad.Close()
			goodAddr := strings.TrimPrefix(good.URL, "http://")
			badAddr := strings.TrimPrefix(bad.URL, "http://")
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			h := NewHandler(s, log.New(logs, "", 0), []string{goodAddr, badAddr}, 3, 1)
			req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("Parimal"))
			req.SetPathValue("key", "name")
			response := httptest.NewRecorder()
			start := time.Now()
			h.PutHandler(response, req)
			elapsed := time.Since(start)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("insufficient acks: status=%d body=%s", response.Code, response.Body.String())
			}
			if timeout && (elapsed < 1800*time.Millisecond || elapsed > 4*time.Second) {
				t.Fatalf("expected two-second timeout, got %s", elapsed)
			}
			var body replicationResponse
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Replicated) != 1 || body.Replicated[0] != goodAddr {
				t.Fatalf("missing successful peer: %+v", body)
			}
			if !timeout && (len(body.Failed) != 1 || body.Failed[0].Peer != badAddr) {
				t.Fatalf("missing failed peer: %+v", body)
			}
			if value, exists, err := s.Get("name"); err != nil || !exists || value != "Parimal" {
				t.Fatal("failed quorum must preserve the local write")
			}
			logs.waitFor(t, "-> "+badAddr+" FAILED")
		})
	}
}

func TestWriteQuorumLocalFailureDoesNotReplicate(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer peer.Close()
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s, log.New(io.Discard, "", 0), []string{strings.TrimPrefix(peer.URL, "http://")}, 2, 1)
	req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("Parimal"))
	req.SetPathValue("key", "name")
	response := httptest.NewRecorder()
	h.PutHandler(response, req)
	if response.Code != http.StatusInternalServerError || calls.Load() != 0 {
		t.Fatalf("local failure: status=%d peer calls=%d", response.Code, calls.Load())
	}
}
