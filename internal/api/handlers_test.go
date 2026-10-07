package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kvstore/internal/replication"
	"kvstore/internal/store"
	"kvstore/internal/vectorclock"
	"log"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestInternalGetHandler(t *testing.T) {
	const counter int64 = 1720000000000000123
	tests := []struct {
		name    string
		value   string
		counter int64
		exists  bool
	}{
		{name: "existing", value: "Parimal", counter: counter, exists: true},
		{name: "empty value", value: "", counter: counter, exists: true},
		{name: "zero counter", value: "zero", counter: 0, exists: true},
		{name: "missing", exists: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			if tc.exists {
				if _, err := s.PutWithClock("name", tc.value, vectorclock.VectorClock{"origin": tc.counter}); err != nil {
					t.Fatal(err)
				}
			}
			h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1")
			mux := http.NewServeMux()
			mux.HandleFunc("GET /internal/kv/{key}", h.InternalGetHandler)
			mux.HandleFunc("GET /kv/{key}", h.GetHandler)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/kv/name", nil))
			if response.Code != http.StatusOK {
				t.Fatalf("internal GET status=%d body=%s", response.Code, response.Body.String())
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("unexpected content type: %q", got)
			}
			var body []replication.Version
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body == nil {
				t.Fatal("expected array, not null")
			}
			if tc.exists {
				if len(body) != 1 || body[0].Value != tc.value || !maps.Equal(body[0].VC, vectorclock.VectorClock{"origin": tc.counter}) {
					t.Fatalf("unexpected response: %s", response.Body.String())
				}
			} else if len(body) != 0 {
				t.Fatal("missing key returned versions")
			}

			// External reads retain their plain-value / 404 contract.
			external := httptest.NewRecorder()
			mux.ServeHTTP(external, httptest.NewRequest(http.MethodGet, "/kv/name", nil))
			if tc.exists {
				if external.Code != http.StatusOK || external.Body.String() != tc.value {
					t.Fatalf("external GET status=%d body=%q", external.Code, external.Body.String())
				}
			} else if external.Code != http.StatusNotFound {
				t.Fatalf("missing external GET status=%d", external.Code)
			}
		})
	}
}

func TestWritePreservesCoordinatorClock(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	var peers []string
	var stores []*store.Store
	for i := 0; i < 2; i++ {
		s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
		defer s.Close()
		stores = append(stores, s)
		h := NewHandler(s, logger, nil, 1, 1, fmt.Sprintf("peer%d", i))
		mux := http.NewServeMux()
		mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
		peer := httptest.NewServer(mux)
		defer peer.Close()
		peers = append(peers, strings.TrimPrefix(peer.URL, "http://"))
	}
	origin := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	defer origin.Close()
	h := NewHandler(origin, logger, peers, len(peers)+1, 1, "n1")
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
	for index, value := range []string{"Parimal", "", "नमस्ते\n\"quoted\"", strings.Repeat("\x00", 1<<20)} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader(value)))
		if response.Code != http.StatusOK {
			t.Fatalf("PUT status=%d body=%s", response.Code, response.Body.String())
		}
		got, err := origin.GetSiblings("name")
		want := vectorclock.VectorClock{"n1": int64(index + 1)}
		if err != nil || len(got) != 1 || got[0].Value != value || !maps.Equal(got[0].VC, want) {
			t.Fatalf("coordinator versions=%v err=%v", got, err)
		}
		for i, s := range stores {
			versions, err := s.GetSiblings("name")
			if err != nil || len(versions) != 1 || versions[0].Value != value || !maps.Equal(versions[0].VC, want) {
				t.Fatalf("peer %d versions=%v err=%v", i, versions, err)
			}
		}
	}
}

func TestInternalPutJSONValidation(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"raw body", "Parimal", http.StatusBadRequest},
		{"old protocol", `{"value":"old","ts":123}`, http.StatusBadRequest},
		{"negative counter", `{"value":"x","vc":{"n1":-1}}`, http.StatusBadRequest},
		{"empty node id", `{"value":"x","vc":{"":1}}`, http.StatusBadRequest},
		{"missing vc", `{"value":"Parimal"}`, http.StatusBadRequest},
		{"missing value", `{"vc":{"n1":1}}`, http.StatusBadRequest},
		{"null vc", `{"value":"Parimal","vc":null}`, http.StatusBadRequest},
		{"null value", `{"value":null,"vc":{"n1":1}}`, http.StatusBadRequest},
		{"fractional counter", `{"value":"Parimal","vc":{"n1":1.5}}`, http.StatusBadRequest},
		{"trailing JSON", `{"value":"Parimal","vc":{"n1":1}}{}`, http.StatusBadRequest},
		{"oversized value", `{"value":"` + strings.Repeat("a", (1<<20)+1) + `","vc":{"n1":1}}`, http.StatusRequestEntityTooLarge},
		{"oversized body", strings.Repeat(" ", replication.MaxWriteBytes+1), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1")
			mux := http.NewServeMux()
			mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/internal/kv/name", strings.NewReader(tc.body)))
			if response.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.status, response.Body.String())
			}
			if _, _, exists, err := s.GetWithMeta("name"); err != nil || exists {
				t.Fatalf("rejected request modified store: exists=%v err=%v", exists, err)
			}
		})
	}
}

func TestInternalPutPreservesClockAfterRestart(t *testing.T) {
	for _, counter := range []int64{0, 1720000000000000123} {
		t.Run(fmt.Sprint(counter), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			s := store.NewStore(path)
			// Internal PUT must not contact this configured, unreachable peer.
			h := NewHandler(s, log.New(io.Discard, "", 0), []string{"127.0.0.1:1"}, 2, 1, "n1")
			mux := http.NewServeMux()
			mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
			body := fmt.Sprintf(`{"value":"Parimal","vc":{"coordinator":%d}}`, counter)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/internal/kv/name", strings.NewReader(body)))
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if response.Code != http.StatusOK {
				t.Fatalf("PUT status=%d body=%s", response.Code, response.Body.String())
			}
			recovered := store.NewStore(path)
			defer recovered.Close()
			versions, err := recovered.GetSiblings("name")
			if err != nil || len(versions) != 1 || versions[0].Value != "Parimal" || !maps.Equal(versions[0].VC, vectorclock.VectorClock{"coordinator": counter}) {
				t.Fatalf("recovered=%v err=%v", versions, err)
			}
		})
	}
}

func TestInternalGetUnhealthyStore(t *testing.T) {
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	// A write to the closed WAL triggers the store's unhealthy state.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutWithClock("name", "Parimal", vectorclock.VectorClock{"n1": 1}); err == nil {
		t.Fatal("expected WAL write failure")
	}
	h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/kv/{key}", h.InternalGetHandler)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/kv/name", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("unhealthy GET status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestPutReplicatesWithCancelledClientContext(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	peerStore := store.NewStore(filepath.Join(t.TempDir(), "peer.wal"))
	defer peerStore.Close()
	peerHandler := NewHandler(peerStore, logger, nil, 1, 1, "n1")
	peerMux := http.NewServeMux()
	peerMux.HandleFunc("PUT /internal/kv/{key}", peerHandler.InternalPutHandler)
	peer := httptest.NewServer(peerMux)
	defer peer.Close()

	origin := store.NewStore(filepath.Join(t.TempDir(), "origin.wal"))
	defer origin.Close()
	h := NewHandler(origin, logger, []string{strings.TrimPrefix(peer.URL, "http://")}, 2, 1, "n1")
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The body is already available even though the client's context is cancelled.
	req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("Parimal")).WithContext(ctx)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("replication was cancelled with the client: status=%d body=%s", response.Code, response.Body.String())
	}
	original, err := origin.GetSiblings("name")
	if err != nil {
		t.Fatal(err)
	}
	versions, err := peerStore.GetSiblings("name")
	if err != nil || len(versions) != 1 || versions[0].Value != "Parimal" || !maps.Equal(versions[0].VC, original[0].VC) {
		t.Fatalf("peer versions=%v err=%v", versions, err)
	}
}
