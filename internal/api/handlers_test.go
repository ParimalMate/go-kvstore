package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kvstore/internal/store"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestInternalGetHandler(t *testing.T) {
	const timestamp int64 = 1720000000000000123
	tests := []struct {
		name   string
		value  string
		ts     int64
		exists bool
	}{
		{name: "existing", value: "Parimal", ts: timestamp, exists: true},
		{name: "empty value", value: "", ts: timestamp, exists: true},
		{name: "zero timestamp", value: "legacy", ts: 0, exists: true},
		{name: "missing", exists: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			if tc.exists {
				if err := s.PutAt("name", tc.value, tc.ts); err != nil {
					t.Fatal(err)
				}
			}
			h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1)
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
			// Decode into a separate wire-format shape and require all fields.
			var body struct {
				Value  *string `json:"value"`
				Ts     *int64  `json:"ts"`
				Exists *bool   `json:"exists"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Value == nil || body.Ts == nil || body.Exists == nil {
				t.Fatalf("missing JSON fields: %s", response.Body.String())
			}
			if *body.Value != tc.value || *body.Ts != tc.ts || *body.Exists != tc.exists {
				t.Fatalf("unexpected response: %s", response.Body.String())
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

func TestWritePreservesCoordinatorTimestamp(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	var peers []string
	var stores []*store.Store
	for i := 0; i < 2; i++ {
		s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
		defer s.Close()
		stores = append(stores, s)
		h := NewHandler(s, logger, nil, 1, 1)
		mux := http.NewServeMux()
		mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
		peer := httptest.NewServer(mux)
		defer peer.Close()
		peers = append(peers, strings.TrimPrefix(peer.URL, "http://"))
	}
	origin := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	defer origin.Close()
	h := NewHandler(origin, logger, peers, len(peers)+1, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
	for _, value := range []string{"Parimal", "", "नमस्ते\n\"quoted\"", strings.Repeat("\x00", 1<<20)} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader(value)))
		if response.Code != http.StatusOK {
			t.Fatalf("PUT status=%d body=%s", response.Code, response.Body.String())
		}
		got, ts, exists, err := origin.GetWithMeta("name")
		if err != nil || !exists || got != value || ts <= 0 {
			t.Fatalf("unexpected coordinator result: ts=%d exists=%v err=%v", ts, exists, err)
		}
		for i, s := range stores {
			peerValue, peerTs, peerExists, peerErr := s.GetWithMeta("name")
			if peerErr != nil || !peerExists || peerValue != value || peerTs != ts {
				t.Fatalf("peer %d differs: ts=%d want=%d exists=%v err=%v", i, peerTs, ts, peerExists, peerErr)
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
		{"missing ts", `{"value":"Parimal"}`, http.StatusBadRequest},
		{"missing value", `{"ts":123}`, http.StatusBadRequest},
		{"null ts", `{"value":"Parimal","ts":null}`, http.StatusBadRequest},
		{"null value", `{"value":null,"ts":123}`, http.StatusBadRequest},
		{"fractional ts", `{"value":"Parimal","ts":1.5}`, http.StatusBadRequest},
		{"trailing JSON", `{"value":"Parimal","ts":123}{}`, http.StatusBadRequest},
		{"oversized value", `{"value":"` + strings.Repeat("a", (1<<20)+1) + `","ts":123}`, http.StatusRequestEntityTooLarge},
		{"oversized body", strings.Repeat(" ", 6*(1<<20)+1025), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1)
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

func TestInternalPutPreservesTimestampAfterRestart(t *testing.T) {
	for _, ts := range []int64{0, 1720000000000000123} {
		t.Run(fmt.Sprint(ts), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			s := store.NewStore(path)
			// Internal PUT must not contact this configured, unreachable peer.
			h := NewHandler(s, log.New(io.Discard, "", 0), []string{"127.0.0.1:1"}, 2, 1)
			mux := http.NewServeMux()
			mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
			body := fmt.Sprintf(`{"value":"Parimal","ts":%d}`, ts)
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
			value, gotTs, exists, err := recovered.GetWithMeta("name")
			if err != nil || !exists || value != "Parimal" || gotTs != ts {
				t.Fatalf("recovered value=%q ts=%d want=%d exists=%v err=%v", value, gotTs, ts, exists, err)
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
	if err := s.PutAt("name", "Parimal", 123); err == nil {
		t.Fatal("expected WAL write failure")
	}
	h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1)
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
	peerHandler := NewHandler(peerStore, logger, nil, 1, 1)
	peerMux := http.NewServeMux()
	peerMux.HandleFunc("PUT /internal/kv/{key}", peerHandler.InternalPutHandler)
	peer := httptest.NewServer(peerMux)
	defer peer.Close()

	origin := store.NewStore(filepath.Join(t.TempDir(), "origin.wal"))
	defer origin.Close()
	h := NewHandler(origin, logger, []string{strings.TrimPrefix(peer.URL, "http://")}, 2, 1)
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
	_, originTs, _, _ := origin.GetWithMeta("name")
	value, ts, exists, err := peerStore.GetWithMeta("name")
	if err != nil || !exists || value != "Parimal" || ts != originTs {
		t.Fatalf("peer write: value=%q ts=%d want=%d exists=%v err=%v", value, ts, originTs, exists, err)
	}
}
