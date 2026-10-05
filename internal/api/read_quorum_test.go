package api

import (
	"io"
	"kvstore/internal/store"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadQuorum(t *testing.T) {
	for _, tc := range []struct {
		name   string
		local  bool
		peers  []string
		r      int
		status int
		value  string
	}{
		{"fresh peer", true, []string{`{"value":"fresh","ts":200,"exists":true}`}, 2, 200, "fresh"},
		{"fresh local", true, []string{`{"value":"old","ts":50,"exists":true}`}, 2, 200, "local"},
		{"local absent", false, []string{`{"value":"fresh","ts":200,"exists":true}`}, 2, 200, "fresh"},
		{"all absent", false, []string{`{"value":"","ts":0,"exists":false}`}, 2, 404, ""},
		{"empty value exists", false, []string{`{"value":"","ts":0,"exists":true}`}, 2, 200, ""},
		{"failure is not absence", false, []string{"FAIL"}, 2, 503, ""},
		{"malformed is not an answer", true, []string{`{"value":"bad"}`}, 2, 503, ""},
		{"other peer rescues failure", true, []string{"FAIL", `{"value":"fresh","ts":200,"exists":true}`}, 2, 200, "fresh"},
		{"need all three", true, []string{`{"value":"middle","ts":150,"exists":true}`, `{"value":"fresh","ts":200,"exists":true}`}, 3, 200, "fresh"},
		{"R one", true, nil, 1, 200, "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			if tc.local {
				if err := s.PutAt("name", "local", 100); err != nil {
					t.Fatal(err)
				}
			}
			var peers []string
			for _, body := range tc.peers {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "GET" || r.URL.Path != "/internal/kv/name" {
						t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					}
					if body == "FAIL" {
						http.Error(w, "unavailable", 500)
						return
					}
					io.WriteString(w, body)
				}))
				defer server.Close()
				peers = append(peers, strings.TrimPrefix(server.URL, "http://"))
			}
			h := NewHandler(s, log.New(io.Discard, "", 0), peers, len(peers)+1, tc.r)
			mux := http.NewServeMux()
			mux.HandleFunc("GET /kv/{key}", h.GetHandler)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest("GET", "/kv/name", nil))
			if response.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.status, response.Body.String())
			}
			if tc.status == 200 && response.Body.String() != tc.value {
				t.Fatalf("value=%q want=%q", response.Body.String(), tc.value)
			}
		})
	}
}

func TestReadQuorumSlowPeer(t *testing.T) {
	for _, enough := range []bool{true, false} {
		t.Run(map[bool]string{true: "early response", false: "deadline"}[enough], func(t *testing.T) {
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
			defer slow.Close()
			peers := []string{strings.TrimPrefix(slow.URL, "http://")}
			if enough {
				fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.WriteString(w, `{"value":"fresh","ts":200,"exists":true}`)
				}))
				defer fast.Close()
				peers = append(peers, strings.TrimPrefix(fast.URL, "http://"))
			}
			logs := make(quorumLog, 16)
			h := NewHandler(s, log.New(logs, "", 0), peers, 2, 2)
			req := httptest.NewRequest("GET", "/kv/name", nil)
			req.SetPathValue("key", "name")
			response := httptest.NewRecorder()
			start := time.Now()
			h.GetHandler(response, req)
			elapsed := time.Since(start)
			if enough {
				if response.Code != 200 || elapsed > time.Second {
					t.Fatalf("status=%d elapsed=%s", response.Code, elapsed)
				}
			} else if response.Code != 503 || elapsed < 1800*time.Millisecond || elapsed > 4*time.Second {
				t.Fatalf("status=%d elapsed=%s", response.Code, elapsed)
			}
			logs.waitFor(t, "FAILED")
		})
	}
}
