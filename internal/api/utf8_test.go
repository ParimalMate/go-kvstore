package api

import (
	"bytes"
	"encoding/json"
	"io"
	"kvstore/internal/store"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestPutEndpointsRejectInvalidUTF8(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		body       []byte
	}{
		{"external key", "/kv/%FF", []byte("value")},
		{"external value", "/kv/key", []byte{'a', 0xff, 0xfe, 'b'}},
		{"internal key", "/internal/kv/%FF", []byte(`{"value":"ok","vc":{"n1":1}}`)},
		{"internal value", "/internal/kv/key", append(append([]byte(`{"value":"a`), 0xff, 0xfe), []byte(`b","vc":{"n1":1}}`)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			s := store.NewStore(path)
			defer s.Close()
			h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1")
			mux := http.NewServeMux()
			mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
			mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest("PUT", tc.path, bytes.NewReader(tc.body)))
			if response.Code != 400 {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			content, err := os.ReadFile(path)
			if err != nil || len(content) != 0 {
				t.Fatal("invalid request wrote WAL", err)
			}
		})
	}
}

func TestPutEndpointsPreserveTextAfterRestart(t *testing.T) {
	for _, route := range []string{"/kv/", "/internal/kv/"} {
		t.Run(route, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			s := store.NewStore(path)
			h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1")
			mux := http.NewServeMux()
			mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
			mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
			cases := map[string]string{"शहर": "पुणे 🌍", "empty": "", "nul\x00key": "a\x00b", "line\nkey": "a\nb"}
			for key, value := range cases {
				body := []byte(value)
				if route == "/internal/kv/" {
					var err error
					body, err = json.Marshal(internalPutRequest{Value: &value, VC: map[string]int64{"n2": 1}})
					if err != nil {
						t.Fatal(err)
					}
				}
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, httptest.NewRequest("PUT", route+url.PathEscape(key), bytes.NewReader(body)))
				if response.Code != 200 {
					s.Close()
					t.Fatalf("key=%q status=%d body=%s", key, response.Code, response.Body.String())
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = store.NewStore(path)
			defer s.Close()
			for key, want := range cases {
				got, exists, err := s.Get(key)
				if err != nil || !exists || got != want {
					t.Fatalf("key=%q got=%q want=%q exists=%v err=%v", key, got, want, exists, err)
				}
			}
		})
	}
}
