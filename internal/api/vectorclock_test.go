package api

import (
	"encoding/json"
	"fmt"
	"io"
	"kvstore/internal/replication"
	"kvstore/internal/store"
	"kvstore/internal/vectorclock"
	"log"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func clockMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
	mux.HandleFunc("GET /kv/{key}", h.GetHandler)
	mux.HandleFunc("PUT /internal/kv/{key}", h.InternalPutHandler)
	mux.HandleFunc("GET /internal/kv/{key}", h.InternalGetHandler)
	return mux
}

func clockRequest(mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	result := httptest.NewRecorder()
	mux.ServeHTTP(result, httptest.NewRequest(method, path, strings.NewReader(body)))
	return result
}

func TestConflictExchangeAndResolution(t *testing.T) {
	stores := make([]*store.Store, 2)
	muxes := make([]*http.ServeMux, 2)
	paths := make([]string, 2)
	for i := range stores {
		paths[i] = filepath.Join(t.TempDir(), "wal")
		stores[i] = store.NewStore(paths[i])
		defer stores[i].Close()
		muxes[i] = clockMux(NewHandler(stores[i], log.New(io.Discard, "", 0), nil, 1, 1, fmt.Sprintf("n%d", i+1)))
	}
	// Independent external writes before either node receives the other's version.
	for i, value := range []string{"Mumbai", "Pune"} {
		if response := clockRequest(muxes[i], "PUT", "/kv/city", value); response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
	}
	versions := make([]replication.Version, 2)
	for i := range stores {
		entries, err := stores[i].GetSiblings("city")
		if err != nil {
			t.Fatal(err)
		}
		versions[i] = toWire(entries)[0]
	}
	// Deliver each original version through the real internal handler, twice.
	for i := range stores {
		body, err := json.Marshal(versions[1-i])
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			response := clockRequest(muxes[i], "PUT", "/internal/kv/city", string(body))
			if response.Code != 200 {
				t.Fatal(response.Code, response.Body.String())
			}
		}
		internal := clockRequest(muxes[i], "GET", "/internal/kv/city", "")
		var siblings []replication.Version
		if err := json.Unmarshal(internal.Body.Bytes(), &siblings); err != nil || len(siblings) != 2 {
			t.Fatalf("internal siblings=%v error=%v", siblings, err)
		}
		external := clockRequest(muxes[i], "GET", "/kv/city", "")
		if external.Code != http.StatusMultipleChoices {
			t.Fatalf("wanted Multiple Choices, got %d %s", external.Code, external.Body.String())
		}
		var alternatives []replication.Version
		if err := json.Unmarshal(external.Body.Bytes(), &alternatives); err != nil {
			t.Fatal(err)
		}
		if len(alternatives) != 2 {
			t.Fatalf("expected both alternatives, got %v", alternatives)
		}
		for _, want := range versions {
			found := false
			for _, got := range alternatives {
				if got.Value == want.Value && maps.Equal(got.VC, want.VC) {
					found = true
				}
			}
			if !found {
				t.Fatalf("conflict response lost %v: %v", want, alternatives)
			}
		}
	}
	if response := clockRequest(muxes[0], "PUT", "/kv/city", "Chennai"); response.Code != 200 {
		t.Fatal(response.Code)
	}
	entries, err := stores[0].GetSiblings("city")
	if err != nil || len(entries) != 1 || !maps.Equal(entries[0].VC, vectorclock.VectorClock{"n1": 2, "n2": 1}) {
		t.Fatalf("resolved versions=%v err=%v", entries, err)
	}
	body, _ := json.Marshal(toWire(entries)[0])
	if response := clockRequest(muxes[1], "PUT", "/internal/kv/city", string(body)); response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	for i := range stores {
		response := clockRequest(muxes[i], "GET", "/kv/city", "")
		if response.Code != 200 || response.Body.String() != "Chennai" {
			t.Fatal(response.Code, response.Body.String())
		}
		// A stale delivery cannot resurrect the old siblings.
		old, _ := json.Marshal(versions[i])
		if response := clockRequest(muxes[i], "PUT", "/internal/kv/city", string(old)); response.Code != 200 {
			t.Fatal(response.Code)
		}
		recovered := store.NewStore(paths[i])
		recoveredEntries, err := recovered.GetSiblings("city")
		recovered.Close()
		if err != nil || len(recoveredEntries) != 1 || recoveredEntries[0].Value != "Chennai" || !maps.Equal(recoveredEntries[0].VC, entries[0].VC) {
			t.Fatalf("recovered=%v error=%v", recoveredEntries, err)
		}
	}
}

func TestConcurrentCoordinatorWritesAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s := store.NewStore(path)
	mux := clockMux(NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "writer"))
	var wg sync.WaitGroup
	const count = 30
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			response := clockRequest(mux, "PUT", "/kv/key", fmt.Sprint(i))
			if response.Code != 200 {
				t.Errorf("write status %d: %s", response.Code, response.Body.String())
			}
		}(i)
	}
	wg.Wait()
	entries, err := s.GetSiblings("key")
	if err != nil || len(entries) != 1 || entries[0].VC["writer"] != count {
		t.Fatalf("versions=%v err=%v", entries, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := store.NewStore(path)
	defer recovered.Close()
	mux = clockMux(NewHandler(recovered, log.New(io.Discard, "", 0), nil, 1, 1, "writer"))
	if response := clockRequest(mux, "PUT", "/kv/key", "after restart"); response.Code != 200 {
		t.Fatal(response.Code)
	}
	entries, err = recovered.GetSiblings("key")
	if err != nil || len(entries) != 1 || entries[0].VC["writer"] != count+1 {
		t.Fatalf("recovered versions=%v err=%v", entries, err)
	}
}

func TestCoordinatorCounterOverflow(t *testing.T) {
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	if _, err := s.PutWithClock("key", "old", vectorclock.VectorClock{"n1": math.MaxInt64}); err != nil {
		t.Fatal(err)
	}
	mux := clockMux(NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1"))
	response := clockRequest(mux, "PUT", "/kv/key", "new")
	if response.Code != 500 {
		t.Fatal(response.Code)
	}
	versions, err := s.GetSiblings("key")
	if err != nil || len(versions) != 1 || versions[0].Value != "old" {
		t.Fatal(versions, err)
	}
}

// A version may look current within one replica but be dominated by a version
// returned by another. Pruning must cover the whole quorum, without read repair.
func TestQuorumPrunesAcrossReplicaSiblingLists(t *testing.T) {
	for _, resolved := range []bool{false, true} {
		t.Run(fmt.Sprintf("resolved=%t", resolved), func(t *testing.T) {
			s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
			defer s.Close()
			original, err := s.PutWithClock("city", "old-A", vectorclock.VectorClock{"n1": 1})
			if err != nil {
				t.Fatal(err)
			}
			a := replication.Version{Value: "new-A", VC: vectorclock.VectorClock{"n1": 2}}
			b := replication.Version{Value: "B", VC: vectorclock.VectorClock{"n2": 1}}
			answers := [][]replication.Version{{a, b}, {b, {Value: "old-A", VC: vectorclock.VectorClock{"n1": 1}}}}
			if resolved {
				answers[1] = append(answers[1], replication.Version{Value: "resolved", VC: vectorclock.VectorClock{"n1": 3, "n2": 1}})
			}
			var peers []string
			for _, answer := range answers {
				peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(answer); err != nil {
						t.Error(err)
					}
				}))
				defer peer.Close()
				peers = append(peers, strings.TrimPrefix(peer.URL, "http://"))
			}
			mux := clockMux(NewHandler(s, log.New(io.Discard, "", 0), peers, 2, 3, "n1"))
			response := clockRequest(mux, "GET", "/kv/city", "")
			if resolved {
				if response.Code != 200 || response.Body.String() != "resolved" {
					t.Fatalf("single survivor: %d %s", response.Code, response.Body.String())
				}
			} else {
				if response.Code != http.StatusMultipleChoices || response.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("conflict: %d %s", response.Code, response.Body.String())
				}
				var survivors []replication.Version
				if err := json.Unmarshal(response.Body.Bytes(), &survivors); err != nil {
					t.Fatal(err)
				}
				if len(survivors) != 2 {
					t.Fatalf("expected two maximal versions: %v", survivors)
				}
				for _, want := range []replication.Version{a, b} {
					found := false
					for _, got := range survivors {
						if got.Value == want.Value && maps.Equal(got.VC, want.VC) {
							found = true
						}
					}
					if !found {
						t.Fatalf("missing %v in %v", want, survivors)
					}
				}
			}
			local, err := s.GetSiblings("city")
			if err != nil || len(local) != 1 || local[0].Value != original[0].Value || !maps.Equal(local[0].VC, original[0].VC) || local[0].Ts != original[0].Ts {
				t.Fatalf("read unexpectedly changed local state: %v %v", local, err)
			}
		})
	}
}
