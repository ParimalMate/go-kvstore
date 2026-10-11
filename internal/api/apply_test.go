package api

import (
	"errors"
	"fmt"
	"io"
	"kvstore/internal/antientropy"
	"kvstore/internal/replication"
	"kvstore/internal/store"
	"kvstore/internal/vectorclock"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var _ antientropy.Applier = (*Handler)(nil)

func TestApplyVersionsCountsAndErrors(t *testing.T) {
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1")
	a := replication.Version{Value: "a", VC: vectorclock.VectorClock{"n2": 1}}
	b := replication.Version{Value: "b", VC: vectorclock.VectorClock{"n2": 2}}
	bad := replication.Version{Value: "collision", VC: b.VC}
	changed, err := h.ApplyVersions("key", []replication.Version{a, a, b, bad})
	if changed != 2 || !errors.Is(err, store.ErrClockCollision) {
		t.Fatal(changed, err)
	}
	// The caller continues after the offending version; no permanent failure.
	c := replication.Version{Value: "c", VC: vectorclock.VectorClock{"n2": 3}}
	if changed, err = h.ApplyVersions("key", []replication.Version{c}); changed != 1 || err != nil {
		t.Fatal(changed, err)
	}
	if changed, err = h.ApplyVersions("key", []replication.Version{a, b, c}); changed != 0 || err != nil {
		t.Fatal(changed, err)
	}
	if got, _, err := s.Get("key"); got != "c" || err != nil {
		t.Fatal(got, err)
	}
	if changed, err = h.ApplyVersions("key", []replication.Version{{Value: "sibling", VC: vectorclock.VectorClock{"n3": 1}}}); changed != 1 || err != nil {
		t.Fatal(changed, err)
	}
	if changed, err = h.ApplyVersions("key", []replication.Version{{Value: "resolved", VC: vectorclock.VectorClock{"n2": 4, "n3": 1}}}); changed != 1 || err != nil {
		t.Fatal(changed, err)
	}
	if changed, err = h.ApplyVersions("key", nil); changed != 0 || err != nil {
		t.Fatal(changed, err)
	}
	if changed, err = h.ApplyVersions("key", []replication.Version{{Value: string([]byte{0xff}), VC: c.VC}}); changed != 0 || !errors.Is(err, store.ErrInvalidText) {
		t.Fatal(changed, err)
	}
}

func TestApplyVersionsMixedWithCoordinatorPUTs(t *testing.T) {
	s := store.NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	h := NewHandler(s, log.New(io.Discard, "", 0), nil, 1, 1, "n1")
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", h.PutHandler)
	var workers sync.WaitGroup
	for i := 0; i < 50; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			if i%2 == 0 {
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, httptest.NewRequest("PUT", "/kv/key", strings.NewReader(fmt.Sprintf("local-%d", i))))
				if response.Code != 200 {
					t.Errorf("coordinator status %d: %s", response.Code, response.Body.String())
				}
			} else {
				version := replication.Version{Value: fmt.Sprintf("remote-%d", i), VC: vectorclock.VectorClock{fmt.Sprintf("peer%d", i): 1}}
				if _, err := h.ApplyVersions("key", []replication.Version{version}); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	workers.Wait()
	// A final coordinator write joins all remaining remote evidence. Its counter
	// proves no local increment was lost, and every independent repair is included.
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest("PUT", "/kv/key", strings.NewReader("final")))
	versions, err := s.GetSiblings("key")
	if response.Code != 200 || err != nil || len(versions) != 1 {
		t.Fatal(response.Code, versions, err)
	}
	if versions[0].VC["n1"] != 26 {
		t.Fatal("lost coordinator increments", versions)
	}
	for i := 1; i < 50; i += 2 {
		if versions[0].VC[fmt.Sprintf("peer%d", i)] != 1 {
			t.Fatal("lost repair history", i, versions)
		}
	}
}
