package store

import (
	"encoding/json"
	"fmt"
	"kvstore/internal/vectorclock"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestPutWithClockLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s := NewStore(path)
	defer s.Close()
	steps := []struct {
		value string
		vc    vectorclock.VectorClock
		want  []string
	}{
		{"Delhi", vectorclock.VectorClock{"n1": 1}, []string{"Delhi"}},
		{"Mumbai", vectorclock.VectorClock{"n1": 2}, []string{"Mumbai"}},
		{"stale", vectorclock.VectorClock{"n1": 1}, []string{"Mumbai"}},
		{"Pune", vectorclock.VectorClock{"n1": 1, "n2": 1}, []string{"Mumbai", "Pune"}},
		{"Mumbai", vectorclock.VectorClock{"n1": 2, "n2": 0}, []string{"Mumbai", "Pune"}},
		{"Chennai", vectorclock.VectorClock{"n1": 3, "n2": 1}, []string{"Chennai"}},
	}
	for _, step := range steps {
		got, err := s.PutWithClock("city", step.value, step.vc)
		if err != nil {
			t.Fatal(err)
		}
		values := make([]string, len(got))
		for i, v := range got {
			values[i] = v.Value
		}
		if !reflect.DeepEqual(values, step.want) {
			t.Fatalf("after %s: got %v want %v", step.value, values, step.want)
		}
		// Restart after every stage to verify replay, timestamps, clocks and siblings.
		reopened := NewStore(path)
		recovered, err := reopened.GetSiblings("city")
		reopened.Close()
		if err != nil || !reflect.DeepEqual(got, recovered) {
			t.Fatalf("recovery got %#v err=%v want %#v", recovered, err, got)
		}
	}
}

func TestClockedWriteRejectsWithoutChangingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s := NewStore(path)
	defer s.Close()
	before, err := s.PutWithClock("key", "fresh", vectorclock.VectorClock{"n1": 2})
	if err != nil {
		t.Fatal(err)
	}
	walBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value     string
		vc        vectorclock.VectorClock
		wantError bool
	}{
		{"stale", vectorclock.VectorClock{"n1": 1}, false},
		{"fresh", vectorclock.VectorClock{"n1": 2}, false},
		{"different", vectorclock.VectorClock{"n1": 2}, true},
		{"negative", vectorclock.VectorClock{"n1": -1}, true},
	} {
		_, err = s.PutWithClock("key", tc.value, tc.vc)
		if (err != nil) != tc.wantError {
			t.Fatalf("%s: error=%v", tc.value, err)
		}
		got, err := s.GetSiblings("key")
		if err != nil || !reflect.DeepEqual(got, before) {
			t.Fatalf("state changed for %s", tc.value)
		}
		wal, err := os.ReadFile(path)
		if err != nil || string(wal) != string(walBefore) {
			t.Fatalf("WAL changed for %s", tc.value)
		}
	}
}

func TestSiblingCopiesAndPartialDominance(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	vc := vectorclock.VectorClock{"n1": 1}
	versions, err := s.PutWithClock("key", "A", vc)
	if err != nil {
		t.Fatal(err)
	}
	vc["n1"] = 999
	versions[0].VC["n1"] = 888
	versions[0].Value = "changed"
	_, err = s.PutWithClock("key", "B", vectorclock.VectorClock{"n2": 1})
	if err != nil {
		t.Fatal(err)
	}
	versions, err = s.PutWithClock("key", "A2", vectorclock.VectorClock{"n1": 2})
	if err != nil || len(versions) != 2 || versions[0].Value != "B" || versions[1].Value != "A2" {
		t.Fatalf("partial dominance: %#v %v", versions, err)
	}
	got, err := s.GetSiblings("key")
	if err != nil {
		t.Fatal(err)
	}
	got[0].VC["n2"] = 999
	again, err := s.GetSiblings("key")
	if err != nil || again[0].VC["n2"] != 1 {
		t.Fatal("GetSiblings exposed stored map")
	}
	if _, _, _, err := s.GetWithMeta("key"); err == nil {
		t.Fatal("legacy read silently selected one sibling")
	}
	missing, err := s.GetSiblings("missing")
	if err != nil || missing == nil || len(missing) != 0 {
		t.Fatalf("missing: %#v %v", missing, err)
	}
}

func TestClockedReplayIgnoresTimestampOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	records := []record{
		{Op: "PUT", Key: "key", Value: "fresh", VC: vectorclock.VectorClock{"n1": 2}, Ts: 1},
		{Op: "PUT", Key: "key", Value: "stale", VC: vectorclock.VectorClock{"n1": 1}, Ts: 999},
		{Op: "PUT", Key: "key", Value: "independent", VC: vectorclock.VectorClock{"n2": 1}, Ts: 0},
		{Op: "PUT", Key: "key", Value: "fresh", VC: vectorclock.VectorClock{"n1": 2}, Ts: 1000},
	}
	var contents []byte
	for _, rec := range records {
		line, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		contents = append(contents, append(line, '\n')...)
	}
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	s := NewStore(path)
	defer s.Close()
	versions, err := s.GetSiblings("key")
	if err != nil || len(versions) != 2 || versions[0].Value != "fresh" || versions[0].Ts != 1 || versions[1].Value != "independent" {
		t.Fatalf("replay: %#v %v", versions, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(contents) {
		t.Fatal("replay rewrote WAL")
	}
}

func TestClockedWriteDurabilityFailure(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "wal"))
	if _, err := s.PutWithClock("key", "old", vectorclock.VectorClock{"n1": 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutWithClock("key", "new", vectorclock.VectorClock{"n1": 2}); err == nil {
		t.Fatal("expected closed WAL error")
	}
	if s.data["key"][0].Value != "old" {
		t.Fatal("memory changed before durable append")
	}
	if _, err := s.GetSiblings("key"); err == nil {
		t.Fatal("unhealthy read accepted")
	}
	if _, err := s.PutWithClock("key", "again", vectorclock.VectorClock{"n1": 3}); err == nil {
		t.Fatal("unhealthy write accepted")
	}
}

func TestConcurrentSiblingWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s := NewStore(path)
	defer s.Close()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := fmt.Sprintf("n%d", i)
			if _, err := s.PutWithClock("key", node, vectorclock.VectorClock{node: 1}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	versions, err := s.GetSiblings("key")
	if err != nil || len(versions) != 12 {
		t.Fatalf("lost sibling: count=%d err=%v", len(versions), err)
	}
	reopened := NewStore(path)
	defer reopened.Close()
	recovered, err := reopened.GetSiblings("key")
	if err != nil || !reflect.DeepEqual(versions, recovered) {
		t.Fatal("concurrent siblings did not recover")
	}
}

func TestEmptyClockCanBeSuperseded(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	versions, err := s.PutWithClock("empty", "", nil)
	if err != nil || len(versions) != 1 || versions[0].VC == nil {
		t.Fatalf("empty clock: %#v %v", versions, err)
	}
	versions, err = s.PutWithClock("empty", "new", vectorclock.VectorClock{"n1": 1})
	if err != nil || len(versions) != 1 || versions[0].Value != "new" {
		t.Fatalf("empty-clock transition: %#v %v", versions, err)
	}
}
