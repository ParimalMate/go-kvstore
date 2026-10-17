package store

import (
	"errors"
	"fmt"
	"kvstore/internal/merkle"
	"kvstore/internal/vectorclock"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestRevisionChangesOnlyWithState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s := NewStore(path)
	defer func() { s.Close() }()
	if s.Revision() != 1 {
		t.Fatal("fresh store must initialize revision once")
	}
	for _, tc := range []struct {
		name, value string
		vc          vectorclock.VectorClock
		changed     bool
		wantErr     error
	}{
		{"fresh", "first", vectorclock.VectorClock{"n1": 1}, true, nil},
		{"duplicate", "first", vectorclock.VectorClock{"n1": 1}, false, nil},
		{"equivalent clock", "first", vectorclock.VectorClock{"n1": 1, "n2": 0}, false, nil},
		{"newer", "second", vectorclock.VectorClock{"n1": 2}, true, nil},
		{"stale", "first", vectorclock.VectorClock{"n1": 1}, false, nil},
		{"collision", "different", vectorclock.VectorClock{"n1": 2}, false, ErrClockCollision},
		{"invalid text", string([]byte{0xff}), vectorclock.VectorClock{"n1": 3}, false, ErrInvalidText},
		{"concurrent", "sibling", vectorclock.VectorClock{"n2": 1}, true, nil},
		{"collapse", "resolved", vectorclock.VectorClock{"n1": 3, "n2": 1}, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := s.Revision()
			_, err := s.PutWithClock("key", tc.value, tc.vc)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v want %v", err, tc.wantErr)
			}
			want := before
			if tc.changed {
				want++
			}
			if got := s.Revision(); got != want {
				t.Fatalf("revision=%d want=%d", got, want)
			}
		})
	}
	before := s.Revision()
	if _, err := s.PutWithClock("key", "bad", vectorclock.VectorClock{"n1": -1}); err == nil {
		t.Fatal("invalid clock accepted")
	}
	if s.Revision() != before {
		t.Fatal("invalid clock changed revision")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = NewStore(path)
	if s.Revision() != 1 {
		t.Fatal("replay counted individual records")
	}
	value, exists, err := s.Get("key")
	if err != nil || !exists || value != "resolved" {
		t.Fatal(value, exists, err)
	}
	if _, err := s.PutWithClock("other", "value", vectorclock.VectorClock{"n1": 1}); err != nil {
		t.Fatal(err)
	}
	if s.Revision() != 2 {
		t.Fatal("post-replay update did not advance revision")
	}
	// A durable write failure must not publish state or a new revision.
	before = s.Revision()
	if err := s.log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutWithClock("failure", "value", vectorclock.VectorClock{"n1": 1}); err == nil {
		t.Fatal("expected closed WAL failure")
	}
	if s.Revision() != before {
		t.Fatal("failed append advanced revision")
	}
}

func TestRangeVisitsEveryKey(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	visited := 0
	s.Range(func(string, []Entry) { visited++ })
	if visited != 0 {
		t.Fatal("empty store visited keys")
	}
	for _, key := range []string{"city", "empty", "शहर"} {
		if _, err := s.PutWithClock(key, "", vectorclock.VectorClock{"n1": 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.PutWithClock("city", "Pune", vectorclock.VectorClock{"n2": 1}); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	s.Range(func(key string, versions []Entry) {
		counts[key]++
		expected := 1
		if key == "city" {
			expected = 2
		}
		if len(versions) != expected {
			t.Errorf("%s: %d versions", key, len(versions))
		}
	})
	if !reflect.DeepEqual(counts, map[string]int{"city": 1, "empty": 1, "शहर": 1}) {
		t.Fatal(counts)
	}
}

// normalize excludes informational timestamps and sibling slice ordering.
func canonicalVersions(entries []Entry) []merkle.Version {
	result := make([]merkle.Version, len(entries))
	for i, entry := range entries {
		result[i] = merkle.Version{Value: entry.Value, Clock: entry.VC}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Value < result[j].Value })
	return result
}

func TestVersionApplicationOrderConverges(t *testing.T) {
	for seed := int64(0); seed < 20; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			versions := []merkle.Version{}
			// These independent branches ensure meaningful surviving concurrent versions.
			for node := 0; node < 3; node++ {
				for count := int64(1); count <= 6; count++ {
					versions = append(versions, merkle.Version{Value: fmt.Sprintf("branch-%d-%d", node, count), Clock: map[string]int64{fmt.Sprintf("n%d", node): count}})
				}
			}
			for i := 0; i < 30; i++ {
				clock := map[string]int64{}
				for node := 0; node < 3; node++ {
					if count := rng.Int63n(6); count > 0 {
						clock[fmt.Sprintf("n%d", node)] = count
					}
				}
				// Equal clocks must carry equal values; conflicting equal-clock input is
				// rejected by design, not covered by the convergence guarantee.
				encoded, err := merkle.CanonicalClock(clock)
				if err != nil {
					t.Fatal(err)
				}
				versions = append(versions, merkle.Version{Value: fmt.Sprintf("mixed-%x", encoded), Clock: clock})
			}
			// Use a value derived solely from each clock, including branch clocks.
			for i := range versions {
				encoded, _ := merkle.CanonicalClock(versions[i].Clock)
				versions[i].Value = fmt.Sprintf("value-%x", encoded)
			}
			a := NewStore(filepath.Join(t.TempDir(), "a.wal"))
			defer a.Close()
			b := NewStore(filepath.Join(t.TempDir(), "b.wal"))
			defer b.Close()
			for _, target := range []*Store{a, b} {
				for _, i := range rng.Perm(len(versions)) {
					v := versions[i]
					if _, err := target.PutWithClock("city", v.Value, v.Clock); err != nil {
						t.Fatal(err)
					}
					if _, err := target.PutWithClock("city", v.Value, v.Clock); err != nil {
						t.Fatal("duplicate retry", err)
					}
				}
			}
			entriesA, err := a.GetSiblings("city")
			if err != nil {
				t.Fatal(err)
			}
			entriesB, err := b.GetSiblings("city")
			if err != nil {
				t.Fatal(err)
			}
			va, vb := canonicalVersions(entriesA), canonicalVersions(entriesB)
			if !reflect.DeepEqual(va, vb) {
				t.Fatalf("order changed sibling set: %v vs %v", va, vb)
			}
			ha, err := merkle.KeyHash("city", va)
			if err != nil {
				t.Fatal(err)
			}
			hb, err := merkle.KeyHash("city", vb)
			if err != nil || ha != hb {
				t.Fatal("order changed key hash", err)
			}
		})
	}
}

func TestConcurrentRevisionAndRange(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "wal"))
	defer s.Close()
	var workers sync.WaitGroup
	for i := 0; i < 10; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			for j := int64(1); j <= 10; j++ {
				if _, err := s.PutWithClock(fmt.Sprintf("key-%d", i), "value", vectorclock.VectorClock{"n1": j}); err != nil {
					t.Error(err)
				}
				s.Revision()
				s.Range(func(key string, versions []Entry) {
					if len(versions) != 1 {
						t.Errorf("%s: inconsistent versions", key)
					}
				})
			}
		}(i)
	}
	workers.Wait()
	if s.Revision() != 101 {
		t.Fatal("lost revisions", s.Revision())
	}
}
