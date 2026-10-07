package store

import (
	"fmt"
	"kvstore/internal/vectorclock"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestPutGet(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "test.log")
	store := NewStore(filePath)
	defer store.Close()

	_, err := store.PutWithClock("name", "Parimal", vectorclock.VectorClock{"n1": 1})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, exists, err := store.Get("name")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !exists {
		t.Fatal("Expected key to exist")
	}

	if value != "Parimal" {
		t.Fatalf("Expected Parimal, got %s", value)
	}
}

func TestWALRecovery(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "recovery.log")

	store := NewStore(filePath)

	_, err := store.PutWithClock("name", "Parimal", vectorclock.VectorClock{"n1": 1})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen the same WAL file to rebuild the in-memory map.
	recoveredStore := NewStore(filePath)
	defer recoveredStore.Close()

	value, exists, err := recoveredStore.Get("name")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !exists {
		t.Fatal("Expected recovered key to exist")
	}

	if value != "Parimal" {
		t.Fatalf("Expected Parimal, got %s", value)
	}
}

func TestConcurrentAccess(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "concurrent.log")
	store := NewStore(filePath)
	defer store.Close()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			// Write to the store.
			if _, err := store.PutWithClock(key, value, vectorclock.VectorClock{"n1": 1}); err != nil {
				t.Errorf("Put failed: %v", err)
				return
			}

			// Read from the store.
			got, exists, err := store.Get(key)
			if err != nil {
				t.Errorf("Get failed for key %s: %v", key, err)
				return
			}
			if !exists {
				t.Errorf("Key %s not found", key)
				return
			}

			if got != value {
				t.Errorf("For key %s, expected %s, got %s",
					key, value, got)
			}
		}(i)
	}

	wg.Wait()
}

func TestWALTailRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kvlog")

	store := NewStore(path)

	if _, err := store.PutWithClock("a", "100", vectorclock.VectorClock{"n1": 1}); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash during a WAL write.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = file.WriteString(`{"op":"PUT","key":"broken","val`)
	if err != nil {
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart the store.
	store = NewStore(path)
	defer store.Close()

	value, ok, err := store.Get("a")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok || value != "100" {
		t.Fatalf("expected a=100, got %q, exists=%v", value, ok)
	}

	_, ok, err = store.Get("broken")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if ok {
		t.Fatal("incomplete WAL record should not be recovered")
	}
}
func TestUnhealthyStore(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "unhealthy.log")
	store := NewStore(filePath)
	defer store.Close()

	// Simulate a WAL failure.
	store.mu.Lock()
	store.failed = true
	store.mu.Unlock()

	// PUT should be rejected.
	if _, err := store.PutWithClock("name", "Parimal", vectorclock.VectorClock{"n1": 1}); err == nil {
		t.Fatal("Expected Put to fail on unhealthy store")
	}

	// GET should also return an error.
	_, _, err := store.Get("name")
	if err == nil {
		t.Fatal("Expected Get to fail on unhealthy store")
	}
}

func TestGetMissingKey(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "missing.log")
	s := NewStore(filePath)
	defer s.Close()

	_, exists, err := s.Get("unknown")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if exists {
		t.Fatal("Expected missing key to not exist")
	}
}

func TestPutOverwrite(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "overwrite.log")
	s := NewStore(filePath)
	defer s.Close()

	if _, err := s.PutWithClock("name", "Parimal", vectorclock.VectorClock{"n1": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutWithClock("name", "Mate", vectorclock.VectorClock{"n1": 2}); err != nil {
		t.Fatal(err)
	}

	value, exists, err := s.Get("name")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "Mate" {
		t.Fatalf("Expected Mate, got %q, exists=%v", value, exists)
	}
}

func TestSpecialValues(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "special.log")
	s := NewStore(filePath)

	testCases := []struct {
		key   string
		value string
	}{
		{"spaces", "hello world with spaces"},
		{"empty", ""},
		{"newline", "first line\nsecond line"},
		{"unicode", "नमस्ते 世界 🌍"},
	}

	for _, tc := range testCases {
		if _, err := s.PutWithClock(tc.key, tc.value, vectorclock.VectorClock{"n1": 1}); err != nil {
			t.Fatalf("Put failed for key %q: %v", tc.key, err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the WAL to verify special values survive recovery.
	recovered := NewStore(filePath)
	defer recovered.Close()

	for _, tc := range testCases {
		got, exists, err := recovered.Get(tc.key)
		if err != nil {
			t.Fatalf("Get failed for key %q: %v", tc.key, err)
		}
		if !exists {
			t.Errorf("Expected key %q to exist", tc.key)
			continue
		}
		if got != tc.value {
			t.Errorf("For key %q, expected %q, got %q", tc.key, tc.value, got)
		}
	}
}

func TestClockMetadataRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.log")
	s := NewStore(path)
	versions, err := s.PutWithClock("name", "Parimal", vectorclock.VectorClock{"n1": 1})
	if err != nil {
		t.Fatal(err)
	}
	timestamp := versions[0].Ts
	check := func(s *Store) {
		t.Helper()
		value, ts, exists, err := s.GetWithMeta("name")
		if err != nil || !exists || value != "Parimal" || ts != timestamp {
			t.Fatalf("metadata: %q %d %t %v", value, ts, exists, err)
		}
	}
	check(s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := NewStore(path)
	defer recovered.Close()
	check(recovered)
}

func TestLegacyWALRejectedWithoutModification(t *testing.T) {
	legacy := `{"op":"PUT","key":"name","value":"old","ts":123}` + "\n"
	clocked := `{"op":"PUT","key":"name","value":"new","vc":{"n1":1},"ts":10}` + "\n"
	for name, contents := range map[string]string{
		"timestamp only":              legacy,
		"no timestamp":                `{"op":"PUT","key":"name","value":"old"}` + "\n",
		"null clock":                  `{"op":"PUT","key":"name","value":"old","vc":null}` + "\n",
		"legacy with incomplete tail": legacy + `{"op":"PUT","key":"unfinished"`,
		"mixed formats":               clocked + legacy,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.wal")
			if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			var failure any
			func() {
				defer func() { failure = recover() }()
				opened := NewStore(path)
				opened.Close()
			}()
			if failure == nil || !strings.Contains(fmt.Sprint(failure), "fresh --data path") {
				t.Fatalf("expected clear legacy failure, got %v", failure)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != contents {
				t.Fatalf("rejected WAL was modified: %v", err)
			}
		})
	}
}

func TestMetadataReadStates(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "states.log"))
	defer s.Close()
	value, ts, exists, err := s.GetWithMeta("missing")
	if err != nil || exists || value != "" || ts != 0 {
		t.Fatalf("missing: %q %d %t %v", value, ts, exists, err)
	}
	if _, err := s.PutWithClock("empty", "", vectorclock.VectorClock{"n1": 1}); err != nil {
		t.Fatal(err)
	}
	value, ts, exists, err = s.GetWithMeta("empty")
	if err != nil || !exists || value != "" || ts <= 0 {
		t.Fatalf("empty: %q %d %t %v", value, ts, exists, err)
	}
	s.mu.Lock()
	s.failed = true
	s.mu.Unlock()
	if _, err := s.PutWithClock("empty", "changed", vectorclock.VectorClock{"n1": 2}); err == nil {
		t.Fatal("unhealthy write accepted")
	}
	if _, _, _, err := s.GetWithMeta("empty"); err == nil {
		t.Fatal("unhealthy read accepted")
	}
}
