package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPutGet(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "test.log")
	store := NewStore(filePath)
	defer store.Close()

	err := store.Put("name", "Parimal")
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

	err := store.Put("name", "Parimal")
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
			if err := store.Put(key, value); err != nil {
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

	if err := store.Put("a", "100"); err != nil {
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
	if err := store.Put("name", "Parimal"); err == nil {
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

	if err := s.Put("name", "Parimal"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("name", "Mate"); err != nil {
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
		if err := s.Put(tc.key, tc.value); err != nil {
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
