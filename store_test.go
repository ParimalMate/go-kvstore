package main

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
