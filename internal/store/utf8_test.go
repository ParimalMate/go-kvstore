package store

import (
	"bytes"
	"errors"
	"kvstore/internal/vectorclock"
	"os"
	"path/filepath"
	"testing"
)

func TestPutWithClockRejectsInvalidUTF8(t *testing.T) {
	for _, tc := range []struct{ name, key, value string }{
		{"key", string([]byte{'k', 0xff}), "value"},
		{"value", "key", string([]byte{'a', 'b', 0xff, 0xfe, 'c', 'd'})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			s := NewStore(path)
			defer s.Close()
			if _, err := s.PutWithClock("existing", "safe", vectorclock.VectorClock{"n1": 1}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.PutWithClock(tc.key, tc.value, vectorclock.VectorClock{"n1": 2}); !errors.Is(err, ErrInvalidText) {
				t.Fatalf("wanted ErrInvalidText, got %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("rejected text changed WAL")
			}
			if versions, err := s.GetSiblings(tc.key); err != nil || len(versions) != 0 {
				t.Fatal("rejected text changed memory", versions, err)
			}
		})
	}
}

func TestTextRoundTripAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s := NewStore(path)
	cases := map[string]string{"शहर": "पुणे 🌍", "empty": "", "nul\x00key": "a\x00b", "line\nkey": "first\nsecond", "replacement": "\ufffd"}
	for key, value := range cases {
		if _, err := s.PutWithClock(key, value, vectorclock.VectorClock{"n1": 1}); err != nil {
			s.Close()
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = NewStore(path)
	defer s.Close()
	for key, want := range cases {
		got, exists, err := s.Get(key)
		if err != nil || !exists || got != want {
			t.Fatalf("key=%q got=%q exists=%v err=%v want=%q", key, got, exists, err, want)
		}
	}
}
