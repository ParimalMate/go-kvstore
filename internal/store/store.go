package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kvstore/internal/vectorclock"
	"os"
	"sync"
	"time"
)

// Entry is one sibling version. Ts is informational for clocked entries.
type Entry struct {
	Value string
	VC    vectorclock.VectorClock
	Ts    int64
}

var ErrClockCollision = errors.New("equal vector clocks carry different values")

type Store struct {
	data   map[string][]Entry
	mu     sync.Mutex
	log    *os.File
	failed bool
}

type record struct {
	Op    string                  `json:"op"`
	Key   string                  `json:"key"`
	Value string                  `json:"value"`
	Ts    int64                   `json:"ts"`
	VC    vectorclock.VectorClock `json:"vc"`
}

func NewStore(filePath string) *Store {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}

	ready := false
	defer func() {
		if !ready {
			_ = file.Close()
		}
	}()
	// Validate/replay complete records before any tail truncation. Rejected
	// legacy or invalid clocked WALs must remain byte-for-byte unchanged.
	contents, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}
	complete := contents[:bytes.LastIndexByte(contents, '\n')+1]
	data := make(map[string][]Entry)
	scanner := bufio.NewScanner(bytes.NewReader(complete))
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	for scanner.Scan() {
		var rec record

		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}

		if rec.Op != "PUT" {
			continue
		}
		incoming := Entry{Value: rec.Value, VC: rec.VC, Ts: rec.Ts}
		if rec.VC == nil {
			panic(fmt.Errorf("WAL %q contains a PUT without a vector clock; Milestone 3 WALs require explicit migration; preserve this file and use a fresh --data path for Milestone 4", filePath))
		}
		if err := validateClock(rec.VC); err != nil {
			panic(err)
		}
		next, _, err := resolveSiblings(data[rec.Key], incoming)
		if err != nil {
			panic(err)
		}
		data[rec.Key] = next
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	if err := recoverWALTail(file); err != nil {
		panic(err)
	}
	ready = true
	return &Store{
		log:  file,
		data: data,
	}
}

// appendRecord requires s.mu to be held; memory changes only after durable append.
func (s *Store) appendRecord(rec record) error {
	encoded, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	encoded = append(encoded, '\n')

	n, err := s.log.Write(encoded)
	if err != nil {
		s.failed = true
		return err
	}

	if n != len(encoded) {
		s.failed = true
		return io.ErrShortWrite
	}

	if err := s.log.Sync(); err != nil {
		s.failed = true
		return err
	}

	return nil
}

func (s *Store) Get(key string) (string, bool, error) {
	value, _, exists, err := s.GetWithMeta(key)
	return value, exists, err
}

// GetWithMeta returns the value and its timestamp from the same stored entry.
func (s *Store) GetWithMeta(key string) (string, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failed {
		return "", 0, false, errors.New("store is unhealthy, restart required")
	}

	versions := s.data[key]
	if len(versions) == 0 {
		return "", 0, false, nil
	}
	if len(versions) > 1 {
		return "", 0, false, errors.New("key has concurrent siblings; use GetSiblings")
	}
	return versions[0].Value, versions[0].Ts, true, nil
}

// PutWithClock stores a version according to causal dominance, never timestamp order.
// The returned siblings and their clocks are independent copies.
func (s *Store) PutWithClock(key, value string, vc vectorclock.VectorClock) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return nil, errors.New("store is unhealthy, restart required")
	}
	if err := validateClock(vc); err != nil {
		return nil, err
	}
	// Copy the caller's map so later caller changes cannot alter stored history.
	incoming := Entry{Value: value, VC: vectorclock.Merge(vc, nil), Ts: time.Now().UnixNano()}
	next, changed, err := resolveSiblings(s.data[key], incoming)
	if err != nil {
		return nil, err
	}
	if !changed {
		return cloneEntries(next), nil
	}
	rec := record{Op: "PUT", Key: key, Value: value, VC: incoming.VC, Ts: incoming.Ts}
	if err := s.appendRecord(rec); err != nil {
		return nil, err
	}
	s.data[key] = next
	return cloneEntries(next), nil
}

// GetSiblings returns all versions; an absent key returns an empty slice.
func (s *Store) GetSiblings(key string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return nil, errors.New("store is unhealthy, restart required")
	}
	return cloneEntries(s.data[key]), nil
}

func validateClock(vc vectorclock.VectorClock) error {
	for _, count := range vc {
		if count < 0 {
			return errors.New("vector clock counters must be nonnegative")
		}
	}
	return nil
}

// resolveSiblings is the single dominance rule for live clocked writes and replay.
// It never modifies the input slice or clocks and never performs file I/O.
func resolveSiblings(current []Entry, incoming Entry) ([]Entry, bool, error) {
	// First decide whether to reject the incoming version before dropping anything.
	for _, sibling := range current {
		relation := vectorclock.Compare(incoming.VC, sibling.VC)
		if relation == vectorclock.Before {
			return current, false, nil
		}
		if relation == vectorclock.Equal {
			if incoming.Value != sibling.Value {
				return nil, false, ErrClockCollision
			}
			return current, false, nil // Identical delivery; preserve first-stored timestamp.
		}
	}
	next := make([]Entry, 0, len(current)+1)
	for _, sibling := range current {
		if vectorclock.Compare(incoming.VC, sibling.VC) != vectorclock.After {
			next = append(next, sibling)
		}
	}
	next = append(next, incoming)
	return next, true, nil
}

// Reconcile applies the store's dominance rule to versions collected by a read.
// It performs no persistence and returns independently owned entries.
func Reconcile(versions []Entry) ([]Entry, error) {
	result := make([]Entry, 0, len(versions))
	for _, version := range versions {
		if err := validateClock(version.VC); err != nil {
			return nil, err
		}
		next, _, err := resolveSiblings(result, version)
		if err != nil {
			return nil, err
		}
		result = next
	}
	return cloneEntries(result), nil
}

func cloneEntries(entries []Entry) []Entry {
	result := make([]Entry, len(entries))
	for i, version := range entries {
		result[i] = version
		if version.VC != nil {
			result[i].VC = vectorclock.Merge(version.VC, nil)
		}
	}
	return result
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.log.Close()
}

func recoverWALTail(file *os.File) error {
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	contents, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	lastNewline := bytes.LastIndexByte(contents, '\n')
	validSize := int64(lastNewline + 1)

	if validSize != int64(len(contents)) {
		if err := file.Truncate(validSize); err != nil {
			return err
		}
	}

	_, err = file.Seek(0, io.SeekStart)
	return err
}
