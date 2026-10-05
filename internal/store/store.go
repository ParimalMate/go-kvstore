package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

type entry struct {
	Value string
	Ts    int64
}

type Store struct {
	data   map[string]entry
	mu     sync.Mutex
	log    *os.File
	failed bool
}

type record struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Ts    int64  `json:"ts"`
}

func NewStore(filePath string) *Store {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}

	if err := recoverWALTail(file); err != nil {
		panic(err)
	}

	data := make(map[string]entry)

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	for scanner.Scan() {
		var rec record

		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}

		if rec.Op == "PUT" {
			current, exists := data[rec.Key]
			if !exists || rec.Ts >= current.Ts {
				data[rec.Key] = entry{Value: rec.Value, Ts: rec.Ts}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return &Store{
		log:  file,
		data: data,
	}
}

func (s *Store) Put(key string, value string) error {
	return s.PutAt(key, value, time.Now().UnixNano())
}

// PutAt persists the supplied version unless a higher timestamp is already stored.
func (s *Store) PutAt(key string, value string, ts int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failed {
		return errors.New("store is unhealthy, restart required")
	}
	if current, exists := s.data[key]; exists && ts < current.Ts {
		return nil // A delayed older write must not replace a newer durable value.
	}

	rec := record{
		Op:    "PUT",
		Key:   key,
		Value: value,
		Ts:    ts,
	}

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

	s.data[key] = entry{Value: value, Ts: ts}
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

	value, exists := s.data[key]
	return value.Value, value.Ts, exists, nil
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
