package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type Store struct {
	data   map[string]string
	mu     sync.Mutex
	log    *os.File
	failed bool
}

type record struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewStore(filePath string) *Store {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}

	if err := recoverWALTail(file); err != nil {
		panic(err)
	}

	data := make(map[string]string)

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	for scanner.Scan() {
		var rec record

		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}

		if rec.Op == "PUT" {
			data[rec.Key] = rec.Value
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failed {
		return errors.New("store is unhealthy, restart required")
	}

	rec := record{
		Op:    "PUT",
		Key:   key,
		Value: value,
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

	s.data[key] = value
	return nil
}

func (s *Store) Get(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failed {
		return "", false, errors.New("store is unhealthy, restart required")
	}

	value, exists := s.data[key]
	return value, exists, nil
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
