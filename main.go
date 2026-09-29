package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Store struct {
	data   map[string]string
	mu     sync.Mutex // Allows only one goroutine at a time to access the protected data. Go can run multiple goroutines on a single OS thread, and it can also run goroutines concurrently across multiple OS threads.
	log    *os.File   // reference to the WAL file that contains all the PUT operations which have been successfully executed
	failed bool       // mark store as unhealthy after a WAL failure
}

type record struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewStore(filePath string) *Store { // Initialise Store
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644) // Create the file if it doesn't exist; if it exists, open it for writing and append new data at the end
	if err != nil {
		panic(err)
	}

	if err := recoverWALTail(file); err != nil {
		panic(err)
	}

	data := make(map[string]string)

	// Allow WAL records up to 10 MB instead of Scanner's default 64 KB
	scanner := bufio.NewScanner(file) // Create a scanner to read the WAL one line at a time.
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	// Replay each JSON record to reconstruct the in-memory state
	for scanner.Scan() {
		var rec record

		// Skips records that cannot be decoded as valid JSON
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}

		// Apply valid PUT operations to the map
		if rec.Op == "PUT" {
			data[rec.Key] = rec.Value
		}
	}

	err = scanner.Err() // Check whether scanning stopped because of a read error.
	if err != nil {
		panic(err)
	}

	return &Store{
		log:  file,
		data: data,
	}
}

func (s *Store) Put(key string, value string) error {
	s.mu.Lock()         // Mutex Lock applied
	defer s.mu.Unlock() // Executes just before function is about to return in any way not just normally it makes sure you don't get into deadlock whenever the functions returns with errors or returns in not a regular way, access opened
	// Ensures the mutex is unlocked when Put returns.

	if s.failed {
		return errors.New("store is unhealthy, restart required")
	}

	// Convert the PUT operation into a JSON record
	rec := record{
		Op:    "PUT",
		Key:   key,
		Value: value,
	}

	encoded, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	// Append a newline so each JSON record occupies one line in the WAL
	encoded = append(encoded, '\n')

	// Write the record to the WAL before updating the in-memory map
	n, err := s.log.Write(encoded)
	if err != nil {
		s.failed = true
		return err
	}
	if n != len(encoded) {
		s.failed = true
		return io.ErrShortWrite
	}

	err = s.log.Sync() // Check for synced file in secondary storage or Flushes the WAL data to persistent storage
	if err != nil {
		s.failed = true
		return err
	}

	// Only after both write and sync have successfully executed then only we update map
	s.data[key] = value
	return nil
}

func (s *Store) Get(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failed {
		return "", false, errors.New("store is unhleahty, restart required")
	}

	value, exists := s.data[key] // when accessing map it returns two values one is the key's value and bool value of whether the key exists in the map or not
	return value, exists, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.log.Close()
}

func (s *Store) getHandler(w http.ResponseWriter, r *http.Request) { // getHandler handles GET requests for a specific key.
	// The key is extracted from the URL path, then looked up in the Store.
	key := r.PathValue("key")

	value, exists, err := s.Get(key)
	if err != nil {
		http.Error(w, "store is unhealthy", http.StatusInternalServerError)
		return
	}

	if !exists { // Return 404 when the requested key does not exist.
		http.NotFound(w, r)
		return
	}

	w.Write([]byte(value)) // Send the stored value back to the client as the response body.
}

func (s *Store) putHandler(w http.ResponseWriter, r *http.Request) { // putHandler handles PUT requests for a specific key.
	// The key comes from the URL path and the value comes from the request body.
	key := r.PathValue("key")

	// Limit incoming PUT request bodies to 1 MB
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	body, err := io.ReadAll(r.Body) // Read the value sent by the client in the HTTP request body.
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}

		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	value := string(body)

	// Store the key-value pair using the Store's Put method.
	// Put handles WAL persistence and updating the in-memory map.
	err = s.Put(key, value)
	if err != nil {
		http.Error(w, "failed to store value", http.StatusInternalServerError)
		return
	}

	// Tell the client that the PUT operation succeeded.
	w.Write([]byte("OK"))
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

func main() {
	port := flag.String("port", "8080", "HTTP server port")
	dataFile := flag.String("data", "kvlog", "WAL file path")
	flag.Parse()
	store := NewStore(*dataFile)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /kv/{key}", store.getHandler)
	mux.HandleFunc("PUT /kv/{key}", store.putHandler)

	server := &http.Server{
		Addr:    ":" + *port,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	errCh := make(chan error, 1)

	go func() {
		errCh <- server.ListenAndServe()
	}()

	fmt.Println("Server listening on port", *port)

	select {
	case <-ctx.Done():
		fmt.Println("Shutdown signal received")

	case err := <-errCh:
		_ = store.Close()
		if !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	err := server.Shutdown(shutdownCtx)
	if err != nil {
		fmt.Println("Graceful shutdown timed out:", err)
		_ = server.Close()
	}

	if err := store.Close(); err != nil {
		panic(err)
	}

	fmt.Println("Server shut down cleanly")
}
