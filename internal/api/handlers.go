package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"kvstore/internal/replication"
	"log"
	"net/http"
	"time"

	"kvstore/internal/store"
)

type Handler struct {
	store  *store.Store
	logger *log.Logger
	peers  []string
}

type replicationFailure struct {
	Peer  string `json:"peer"`
	Error string `json:"error"`
}

type replicationResponse struct {
	Replicated []string             `json:"replicated"`
	Failed     []replicationFailure `json:"failed"`
}

func NewHandler(s *store.Store, logger *log.Logger, peers []string) *Handler {
	return &Handler{
		store:  s,
		logger: logger,
		peers:  peers,
	}
}

func (h *Handler) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("GET key=%q", key)

	value, exists, err := h.store.Get(key)
	if err != nil {
		http.Error(w, "store is unhealthy", http.StatusInternalServerError)
		return
	}

	if !exists {
		http.NotFound(w, r)
		return
	}

	w.Write([]byte(value))
}

func (h *Handler) PutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("PUT key=%q", key)

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	body, err := io.ReadAll(r.Body)
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

	err = h.store.Put(key, value)
	if err != nil {
		http.Error(w, "failed to store value", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	results := replication.FanOut(
		ctx,
		h.peers,
		key,
		value,
	)

	var replicated []string
	var failed []replicationFailure
	for result := range results {
		if result.Err != nil {
			h.logger.Printf(
				"replicate key=%q -> %s FAILED (%s): %v",
				key,
				result.Peer,
				result.Latency,
				result.Err,
			)

			failed = append(failed, replicationFailure{
				Peer:  result.Peer,
				Error: result.Err.Error(),
			})
			continue
		}
		h.logger.Printf(
			"replicate key=%q -> %s OK (%s)",
			key,
			result.Peer,
			result.Latency,
		)
		replicated = append(replicated, result.Peer)
	}

	if len(failed) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)

		response := replicationResponse{
			Replicated: replicated,
			Failed:     failed,
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			h.logger.Printf("failed to encode replication response: %v", err)
		}
		return
	}
	w.Write([]byte("OK"))
}

func (h *Handler) InternalPutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("INTERNAL PUT key=%q", key)

	// Apply the same 1 MB limit as the external PUT endpoint
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	body, err := io.ReadAll(r.Body)
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

	// Store the replicated value locally
	// IMPORTANT: this handler never forwards the write to peers
	if err := h.store.Put(key, value); err != nil {
		http.Error(w, "failed to store value", http.StatusInternalServerError)
		return
	}

	w.Write([]byte("OK"))
}
