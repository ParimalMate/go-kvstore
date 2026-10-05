package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"kvstore/internal/replication"
	"log"
	"net/http"
	"sync"
	"time"

	"kvstore/internal/store"
)

type Handler struct {
	store  *store.Store
	logger *log.Logger
	peers  []string
	w      int
	r      int
}

type replicationFailure struct {
	Peer  string `json:"peer"`
	Error string `json:"error"`
}

type replicationResponse struct {
	Replicated []string             `json:"replicated"`
	Failed     []replicationFailure `json:"failed"`
}

type internalGetResponse struct {
	Value  string `json:"value"`
	Ts     int64  `json:"ts"`
	Exists bool   `json:"exists"`
}

// Pointers distinguish missing/null fields from an empty value or timestamp zero.
type internalPutRequest struct {
	Value *string `json:"value"`
	Ts    *int64  `json:"ts"`
}

func NewHandler(s *store.Store, logger *log.Logger, peers []string, w, r int) *Handler {
	return &Handler{
		store:  s,
		logger: logger,
		peers:  peers,
		w:      w,
		r:      r,
	}
}

func (h *Handler) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("GET key=%q", key)

	value, ts, exists, err := h.store.GetWithMeta(key)
	if err != nil {
		http.Error(w, "store is unhealthy", http.StatusInternalServerError)
		return
	}

	answers := 1 // The healthy local lookup counts, including a missing key.
	if h.r > 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		results := replication.ReadFanOut(ctx, h.peers, key)
		logged := make(chan replication.ReadResult, len(h.peers))
		// Keep logging every attempt, even after the handler has enough answers.
		go func() {
			defer cancel()
			defer close(logged)
			for result := range results {
				if result.Err != nil {
					h.logger.Printf("read key=%q -> %s FAILED (%s): %v", key, result.Peer, result.Latency, result.Err)
				} else {
					h.logger.Printf("read key=%q -> %s OK (%s) ts=%d exists=%t", key, result.Peer, result.Latency, result.Ts, result.Exists)
				}
				logged <- result
			}
		}()
		// Each HTTP request has the shared two-second deadline. The channel
		// closes when all attempts finish, even when too few peers succeed.
		for result := range logged {
			if result.Err != nil {
				continue
			}
			answers++
			if result.Exists && (!exists || result.Ts > ts) {
				value, ts, exists = result.Value, result.Ts, true
			}
			if answers >= h.r {
				break
			}
		}
	}
	if answers < h.r {
		h.logger.Printf("READ quorum not reached key=%q answers=%d R=%d", key, answers, h.r)
		http.Error(w, "read quorum not reached", http.StatusServiceUnavailable)
		return
	}
	h.logger.Printf("READ quorum reached key=%q answers=%d R=%d selected_ts=%d exists=%t", key, answers, h.r, ts, exists)
	if !exists {
		http.NotFound(w, r)
		return
	}

	w.Write([]byte(value))
}

func (h *Handler) InternalGetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("INTERNAL GET key=%q", key)

	value, ts, exists, err := h.store.GetWithMeta(key)
	if err != nil {
		http.Error(w, "store is unhealthy", http.StatusInternalServerError)
		return
	}

	response := internalGetResponse{
		Value:  value,
		Ts:     ts,
		Exists: exists,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Printf("failed to encode internal GET response: %v", err)
	}
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

	// Generate once so the local write and every replica use the same timestamp.
	ts := time.Now().UnixNano()
	err = h.store.PutAt(key, value, ts)
	if err != nil {
		http.Error(w, "failed to store value", http.StatusInternalServerError)
		return
	}

	// The collector owns cancellation, so returning a quorum response does not
	// cancel writes that are still running on slower peers.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	results := replication.FanOut(
		ctx,
		h.peers,
		key,
		value,
		ts,
	)

	var mu sync.Mutex
	replicated := make([]string, 0, len(h.peers))
	failed := make([]replicationFailure, 0, len(h.peers))
	done := make(chan struct{})
	var once sync.Once
	if h.w == 1 {
		once.Do(func() { close(done) }) // The local durable write is enough.
	}

	go func() {
		defer cancel()
		for result := range results {
			mu.Lock()
			if result.Err != nil {
				failed = append(failed, replicationFailure{Peer: result.Peer, Error: result.Err.Error()})
			} else {
				replicated = append(replicated, result.Peer)
				if len(replicated) >= h.w-1 {
					once.Do(func() { close(done) })
				}
			}
			mu.Unlock()

			// Keep draining and logging after the client has received its response.
			if result.Err != nil {
				h.logger.Printf("replicate key=%q -> %s FAILED (%s): %v", key, result.Peer, result.Latency, result.Err)
			} else {
				h.logger.Printf("replicate key=%q -> %s OK (%s)", key, result.Peer, result.Latency)
			}
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	// Quorum and cancellation can become ready together. Check the protected
	// count before choosing a status, and copy slices before releasing the lock.
	mu.Lock()
	acks := 1 + len(replicated)
	response := replicationResponse{
		Replicated: append([]string{}, replicated...),
		Failed:     append([]replicationFailure{}, failed...),
	}
	mu.Unlock()
	if acks >= h.w {
		h.logger.Printf("WRITE quorum reached key=%q acks=%d W=%d ts=%d", key, acks, h.w, ts)
		w.Write([]byte("OK"))
		return
	}
	h.logger.Printf("WRITE quorum not reached key=%q acks=%d W=%d ts=%d", key, acks, h.w, ts)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Printf("failed to encode replication response: %v", err)
	}
}

func (h *Handler) InternalPutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("INTERNAL PUT key=%q", key)

	// JSON can expand a byte into six characters (for example, \u0000).
	// Allow encoding overhead, then enforce the 1 MiB decoded-value limit.
	r.Body = http.MaxBytesReader(w, r.Body, 6*(1<<20)+1024)

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

	var write internalPutRequest
	if err := json.Unmarshal(body, &write); err != nil {
		http.Error(w, "invalid replication JSON", http.StatusBadRequest)
		return
	}
	if write.Value == nil || write.Ts == nil {
		http.Error(w, "value and ts are required", http.StatusBadRequest)
		return
	}
	if len(*write.Value) > 1<<20 {
		http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Preserve the coordinator's timestamp when storing the replicated value.
	// IMPORTANT: this handler never forwards the write to peers
	if err := h.store.PutAt(key, *write.Value, *write.Ts); err != nil {
		http.Error(w, "failed to store value", http.StatusInternalServerError)
		return
	}

	w.Write([]byte("OK"))
}
