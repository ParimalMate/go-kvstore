package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"kvstore/internal/replication"
	"kvstore/internal/vectorclock"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"kvstore/internal/store"
)

type Handler struct {
	nodeID  string
	writeMu sync.Mutex // Serializes local clock creation with incoming peer writes.
	store   *store.Store
	logger  *log.Logger
	peers   []string
	w       int
	r       int
}

type replicationFailure struct {
	Peer  string `json:"peer"`
	Error string `json:"error"`
}

type replicationResponse struct {
	Replicated []string             `json:"replicated"`
	Failed     []replicationFailure `json:"failed"`
}

// Value is a pointer so an empty string is distinct from an absent field.
type internalPutRequest struct {
	Value *string                 `json:"value"`
	VC    vectorclock.VectorClock `json:"vc"`
}

func NewHandler(s *store.Store, logger *log.Logger, peers []string, w, r int, nodeID string) *Handler {
	if nodeID == "" {
		panic("handler requires a node ID")
	}
	return &Handler{
		nodeID: nodeID,
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

	versions, err := h.store.GetSiblings(key)
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
					h.logger.Printf("read key=%q -> %s OK (%s) clocks=%v", key, result.Peer, result.Latency, clockSummary(result.Versions))
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
			for _, version := range result.Versions {
				versions = append(versions, store.Entry{Value: version.Value, VC: version.VC})
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
	survivors, err := store.Reconcile(versions)
	if err != nil {
		h.logger.Printf("READ inconsistent clocks key=%q: %v", key, err)
		http.Error(w, "inconsistent version metadata", http.StatusBadGateway)
		return
	}
	h.logger.Printf("READ quorum reached key=%q answers=%d R=%d clocks=%v", key, answers, h.r, clockSummary(toWire(survivors)))
	if len(survivors) == 0 {
		http.NotFound(w, r)
		return
	}
	if len(survivors) > 1 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultipleChoices)
		if err := json.NewEncoder(w).Encode(toWire(survivors)); err != nil {
			h.logger.Printf("failed to encode conflict: %v", err)
		}
		return
	}
	w.Write([]byte(survivors[0].Value))
}

// clockSummary logs causal metadata without logging user values.
func clockSummary(versions []replication.Version) []vectorclock.VectorClock {
	clocks := make([]vectorclock.VectorClock, 0, len(versions))
	for _, version := range versions {
		clocks = append(clocks, version.VC)
	}
	return clocks
}

// toWire excludes node-local storage timestamps from the peer protocol.
func toWire(entries []store.Entry) []replication.Version {
	result := make([]replication.Version, 0, len(entries))
	for _, entry := range entries {
		result = append(result, replication.Version{Value: entry.Value, VC: vectorclock.Merge(entry.VC, nil)})
	}
	return result
}

func (h *Handler) InternalGetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("INTERNAL GET key=%q", key)

	versions, err := h.store.GetSiblings(key)
	if err != nil {
		http.Error(w, "store is unhealthy", http.StatusInternalServerError)
		return
	}

	response := toWire(versions)
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

	vc, err := h.coordinateWrite(key, value)
	if err != nil {
		h.logger.Printf("local PUT failed key=%q: %v", key, err)
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
		vc,
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
		h.logger.Printf("WRITE quorum reached key=%q acks=%d W=%d vc=%v", key, acks, h.w, vc)
		w.Write([]byte("OK"))
		return
	}
	h.logger.Printf("WRITE quorum not reached key=%q acks=%d W=%d vc=%v", key, acks, h.w, vc)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Printf("failed to encode replication response: %v", err)
	}
}

// coordinateWrite keeps read/join/increment/store together for this node.
// The same mutex also guards internal PUTs, but no network call holds it.
func (h *Handler) coordinateWrite(key, value string) (vectorclock.VectorClock, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	siblings, err := h.store.GetSiblings(key)
	if err != nil {
		return nil, err
	}
	vc := make(vectorclock.VectorClock)
	for _, sibling := range siblings {
		vc = vectorclock.Merge(vc, sibling.VC)
	}
	if vc[h.nodeID] == math.MaxInt64 {
		return nil, errors.New("vector clock counter exhausted")
	}
	vc[h.nodeID]++
	if err := replication.ValidateVersion(replication.Version{Value: value, VC: vc}); err != nil {
		return nil, err
	}
	if _, err := h.store.PutWithClock(key, value, vc); err != nil {
		return nil, err
	}
	return vc, nil
}

func (h *Handler) InternalPutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("INTERNAL PUT key=%q", key)

	// JSON can expand a byte into six characters (for example, \u0000).
	// Allow encoding overhead, then enforce the 1 MiB decoded-value limit.
	r.Body = http.MaxBytesReader(w, r.Body, replication.MaxWriteBytes)

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
	if write.Value == nil || write.VC == nil {
		http.Error(w, "value and vc are required", http.StatusBadRequest)
		return
	}
	if len(*write.Value) > 1<<20 {
		http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
		return
	}

	if err := replication.ValidateVersion(replication.Version{Value: *write.Value, VC: write.VC}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Store the coordinator's clock unchanged, without forwarding or incrementing.
	// A stale version is acknowledged without changing state; a concurrent
	// version is retained as a sibling. Milestone 3's timestamp ordering could
	// ignore older timestamps, but could not detect and preserve concurrency.
	h.writeMu.Lock()
	_, err = h.store.PutWithClock(key, *write.Value, write.VC)
	h.writeMu.Unlock()
	if err != nil {
		if errors.Is(err, store.ErrClockCollision) {
			http.Error(w, err.Error(), http.StatusConflict)
		} else {
			http.Error(w, "failed to store value", http.StatusInternalServerError)
		}
		return
	}

	w.Write([]byte("OK"))
}
