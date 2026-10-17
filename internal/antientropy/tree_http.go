package antientropy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kvstore/internal/merkle"
	"net/http"
	"unicode/utf8"
)

const MaxRequestBytes = 64 << 10
const MaxResponseBytes = 8 << 20

type nodesRequest struct {
	Level   *int  `json:"level"`
	Indexes []int `json:"indexes"`
}
type bucketsRequest struct {
	Indexes []int `json:"indexes"`
}
type BucketListing struct {
	Index int                `json:"index"`
	Keys  []merkle.KeyDigest `json:"keys"`
}
type BucketsResponse struct {
	Fanout  int             `json:"fanout"`
	Levels  int             `json:"levels"`
	Buckets []BucketListing `json:"buckets"`
}

type TreeHandler struct{ cache *SnapshotCache }

func NewTreeHandler(cache *SnapshotCache) *TreeHandler { return &TreeHandler{cache: cache} }
func (h *TreeHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/ae/nodes", h.HandleNodes)
	mux.HandleFunc("POST /internal/ae/buckets", h.HandleBuckets)
}

func validateIndexes(level int, indexes []int, limit int) error {
	if level < 0 || level > merkle.Levels {
		return fmt.Errorf("level must be between 0 and %d", merkle.Levels)
	}
	if len(indexes) == 0 || len(indexes) > limit {
		return fmt.Errorf("expected 1..%d indexes", limit)
	}
	count := 1
	for i := 0; i < level; i++ {
		count *= merkle.Fanout
	}
	seen := make(map[int]bool, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= count {
			return fmt.Errorf("index %d out of range for level %d", index, level)
		}
		if seen[index] {
			return fmt.Errorf("duplicate index %d", index)
		}
		seen[index] = true
	}
	return nil
}

func readRequest(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBytes)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var sizeError *http.MaxBytesError
		if errors.As(err, &sizeError) {
			http.Error(w, "body exceeds 64 KiB", 413)
		} else {
			http.Error(w, "cannot read body", 400)
		}
		return false
	}
	if !utf8.Valid(body) {
		http.Error(w, "invalid UTF-8", 400)
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		http.Error(w, "invalid JSON", 400)
		return false
	}
	return true
}

func writeResponse(w http.ResponseWriter, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "cannot encode response", 500)
		return
	}
	// A large bucket needs a different transport strategy; never return a
	// truncated listing which might conceal divergent keys.
	if len(body) > MaxResponseBytes {
		http.Error(w, "tree response exceeds 8 MiB", http.StatusRequestEntityTooLarge)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func (h *TreeHandler) HandleNodes(w http.ResponseWriter, r *http.Request) {
	var req nodesRequest
	if !readRequest(w, r, &req) {
		return
	}
	if req.Level == nil {
		http.Error(w, "level is required", 400)
		return
	}
	if err := validateIndexes(*req.Level, req.Indexes, 256); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	tree, err := h.cache.Tree()
	if err != nil {
		http.Error(w, "cannot build tree", 500)
		return
	}
	response := merkle.NodeBatch{Fanout: merkle.Fanout, Levels: merkle.Levels, Hashes: make([]merkle.Hash, 0, len(req.Indexes))}
	for _, index := range req.Indexes {
		hash, _ := tree.Node(*req.Level, index)
		response.Hashes = append(response.Hashes, hash)
	}
	writeResponse(w, response)
}

func (h *TreeHandler) HandleBuckets(w http.ResponseWriter, r *http.Request) {
	var req bucketsRequest
	if !readRequest(w, r, &req) {
		return
	}
	if err := validateIndexes(merkle.Levels, req.Indexes, 64); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	tree, err := h.cache.Tree()
	if err != nil {
		http.Error(w, "cannot build tree", 500)
		return
	}
	response := BucketsResponse{Fanout: merkle.Fanout, Levels: merkle.Levels, Buckets: make([]BucketListing, 0, len(req.Indexes))}
	for _, index := range req.Indexes {
		keys, _ := tree.BucketKeys(index)
		response.Buckets = append(response.Buckets, BucketListing{index, keys})
	}
	writeResponse(w, response)
}
