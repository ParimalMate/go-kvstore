package antientropy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kvstore/internal/merkle"
	"net/http"
	"time"
	"unicode/utf8"
)

const RequestTimeout = 5 * time.Second

// HTTPStatusError preserves non-200 status for cycle-level error classification.
type HTTPStatusError struct {
	Peer, Path string
	StatusCode int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("peer %s %s returned HTTP %d", e.Peer, e.Path, e.StatusCode)
}

type InvalidResponseError struct {
	Peer string
	Err  error
}

func (e *InvalidResponseError) Error() string {
	return fmt.Sprintf("peer %s invalid response: %v", e.Peer, e.Err)
}
func (e *InvalidResponseError) Unwrap() error { return e.Err }

type HTTPRemote struct {
	ctx      context.Context
	peer     string
	client   *http.Client
	requests int // Owned by one cycle, including comparison, data and verification.
}

func NewHTTPRemote(ctx context.Context, peer string) *HTTPRemote {
	return &HTTPRemote{ctx: ctx, peer: peer, client: &http.Client{Timeout: RequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

var _ merkle.Remote = (*HTTPRemote)(nil)

func (r *HTTPRemote) invalid(err error) error { return &InvalidResponseError{r.peer, err} }
func (r *HTTPRemote) post(path string, request, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	data, err := r.request("POST", path, body, MaxResponseBytes)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, response); err != nil {
		return r.invalid(err)
	}
	return nil
}

func (r *HTTPRemote) request(method, path string, body []byte, limit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(r.ctx, RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://"+r.peer+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	r.requests++
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// Valid responses are drained through EOF; oversize/slow streams are bounded
	// by the byte cap and context, then closed instead of drained indefinitely.
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPStatusError{r.peer, path, resp.StatusCode}
	}
	if readErr != nil {
		return nil, readErr
	}
	if int64(len(data)) > limit {
		return nil, r.invalid(fmt.Errorf("body exceeds %d bytes", limit))
	}
	if !utf8.Valid(data) {
		return nil, r.invalid(fmt.Errorf("invalid UTF-8"))
	}
	return data, nil
}

func (r *HTTPRemote) Nodes(level int, indexes []int) (merkle.NodeBatch, error) {
	var response merkle.NodeBatch
	if err := validateIndexes(level, indexes, 256); err != nil {
		return response, err
	}
	if err := r.post("/internal/ae/nodes", nodesRequest{&level, indexes}, &response); err != nil {
		return response, err
	}
	if response.Fanout != merkle.Fanout || response.Levels != merkle.Levels {
		return response, r.invalid(fmt.Errorf("incompatible tree shape"))
	}
	if len(response.Hashes) != len(indexes) {
		return response, r.invalid(fmt.Errorf("wrong hash count"))
	}
	// The protocol identifies nodes by positional order, not returned indexes.
	return response, nil
}

func (r *HTTPRemote) Buckets(indexes []int) (map[int][]merkle.KeyDigest, error) {
	if err := validateIndexes(merkle.Levels, indexes, 64); err != nil {
		return nil, err
	}
	// Pointer fields distinguish omitted hashes/keys/indexes from valid zeros.
	var response struct {
		Fanout  int `json:"fanout"`
		Levels  int `json:"levels"`
		Buckets []struct {
			Index *int `json:"index"`
			Keys  []struct {
				Key  *string      `json:"key"`
				Hash *merkle.Hash `json:"hash"`
			} `json:"keys"`
		} `json:"buckets"`
	}
	if err := r.post("/internal/ae/buckets", bucketsRequest{indexes}, &response); err != nil {
		return nil, err
	}
	if response.Fanout != merkle.Fanout || response.Levels != merkle.Levels {
		return nil, r.invalid(fmt.Errorf("incompatible tree shape"))
	}
	if len(response.Buckets) != len(indexes) {
		return nil, r.invalid(fmt.Errorf("wrong bucket count"))
	}
	result := make(map[int][]merkle.KeyDigest, len(indexes))
	for i, bucket := range response.Buckets {
		if bucket.Index == nil || *bucket.Index != indexes[i] {
			return nil, r.invalid(fmt.Errorf("bucket indexes out of order or range"))
		}
		if bucket.Keys == nil {
			return nil, r.invalid(fmt.Errorf("bucket keys array is required"))
		}
		keys := make([]merkle.KeyDigest, 0, len(bucket.Keys))
		for j, key := range bucket.Keys {
			if key.Key == nil || key.Hash == nil {
				return nil, r.invalid(fmt.Errorf("key and hash are required"))
			}
			if !utf8.ValidString(*key.Key) || merkle.BucketOf(*key.Key) != *bucket.Index {
				return nil, r.invalid(fmt.Errorf("key in wrong bucket"))
			}
			if j > 0 && *bucket.Keys[j-1].Key >= *key.Key {
				return nil, r.invalid(fmt.Errorf("keys unsorted or duplicated"))
			}
			keys = append(keys, merkle.KeyDigest{Key: *key.Key, Hash: *key.Hash})
		}
		result[*bucket.Index] = keys
	}
	return result, nil
}
