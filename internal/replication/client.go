package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kvstore/internal/vectorclock"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Result struct {
	Peer    string
	Err     error
	Latency time.Duration
}

// Version is the peer protocol's value and causal history. Storage time is local.
type Version struct {
	Value string                  `json:"value"`
	VC    vectorclock.VectorClock `json:"vc"`
}

const MaxValueBytes = 1 << 20
const MaxClockBytes = 64 << 10
const MaxWriteBytes = 6*MaxValueBytes + MaxClockBytes + 1024
const MaxReadBytes = 64 << 20

// ValidateVersion rejects clocks that cannot be safely stored and exchanged.
func ValidateVersion(v Version) error {
	if v.VC == nil {
		return fmt.Errorf("vc is required and must be an object")
	}
	if len(v.Value) > MaxValueBytes {
		return fmt.Errorf("value too large")
	}
	for node, counter := range v.VC {
		if node == "" || counter < 0 {
			return fmt.Errorf("clock needs nonempty node IDs and nonnegative counters")
		}
	}
	encoded, err := json.Marshal(v.VC)
	if err != nil {
		return err
	}
	if len(encoded) > MaxClockBytes {
		return fmt.Errorf("clock too large")
	}
	return nil
}

type ReadResult struct {
	Peer     string
	Versions []Version
	Err      error
	Latency  time.Duration
}

// Read fetches the complete sibling list; an empty array means a missing key.
func Read(ctx context.Context, peer, key string) ([]Version, error) {
	targetURL := "http://" + peer + "/internal/kv/" + url.PathEscape(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer returned status %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxReadBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxReadBytes {
		return nil, fmt.Errorf("peer read response too large")
	}
	var answers []struct {
		Value *string                 `json:"value"`
		VC    vectorclock.VectorClock `json:"vc"`
	}
	if err := json.Unmarshal(body, &answers); err != nil {
		return nil, err
	}
	if answers == nil {
		return nil, fmt.Errorf("peer must return a sibling array, not null")
	}
	versions := make([]Version, 0, len(answers))
	for _, answer := range answers {
		if answer.Value == nil {
			return nil, fmt.Errorf("peer sibling requires value")
		}
		v := Version{Value: *answer.Value, VC: answer.VC}
		if err := ValidateVersion(v); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, nil
}

// ReadFanOut queries every peer so a failed peer cannot block another answer.
func ReadFanOut(ctx context.Context, peers []string, key string) <-chan ReadResult {
	results := make(chan ReadResult, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			start := time.Now()
			versions, err := Read(ctx, peer, key)
			results <- ReadResult{Peer: peer, Versions: versions, Err: err, Latency: time.Since(start)}
		}(peer)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}

var Client = &http.Client{
	Timeout: 2 * time.Second,
}

func Replicate(ctx context.Context, peer, key, value string, vc vectorclock.VectorClock) error {
	if err := ValidateVersion(Version{Value: value, VC: vc}); err != nil {
		return err
	}
	targetURL := "http://" + peer + "/internal/kv/" + url.PathEscape(key)
	body, err := json.Marshal(Version{Value: value, VC: vc})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		targetURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned status %s", resp.Status)
	}

	return nil
}

// StartFanOut gives peer writes their own lifetime, independent of HTTP handlers.
func StartFanOut(peers []string, key, value string, vc vectorclock.VectorClock) <-chan Result {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	return fanOut(ctx, peers, key, value, vc, cancel)
}

// FanOut uses a caller-owned context, useful for explicit cancellation and tests.
func FanOut(ctx context.Context, peers []string, key, value string, vc vectorclock.VectorClock) <-chan Result {
	return fanOut(ctx, peers, key, value, vc, nil)
}

func fanOut(ctx context.Context, peers []string, key, value string, vc vectorclock.VectorClock, cleanup context.CancelFunc) <-chan Result {
	vc = vectorclock.Merge(vc, nil) // Workers share an immutable private copy.
	results := make(chan Result, len(peers))

	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()

			start := time.Now()

			err := Replicate(ctx, peer, key, value, vc)

			results <- Result{
				Peer:    peer,
				Err:     err,
				Latency: time.Since(start),
			}
		}(peer)
	}
	go func() {
		wg.Wait()
		if cleanup != nil {
			cleanup()
		}
		close(results)
	}()
	return results
}
