package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

type ReadResult struct {
	Peer    string
	Value   string
	Ts      int64
	Exists  bool
	Err     error
	Latency time.Duration
}

// Read fetches metadata from one peer. Missing keys are valid answers, not errors.
func Read(ctx context.Context, peer, key string) (string, int64, bool, error) {
	targetURL := "http://" + peer + "/internal/kv/" + url.PathEscape(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return "", 0, false, err
	}
	resp, err := Client.Do(req)
	if err != nil {
		return "", 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, false, fmt.Errorf("peer returned status %s", resp.Status)
	}
	// Bound JSON expansion just as the internal PUT endpoint does.
	const maxBody = 6*(1<<20) + 1024
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return "", 0, false, err
	}
	if len(body) > maxBody {
		return "", 0, false, fmt.Errorf("peer read response too large")
	}
	var answer struct {
		Value  *string `json:"value"`
		Ts     *int64  `json:"ts"`
		Exists *bool   `json:"exists"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", 0, false, err
	}
	if answer.Value == nil || answer.Ts == nil || answer.Exists == nil {
		return "", 0, false, fmt.Errorf("peer read response requires value, ts, and exists")
	}
	if len(*answer.Value) > 1<<20 {
		return "", 0, false, fmt.Errorf("peer read value too large")
	}
	return *answer.Value, *answer.Ts, *answer.Exists, nil
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
			value, ts, exists, err := Read(ctx, peer, key)
			results <- ReadResult{Peer: peer, Value: value, Ts: ts, Exists: exists, Err: err, Latency: time.Since(start)}
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

type writeRequest struct {
	Value string `json:"value"`
	Ts    int64  `json:"ts"`
}

func Replicate(ctx context.Context, peer, key, value string, ts int64) error {
	targetURL := "http://" + peer + "/internal/kv/" + url.PathEscape(key)
	body, err := json.Marshal(writeRequest{Value: value, Ts: ts})
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

	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned status %s", resp.Status)
	}

	return nil
}

// StartFanOut gives peer writes their own lifetime, independent of HTTP handlers.
func StartFanOut(peers []string, key, value string, ts int64) <-chan Result {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	return fanOut(ctx, peers, key, value, ts, cancel)
}

// FanOut uses a caller-owned context, useful for explicit cancellation and tests.
func FanOut(ctx context.Context, peers []string, key, value string, ts int64) <-chan Result {
	return fanOut(ctx, peers, key, value, ts, nil)
}

func fanOut(ctx context.Context, peers []string, key, value string, ts int64, cleanup context.CancelFunc) <-chan Result {
	results := make(chan Result, len(peers))

	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()

			start := time.Now()

			err := Replicate(ctx, peer, key, value, ts)

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
