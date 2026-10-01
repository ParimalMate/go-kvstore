package replication

import (
	"bytes"
	"context"
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

var Client = &http.Client{
	Timeout: 2 * time.Second,
}

func Replicate(ctx context.Context, peer, key, value string) error {
	targetURL := "http://" + peer + "/internal/kv/" + url.PathEscape(key)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		targetURL,
		bytes.NewReader([]byte(value)),
	)
	if err != nil {
		return err
	}
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

func FanOut(ctx context.Context, peers []string, key, value string) <-chan Result {
	results := make(chan Result, len(peers))

	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()

			start := time.Now()

			err := Replicate(ctx, peer, key, value)

			results <- Result{
				Peer:    peer,
				Err:     err,
				Latency: time.Since(start),
			}
		}(peer)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}
