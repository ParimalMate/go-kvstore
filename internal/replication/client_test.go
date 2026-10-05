package replication

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReplicateSuccess(t *testing.T) {
	const ts int64 = 1720000000000000123
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT request, got %s", r.Method)
		}

		if r.URL.Path != "/internal/kv/test-key" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("expected JSON content type")
		}
		var body struct {
			Value string `json:"value"`
			Ts    int64  `json:"ts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		if body.Value != "hello" || body.Ts != ts {
			t.Errorf("unexpected write payload: %+v", body)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer peer.Close()

	peerAddress := strings.TrimPrefix(peer.URL, "http://")

	err := Replicate(
		context.Background(),
		peerAddress,
		"test-key",
		"hello",
		ts,
	)

	if err != nil {
		t.Fatalf("expected replication to succeed, got: %v", err)
	}
}

func TestFanOutAllPeersSucceed(t *testing.T) {
	peer1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer peer1.Close()

	peer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer peer2.Close()

	peers := []string{
		strings.TrimPrefix(peer1.URL, "http://"),
		strings.TrimPrefix(peer2.URL, "http://"),
	}

	results := FanOut(
		context.Background(),
		peers,
		"course",
		"distributed-systems",
		123,
	)

	successCount := 0

	for result := range results {
		if result.Err != nil {
			t.Errorf("peer %s unexpectedly failed: %v", result.Peer, result.Err)
			continue
		}

		successCount++
	}

	if successCount != 2 {
		t.Fatalf("expected 2 successful replications, got %d", successCount)
	}
}
func TestFanOutReportsFailures(t *testing.T) {
	successPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer successPeer.Close()

	failingPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "peer failure", http.StatusInternalServerError)
	}))
	defer failingPeer.Close()

	slowPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(500 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			return
		}
	}))
	defer slowPeer.Close()

	successAddr := strings.TrimPrefix(successPeer.URL, "http://")
	failingAddr := strings.TrimPrefix(failingPeer.URL, "http://")
	slowAddr := strings.TrimPrefix(slowPeer.URL, "http://")

	peers := []string{
		successAddr,
		failingAddr,
		slowAddr,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	results := FanOut(
		ctx,
		peers,
		"test-key",
		"test-value",
		123,
	)

	successCount := 0
	failedPeers := make(map[string]bool)

	for result := range results {
		if result.Err != nil {
			failedPeers[result.Peer] = true
			continue
		}

		successCount++
	}

	if successCount != 1 {
		t.Errorf("expected 1 successful replication, got %d", successCount)
	}

	if len(failedPeers) != 2 {
		t.Errorf("expected 2 failed peers, got %d", len(failedPeers))
	}

	if !failedPeers[failingAddr] {
		t.Errorf("expected failing peer %s to be reported", failingAddr)
	}

	if !failedPeers[slowAddr] {
		t.Errorf("expected slow peer %s to be reported", slowAddr)
	}

	if failedPeers[successAddr] {
		t.Errorf("successful peer %s was incorrectly reported as failed", successAddr)
	}
}

func TestStartFanOutSurvivesHandlerReturn(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	defer unblock()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer fast.Close()

	remaining := make(chan (<-chan Result), 1)
	requestContexts := make(chan context.Context, 1)
	// This small test handler models a future quorum response after one peer ack.
	coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		results := StartFanOut([]string{
			strings.TrimPrefix(fast.URL, "http://"),
			strings.TrimPrefix(slow.URL, "http://"),
		}, "name", "Parimal", 123)
		remaining <- results
		requestContexts <- r.Context()
		first := <-results
		if first.Err != nil {
			http.Error(w, first.Err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer coordinator.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(coordinator.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("early response status=%d", resp.StatusCode)
	}
	ctx := <-requestContexts
	select {
	case <-ctx.Done(): // The HTTP handler has returned and its context is cancelled.
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator handler did not return")
	}
	unblock()
	results := <-remaining
	select {
	case result, ok := <-results:
		if !ok || result.Err != nil || result.Peer != strings.TrimPrefix(slow.URL, "http://") {
			t.Fatalf("straggler failed after response: result=%+v open=%v", result, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("straggler did not finish")
	}
	select {
	case _, ok := <-results:
		if ok {
			t.Fatal("unexpected extra result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("results channel did not close")
	}
}

func TestStartFanOutTimeout(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer peer.Close()
	results := StartFanOut([]string{strings.TrimPrefix(peer.URL, "http://")}, "name", "Parimal", 123)
	select {
	case result, ok := <-results:
		if !ok || !errors.Is(result.Err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline error, got result=%+v open=%v", result, ok)
		}
	case <-time.After(5 * time.Second):
		peer.CloseClientConnections()
		t.Fatal("independent replication did not time out")
	}
	select {
	case _, ok := <-results:
		if ok {
			t.Fatal("unexpected extra result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("results channel did not close after timeout")
	}
}

func TestStartFanOutNoPeers(t *testing.T) {
	select {
	case _, ok := <-StartFanOut(nil, "name", "Parimal", 123):
		if ok {
			t.Fatal("unexpected result without peers")
		}
	case <-time.After(time.Second):
		t.Fatal("empty fan-out did not close")
	}
}
