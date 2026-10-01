package replication

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReplicateSuccess(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT request, got %s", r.Method)
		}

		if r.URL.Path != "/internal/kv/test-key" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}

		if string(body) != "hello" {
			t.Errorf("expected body \"hello\", got %q", string(body))
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
