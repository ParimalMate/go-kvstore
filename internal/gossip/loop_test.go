package gossip

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func liveService(t *testing.T, seeds []string) *Service {
	t.Helper()
	table, err := NewTable("n1", 100, Timing{50 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(table, log.New(io.Discard, "", 0), seeds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Stop)
	return service
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true within 5 seconds")
		case <-ticker.C:
		}
	}
}

func TestLoopProgressAndPromptStop(t *testing.T) {
	entered := make(chan struct{}, 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	service := liveService(t, []string{strings.TrimPrefix(server.URL, "http://")})
	defer service.Stop()
	service.table.Merge([]WireMember{{"n2", 200, 10}})
	transitions := make(chan Transition, 10)
	service.OnChange(func(change Transition) {
		// Re-entering these methods verifies callback invocation holds no locks.
		service.table.Digest()
		service.OnChange(nil)
		service.OnChange(func(change Transition) { transitions <- change })
		transitions <- change
	})
	service.Start(context.Background())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no immediate exchange")
	}
	waitFor(t, func() bool { return service.table.View(nil).Members[1].Status == Dead })
	if hb := service.table.Digest()[0].Heartbeat; hb < 10 {
		t.Fatalf("hung peer delayed heartbeat rounds: %d", hb)
	}
	waitFor(t, func() bool { return len(transitions) == 2 })
	for _, want := range []Transition{{"n2", Alive, Suspect}, {"n2", Suspect, Dead}} {
		if got := <-transitions; got != want {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	stopped := make(chan struct{})
	go func() { service.Stop(); service.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel outstanding exchanges promptly")
	}
	before := service.table.Digest()[0].Heartbeat
	service.Start(context.Background()) // A stopped instance cannot be restarted.
	if service.table.Digest()[0].Heartbeat != before {
		t.Fatal("stopped service restarted")
	}
}

func TestStandaloneDoesNotStartLoop(t *testing.T) {
	service := liveService(t, nil)
	service.Start(context.Background())
	service.lifecycleMu.Lock()
	started := service.cancel != nil
	service.lifecycleMu.Unlock()
	if started || service.table.Digest()[0].Heartbeat != 0 {
		t.Fatal("standalone started a loop")
	}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest("GET", "/internal/members", nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"self":"n1"`) {
		t.Fatal(response.Body.String())
	}
	service.Stop()
	service.Stop()
}

func TestStartStopConcurrent(t *testing.T) {
	service := liveService(t, []string{"127.0.0.1:1"})
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); service.Start(context.Background()); service.Stop() }()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent lifecycle calls blocked")
	}
}

func TestParentCancellation(t *testing.T) {
	service := liveService(t, []string{"127.0.0.1:1"})
	ctx, cancel := context.WithCancel(context.Background())
	service.Start(ctx)
	waitFor(t, func() bool { return service.table.Digest()[0].Heartbeat > 0 })
	cancel()
	done := make(chan struct{})
	go func() { service.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not stop workers")
	}
}
