package gossip

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testService(t *testing.T, id string, generation int64, seeds []string) (*Service, *bytes.Buffer, *fakeClock) {
	t.Helper()
	clock := &fakeClock{time.Now()}
	table, err := NewTable(id, generation, Timing{50 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	output := &bytes.Buffer{}
	service, err := NewService(table, log.New(output, "", 0), seeds)
	if err != nil {
		t.Fatal(err)
	}
	return service, output, clock
}

func messageBody(id string, generation, hb int64) string {
	body, _ := json.Marshal(GossipMessage{id, []WireMember{{id, generation, hb}}})
	return string(body)
}

func TestExchangeHandlerValidation(t *testing.T) {
	valid := messageBody("n2", 2, 1)
	many := GossipMessage{From: "n2", Members: make([]WireMember, MaxMembers+1)}
	manyBody, _ := json.Marshal(many)
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"valid", valid, 200},
		{"empty", "", 400}, {"malformed", "{", 400}, {"null", "null", 400},
		{"trailing object", valid + valid, 400},
		{"bad sender", `{"from":"bad id","members":[]}`, 400},
		{"missing sender entry", `{"from":"n2","members":[]}`, 400},
		{"bad member", `{"from":"n2","members":[{"id":"bad id","generation":2,"heartbeat":1}]}`, 400},
		{"zero generation", messageBody("n2", 0, 0), 400},
		{"negative heartbeat", messageBody("n2", 2, -1), 400},
		{"fraction", `{"from":"n2","members":[{"id":"n2","generation":2,"heartbeat":1.5}]}`, 400},
		{"overflow", `{"from":"n2","members":[{"id":"n2","generation":1e30,"heartbeat":1}]}`, 400},
		{"duplicate", `{"from":"n2","members":[{"id":"n2","generation":2,"heartbeat":1},{"id":"n2","generation":2,"heartbeat":2}]}`, 400},
		{"too many", string(manyBody), 400},
		{"exact body limit", valid + strings.Repeat(" ", MaxBodyBytes-len(valid)), 200},
		{"oversized", valid + strings.Repeat(" ", MaxBodyBytes-len(valid)+1), 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, _, _ := testService(t, "n1", 1, nil)
			before := service.table.Digest()
			recorder := httptest.NewRecorder()
			service.HandleExchange(recorder, httptest.NewRequest("POST", "/internal/gossip", strings.NewReader(tc.body)))
			if recorder.Code != tc.status {
				t.Fatalf("got %d: %s", recorder.Code, recorder.Body.String())
			}
			if tc.status != 200 && !reflect.DeepEqual(before, service.table.Digest()) {
				t.Fatal("invalid request mutated table")
			}
			if tc.status == 200 {
				var reply GossipMessage
				if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
					t.Fatal(err)
				}
				if reply.From != "n1" || len(reply.Members) != 2 || reply.Members[1].ID != "n2" {
					t.Fatal("reply was not taken after merge", reply)
				}
			}
		})
	}
}

func TestMembersAndMethodRoutes(t *testing.T) {
	service, output, clock := testService(t, "z-self", 1, []string{"unreached:8080"})
	service.table.Merge([]WireMember{{"a-peer", 2, 1}})
	clock.Advance(500 * time.Millisecond)
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest("GET", "/internal/members", nil))
	var view MembersResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.Self != "z-self" || view.Generation != 1 || view.Members[0].ID != "z-self" || view.Members[0].Status != Alive || view.Members[1].Status != Suspect || len(view.UnreachedSeeds) != 1 {
		t.Fatal(view)
	}
	if !strings.Contains(output.String(), "alive -> suspect") {
		t.Fatal(output.String())
	}
	recorder = httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest("GET", "/internal/members", nil))
	if strings.Count(output.String(), "alive -> suspect") != 1 {
		t.Fatal("repeated transition")
	}
	for _, tc := range []struct{ method, path string }{{"GET", "/internal/gossip"}, {"PUT", "/internal/gossip"}, {"POST", "/internal/members"}} {
		recorder = httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
		if recorder.Code != 405 {
			t.Fatal(tc, recorder.Code)
		}
	}
}

func TestRealHTTPPushPull(t *testing.T) {
	a, _, _ := testService(t, "n1", 1, nil)
	b, _, _ := testService(t, "n2", 2, nil)
	b.table.Merge([]WireMember{{"n3", 3, 9}})
	mux := http.NewServeMux()
	b.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	if err := a.exchange(context.Background(), addr); err != nil {
		t.Fatal(err)
	}
	if len(a.table.Digest()) != 3 || len(b.table.Digest()) != 3 {
		t.Fatal("push-pull failed")
	}
	view := a.table.View([]string{addr})
	if len(view.UnreachedSeeds) != 0 || view.Members[1].Via != addr || view.Members[2].Via != "" {
		t.Fatal(view)
	}
}

func TestClientFailureAndRecovery(t *testing.T) {
	a, output, _ := testService(t, "n1", 1, nil)
	status := 503
	response := messageBody("n2", 2, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(status)
		io.WriteString(w, response)
	}))
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	before := a.table.View([]string{addr})
	for i := 0; i < 2; i++ {
		if err := a.exchange(context.Background(), addr); err == nil {
			t.Fatal("expected failure")
		}
	}
	if strings.Count(output.String(), "unreachable:") != 1 {
		t.Fatal(output.String())
	}
	if !reflect.DeepEqual(before, a.table.View([]string{addr})) {
		t.Fatal("failed exchange changed table")
	}
	status = 200
	if err := a.exchange(context.Background(), addr); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "reachable again") != 1 {
		t.Fatal(output.String())
	}
	for _, invalid := range []string{"{", messageBody("n2", 0, 1), `{"from":"n2","members":[]}`, response + "{}", response + strings.Repeat(" ", MaxBodyBytes)} {
		response = invalid
		before := a.table.Digest()
		if err := a.exchange(context.Background(), addr); err == nil {
			t.Fatal("accepted invalid response")
		}
		if !reflect.DeepEqual(before, a.table.Digest()) {
			t.Fatal("invalid response mutated table")
		}
	}
}

func TestIdentityAliasAndDuplicate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		generation int64
		alias      bool
	}{{"alias", 100, true}, {"higher duplicate", 200, false}, {"lower duplicate", 50, false}} {
		t.Run(tc.name, func(t *testing.T) {
			a, logA, clockA := testService(t, "n1", 100, nil)
			b, logB, _ := testService(t, "n1", tc.generation, nil)
			mux := http.NewServeMux()
			b.RegisterRoutes(mux)
			server := httptest.NewServer(mux)
			defer server.Close()
			addr := strings.TrimPrefix(server.URL, "http://")
			for i := 0; i < 2; i++ {
				if err := a.exchange(context.Background(), addr); err != nil {
					t.Fatal(err)
				}
			}
			_, eligible := a.table.SelectPeer([]string{addr}, func(int) int { return 0 })
			if eligible == tc.alias {
				t.Fatal("wrong alias classification")
			}
			if a.table.Digest()[0].Generation != 100 || b.table.Digest()[0].Generation != tc.generation {
				t.Fatal("adopted foreign self generation")
			}
			if tc.alias {
				if strings.Count(logA.String(), "is an alias") != 1 || strings.Contains(logA.String(), "ID CONFLICT") || strings.Contains(logB.String(), "ID CONFLICT") {
					t.Fatal(logA.String(), logB.String())
				}
			} else {
				if strings.Count(logA.String(), "ID CONFLICT") != 1 || strings.Count(logB.String(), "ID CONFLICT") != 1 {
					t.Fatal(logA.String(), logB.String())
				}
				clockA.Advance(30 * time.Second)
				if err := a.exchange(context.Background(), addr); err != nil {
					t.Fatal(err)
				}
				if strings.Count(logA.String(), "ID CONFLICT") != 2 {
					t.Fatal("conflict throttle failed")
				}
			}
		})
	}
}

func TestClientCancellationAndTimeout(t *testing.T) {
	a, _, _ := testService(t, "n1", 1, nil)
	entered := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	before := a.table.Digest()
	start := time.Now()
	if err := a.exchange(context.Background(), addr); err == nil {
		t.Fatal("missing timeout")
	}
	if time.Since(start) > time.Second {
		t.Fatal("timeout exceeded one second")
	}
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.exchange(ctx, addr) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation too slow")
	}
	if !reflect.DeepEqual(before, a.table.Digest()) {
		t.Fatal("timeout mutated evidence")
	}
}

func TestExchangeTimeoutClamp(t *testing.T) {
	for _, tc := range []struct{ interval, want time.Duration }{{10 * time.Millisecond, 200 * time.Millisecond}, {time.Second, time.Second}, {time.Minute, 2 * time.Second}} {
		table, err := NewTable("n1", 1, Timing{tc.interval, 3 * tc.interval, 4 * tc.interval}, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		service, err := NewService(table, log.New(io.Discard, "", 0), nil)
		if err != nil || service.timeout != tc.want {
			t.Fatal(err, service)
		}
	}
}
