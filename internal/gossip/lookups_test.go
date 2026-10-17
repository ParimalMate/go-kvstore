package gossip

import (
	"io"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestPeerLookupsUseCurrentAge(t *testing.T) {
	table, clock := testTable(t)
	service, err := NewService(table, log.New(io.Discard, "", 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := service.PeerByAddr("proxy:8080"); ok {
		t.Fatal("unknown address found")
	}
	if _, ok := service.AddrOf("n2"); ok {
		t.Fatal("unknown ID found")
	}
	table.Merge([]WireMember{{"n2", 200, 1}})
	if _, ok := service.AddrOf("n2"); ok {
		t.Fatal("indirect evidence invented address")
	}
	table.NoteDirect("n2", "proxy:8080")
	if addr, ok := service.AddrOf("n2"); !ok || addr != "proxy:8080" {
		t.Fatal(addr, ok)
	}
	for _, step := range []struct {
		advance time.Duration
		want    Status
	}{{0, Alive}, {5 * time.Second, Suspect}, {5 * time.Second, Dead}} {
		clock.Advance(step.advance)
		id, status, ok := service.PeerByAddr("proxy:8080")
		if !ok || id != "n2" || status != step.want {
			t.Fatal(id, status, ok)
		}
	}
	// Lookups must not consume transitions for the repair hook.
	if got := table.Refresh(); !reflect.DeepEqual(got, []Transition{{"n2", Alive, Dead}}) {
		t.Fatal(got)
	}
	table.Merge([]WireMember{{"n2", 200, 2}})
	if _, status, ok := service.PeerByAddr("proxy:8080"); !ok || status != Alive {
		t.Fatal(status, ok)
	}
	if got := table.Refresh(); !reflect.DeepEqual(got, []Transition{{"n2", Dead, Alive}}) {
		t.Fatal(got)
	}
	table.MarkSelfAlias("self:8080")
	if _, _, ok := service.PeerByAddr("self:8080"); ok {
		t.Fatal("self alias reported as peer")
	}
	if _, ok := service.AddrOf("n1"); ok {
		t.Fatal("self reported as repair target")
	}
	table.Merge([]WireMember{{"n3", 300, 1}})
	table.NoteDirect("n3", "proxy:8080")
	if id, _, ok := service.PeerByAddr("proxy:8080"); !ok || id != "n3" {
		t.Fatal(id, ok)
	}
	if _, ok := service.AddrOf("n2"); ok {
		t.Fatal("obsolete address retained")
	}
	table.NoteDirect("unknown", "other:8080")
	if _, _, ok := service.PeerByAddr("other:8080"); ok {
		t.Fatal("missing member has fabricated status")
	}
}
