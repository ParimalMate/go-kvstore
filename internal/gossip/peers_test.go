package gossip

import (
	"reflect"
	"testing"
	"time"
)

func TestViewAndDirectEvidence(t *testing.T) {
	table, clock := testTable(t)
	seeds := []string{"proxy2:8080", "proxy3:8080", "self:8080"}
	table.Merge([]WireMember{{"n2", 200, 10}, {"n3", 300, 1}})
	// Learning an ID second-hand does not establish its configured address.
	if got := table.View(seeds).UnreachedSeeds; len(got) != 3 {
		t.Fatal(got)
	}
	clock.Advance(5500 * time.Millisecond)
	before := table.Digest()
	table.NoteDirect("n2", seeds[0])
	table.MarkSelfAlias(seeds[2])
	view := table.View(seeds)
	if !reflect.DeepEqual(view.UnreachedSeeds, []string{seeds[1]}) {
		t.Fatal(view)
	}
	want := MemberView{"n2", Suspect, 200, 10, 5500, seeds[0]}
	if view.Members[1] != want {
		t.Fatalf("got %+v want %+v", view.Members[1], want)
	}
	if view.Members[0].Status != Alive {
		t.Fatal("self not alive")
	}
	if !reflect.DeepEqual(before, table.Digest()) {
		t.Fatal("address bookkeeping modified evidence")
	}
	if transitions := table.Refresh(); len(transitions) != 2 {
		t.Fatal("View consumed transitions", transitions)
	}
	view.Members[1].Via = "changed"
	view.UnreachedSeeds[0] = "changed"
	if table.View(seeds).Members[1].Via != seeds[0] || table.View(seeds).UnreachedSeeds[0] != seeds[1] {
		t.Fatal("view aliases table or input")
	}
	table.NoteDirect("n2", "other-proxy:8080")
	if table.View(seeds).Members[1].Via != "other-proxy:8080" {
		t.Fatal("latest direct address not recorded")
	}
	// An address can subsequently answer as a different node: last answer wins.
	table.NoteDirect("n3", "other-proxy:8080")
	if table.direct["other-proxy:8080"] != "n3" || table.View(seeds).Members[1].Via != "" {
		t.Fatal("obsolete address attribution")
	}
	// Direct contact alone cannot revive a node without newer evidence.
	clock.Advance(5 * time.Second)
	table.NoteDirect("n2", seeds[0])
	if table.View(seeds).Members[1].Status != Dead {
		t.Fatal("direct contact refreshed evidence")
	}
	table.Merge([]WireMember{{"n2", 200, 11}})
	if member := table.View(seeds).Members[1]; member.Status != Alive || member.LastChangeMSAgo != 0 {
		t.Fatal(member)
	}
}

func TestPeerSelection(t *testing.T) {
	table, clock := testTable(t)
	seeds := []string{"self", "peer2", "peer3", "peer2"}
	table.MarkSelfAlias("self")
	table.Merge([]WireMember{{"n2", 1, 0}})
	table.NoteDirect("n2", "peer2")
	clock.Advance(10 * time.Second)
	table.Refresh()
	for index, want := range []string{"peer2", "peer3"} {
		got, ok := table.SelectPeer(seeds, func(n int) int {
			if n != 2 {
				t.Fatalf("picker got %d choices", n)
			}
			// This would deadlock if the callback ran while the table lock was held.
			table.Digest()
			return index
		})
		if !ok || got != want {
			t.Fatalf("got %q %v want %q", got, ok, want)
		}
	}
	for _, empty := range [][]string{nil, {"self"}, {""}} {
		got, ok := table.SelectPeer(empty, func(int) int { t.Fatal("picker called without peers"); return 0 })
		if got != "" || ok {
			t.Fatal(got, ok)
		}
	}
	// A later validated exchange may identify a formerly self-alias address differently.
	table.NoteDirect("n3", "self")
	if got, ok := table.SelectPeer([]string{"self"}, func(int) int { return 0 }); !ok || got != "self" {
		t.Fatal(got, ok)
	}
}

func TestDirectValidationAndUnknownID(t *testing.T) {
	table, _ := testTable(t)
	table.NoteDirect("bad id", "peer")
	table.NoteDirect("n2", "")
	if len(table.direct) != 0 {
		t.Fatal("accepted invalid direct attribution")
	}
	table.NoteDirect("n2", "peer")
	if got := table.View([]string{"peer"}); len(got.UnreachedSeeds) != 0 || len(got.Members) != 1 {
		t.Fatal(got)
	}
	// NoteDirect has no generation/heartbeat, so it must not invent a member.
}
