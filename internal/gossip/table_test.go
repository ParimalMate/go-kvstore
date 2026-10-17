package gossip

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
func testTable(t *testing.T) (*Table, *fakeClock) {
	t.Helper()
	clock := &fakeClock{time.Now()}
	table, err := NewTable("n1", 100, Timing{time.Second, 5 * time.Second, 10 * time.Second}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	return table, clock
}

func TestMergeEvidence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		incoming         WireMember
		hb, gen          int64
		advance, restart bool
	}{
		{"advance", WireMember{"n2", 100, 11}, 11, 100, true, false},
		{"equal", WireMember{"n2", 100, 10}, 10, 100, false, false},
		{"lower heartbeat", WireMember{"n2", 100, 9}, 10, 100, false, false},
		{"older generation", WireMember{"n2", 99, 99}, 10, 100, false, false},
		{"restart with lower heartbeat", WireMember{"n2", 101, 0}, 0, 101, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table, clock := testTable(t)
			change := table.Merge([]WireMember{{"n2", 100, 10}})
			if !reflect.DeepEqual(change.NewIDs, []string{"n2"}) {
				t.Fatal(change)
			}
			previous := table.entries["n2"].lastChange
			clock.Advance(4 * time.Second)
			change = table.Merge([]WireMember{tc.incoming})
			got := table.Digest()[1]
			if got.Heartbeat != tc.hb || got.Generation != tc.gen {
				t.Fatal(got)
			}
			if (len(change.AdvancedIDs) == 1) != tc.advance || (len(change.Restarted) == 1) != tc.restart {
				t.Fatal(change)
			}
			wantTime := previous
			if tc.advance || tc.restart {
				wantTime = clock.Now()
			}
			if !table.entries["n2"].lastChange.Equal(wantTime) {
				t.Fatal("incorrect observation time")
			}
			if tc.restart && change.Restarted[0] != (Restart{"n2", 100, 101}) {
				t.Fatal(change)
			}
		})
	}
}

func TestStatusBoundaries(t *testing.T) {
	for _, tc := range []struct {
		age  time.Duration
		want Status
	}{
		{5*time.Second - time.Nanosecond, Alive}, {5 * time.Second, Suspect}, {5*time.Second + time.Nanosecond, Suspect},
		{10*time.Second - time.Nanosecond, Suspect}, {10 * time.Second, Dead}, {10*time.Second + time.Nanosecond, Dead},
	} {
		t.Run(tc.age.String(), func(t *testing.T) {
			table, clock := testTable(t)
			table.Merge([]WireMember{{"n2", 100, 1}})
			clock.Advance(tc.age)
			table.Refresh()
			if table.entries["n2"].status != tc.want {
				t.Fatal(table.entries["n2"].status)
			}
			if table.entries["n1"].status != Alive {
				t.Fatal("self must remain alive")
			}
		})
	}
}

func TestTransitionsAndRecovery(t *testing.T) {
	table, clock := testTable(t)
	table.Merge([]WireMember{{"n2", 100, 1}})
	for _, step := range []struct {
		advance  time.Duration
		from, to Status
	}{
		{5 * time.Second, Alive, Suspect}, {5 * time.Second, Suspect, Dead}, {0, Dead, Alive},
	} {
		clock.Advance(step.advance)
		if step.to == Alive {
			table.Merge([]WireMember{{"n2", 100, 2}})
		}
		want := []Transition{{"n2", step.from, step.to}}
		if got := table.Refresh(); !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
		if got := table.Refresh(); len(got) != 0 {
			t.Fatalf("repeated transition %v", got)
		}
	}
}

func TestSelfCapValidationAndCopies(t *testing.T) {
	table, clock := testTable(t)
	change := table.Merge([]WireMember{{"n1", 101, 999}, {"n1", 100, 999}, {"bad id", 100, 1}, {"n2", 0, 1}, {"n3", 1, -1}})
	if !change.Conflict || len(table.Digest()) != 1 {
		t.Fatal(change)
	}
	if got := table.Digest()[0]; got != (WireMember{"n1", 100, 0}) {
		t.Fatal(got)
	}
	clock.Advance(time.Second)
	table.Beat()
	if table.Digest()[0].Heartbeat != 1 || !table.entries["n1"].lastChange.Equal(clock.Now()) {
		t.Fatal("beat did not update self")
	}
	members := make([]WireMember, MaxMembers)
	for i := range members {
		members[i] = WireMember{fmt.Sprintf("peer%03d", i), 1, 0}
	}
	table.Merge(members)
	if len(table.Digest()) != MaxMembers {
		t.Fatal("wrong cap")
	}
	if _, ok := table.entries["peer255"]; ok {
		t.Fatal("inserted over cap")
	}
	table.Merge([]WireMember{{"peer000", 1, 2}})
	if table.entries["peer000"].heartbeat != 2 {
		t.Fatal("existing updates blocked at cap")
	}
	digest := table.Digest()
	for i := 1; i < len(digest); i++ {
		if digest[i-1].ID >= digest[i].ID {
			t.Fatal("unsorted digest")
		}
	}
	digest[0].Heartbeat = 999
	if table.Digest()[0].Heartbeat == 999 {
		t.Fatal("digest aliases table")
	}
	snapshot := table.Digest()
	table.Beat()
	if snapshot[0].Heartbeat != 1 {
		t.Fatal("snapshot mutated")
	}
}

func TestNewTableValidation(t *testing.T) {
	good := Timing{time.Second, 5 * time.Second, 10 * time.Second}
	for _, tc := range []struct {
		id     string
		gen    int64
		timing Timing
		now    func() time.Time
	}{
		{"bad id", 1, good, time.Now}, {"n1", 0, good, time.Now}, {"n1", 1, good, nil},
		{"n1", 1, Timing{time.Millisecond, time.Second, 2 * time.Second}, time.Now},
		{"n1", 1, Timing{2 * time.Minute, 6 * time.Minute, 7 * time.Minute}, time.Now},
		{"n1", 1, Timing{time.Second, time.Second, 2 * time.Second}, time.Now},
		{"n1", 1, Timing{time.Second, 5 * time.Second, 5 * time.Second}, time.Now},
	} {
		if _, err := NewTable(tc.id, tc.gen, tc.timing, tc.now); err == nil {
			t.Fatal("accepted invalid constructor arguments")
		}
	}
}

func TestConcurrentTableAccess(t *testing.T) {
	// The fake clock remains fixed while workers run, so the clock itself has no races.
	table, _ := testTable(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for hb := int64(0); hb < 100; hb++ {
				table.Merge([]WireMember{{fmt.Sprintf("peer%d", i), 1, hb}})
				table.Beat()
				table.Refresh()
				table.Digest()
				table.NoteDirect(fmt.Sprintf("peer%d", i), fmt.Sprintf("address%d", i))
				table.MarkSelfAlias("self-address")
				table.View([]string{"self-address", "other"})
				table.SelectPeer([]string{"self-address", "other"}, func(int) int { return 0 })
			}
		}(i)
	}
	wg.Wait()
	if got := table.Digest()[0].Heartbeat; got != 5000 {
		t.Fatalf("lost beats: %d", got)
	}
}
