package vectorclock

import (
	"maps"
	"testing"
)

func TestCompare(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b VectorClock
		want Relation
	}{
		{"equal", VectorClock{"n1": 2, "n2": 1}, VectorClock{"n1": 2, "n2": 1}, Equal},
		{"before", VectorClock{"n1": 1}, VectorClock{"n1": 2}, Before},
		{"after", VectorClock{"n1": 2}, VectorClock{"n1": 1}, After},
		{"concurrent", VectorClock{"n1": 2, "n2": 0}, VectorClock{"n1": 1, "n2": 1}, Concurrent},
		{"concurrent reversed", VectorClock{"n1": 1, "n2": 1}, VectorClock{"n1": 2, "n2": 0}, Concurrent},
		{"different nodes", VectorClock{"n1": 1}, VectorClock{"n2": 1}, Concurrent},
		{"additional history", VectorClock{"n1": 1}, VectorClock{"n1": 1, "n2": 1}, Before},
		{"zero equals missing", VectorClock{"n1": 1}, VectorClock{"n1": 1, "n2": 0}, Equal},
		{"empty", VectorClock{}, VectorClock{}, Equal},
		{"nil", nil, nil, Equal},
		{"nil equals empty", nil, VectorClock{}, Equal},
		{"nil equals zero", nil, VectorClock{"n1": 0}, Equal},
		{"empty before write", nil, VectorClock{"n1": 1}, Before},
		{"write after empty", VectorClock{"n1": 1}, nil, After},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aBefore, bBefore := maps.Clone(tc.a), maps.Clone(tc.b)
			if got := Compare(tc.a, tc.b); got != tc.want {
				t.Fatalf("Compare(%v, %v) = %v; want %v", tc.a, tc.b, got, tc.want)
			}
			if !maps.Equal(tc.a, aBefore) || !maps.Equal(tc.b, bBefore) {
				t.Fatal("Compare changed an input")
			}
		})
	}
}

func TestMerge(t *testing.T) {
	for _, tc := range []struct {
		name       string
		a, b, want VectorClock
	}{
		{"overlap", VectorClock{"n1": 2, "n2": 1}, VectorClock{"n1": 1, "n2": 3}, VectorClock{"n1": 2, "n2": 3}},
		{"different nodes", VectorClock{"n1": 1}, VectorClock{"n2": 2}, VectorClock{"n1": 1, "n2": 2}},
		{"empty", nil, nil, VectorClock{}},
		{"left empty", nil, VectorClock{"n1": 2}, VectorClock{"n1": 2}},
		{"right empty", VectorClock{"n1": 2}, nil, VectorClock{"n1": 2}},
		{"explicit zero", VectorClock{"n1": 0}, nil, VectorClock{"n1": 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aBefore, bBefore := maps.Clone(tc.a), maps.Clone(tc.b)
			got := Merge(tc.a, tc.b)
			if got == nil || !maps.Equal(got, tc.want) {
				t.Fatalf("Merge(%v, %v) = %v; want %v", tc.a, tc.b, got, tc.want)
			}
			// Modifying the result must not modify either input, even an existing key.
			for node := range got {
				got[node]++
			}
			got["new-node"] = 1
			if !maps.Equal(tc.a, aBefore) || !maps.Equal(tc.b, bBefore) {
				t.Fatal("Merge or its returned map changed an input")
			}
		})
	}
}

// Exhaust all 64 clocks with three counters in [0,3]: 4,096 ordered pairs.
// Check the definition and the join properties independently of map iteration.
func TestClockProperties(t *testing.T) {
	var clocks []VectorClock
	for n1 := int64(0); n1 < 4; n1++ {
		for n2 := int64(0); n2 < 4; n2++ {
			for n3 := int64(0); n3 < 4; n3++ {
				vc := VectorClock{}
				for node, count := range (VectorClock{"n1": n1, "n2": n2, "n3": n3}) {
					if count != 0 {
						vc[node] = count
					}
				}
				clocks = append(clocks, vc)
			}
		}
	}
	for _, a := range clocks {
		for _, b := range clocks {
			le := a["n1"] <= b["n1"] && a["n2"] <= b["n2"] && a["n3"] <= b["n3"]
			ge := a["n1"] >= b["n1"] && a["n2"] >= b["n2"] && a["n3"] >= b["n3"]
			want := Concurrent
			switch {
			case le && ge:
				want = Equal
			case le:
				want = Before
			case ge:
				want = After
			}
			if got := Compare(a, b); got != want {
				t.Fatalf("Compare(%v, %v) = %v; want %v", a, b, got, want)
			}
			joined := Merge(a, b)
			if Compare(joined, a) == Before || Compare(joined, a) == Concurrent || Compare(joined, b) == Before || Compare(joined, b) == Concurrent {
				t.Fatalf("join %v does not cover %v and %v", joined, a, b)
			}
			if !maps.Equal(joined, Merge(b, a)) {
				t.Fatalf("merge is not commutative for %v and %v", a, b)
			}
			if Compare(Merge(a, a), a) != Equal {
				t.Fatalf("merge is not idempotent for %v", a)
			}
			for _, node := range []string{"n1", "n2", "n3"} {
				if joined[node] != max(a[node], b[node]) {
					t.Fatalf("join has incorrect counter for %s", node)
				}
			}
		}
	}
}
