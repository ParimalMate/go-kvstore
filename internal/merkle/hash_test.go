package merkle

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func mustHash(t *testing.T, key string, versions []Version) Hash {
	t.Helper()
	hash, err := KeyHash(key, versions)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func TestGoldenHashes(t *testing.T) {
	// Independently calculated with Python hashlib + struct big-endian packing.
	for _, tc := range []struct {
		key      string
		versions []Version
		want     string
	}{
		{"", nil, "4322fd2bc0a137d1375b37b3b2e2b4715b3d3dd7ca9682438d4fea0f8437fad3"},
		{"city", []Version{{"Pune", map[string]int64{"n1": 1}}}, "d68af7a69cb4072a5473ffeb81d1e28a45537a10cb0ed695b7bc93c22adc2df7"},
		{"शहर", []Version{{"मुंबई", map[string]int64{"n2": 2, "n1": 0}}, {"Pune", map[string]int64{"n1": 3}}}, "9bdb04edef2e1084f1e1646292111cb60145883b5e1f79d891b8237823305beb"},
	} {
		hash := mustHash(t, tc.key, tc.versions)
		if got := hex.EncodeToString(hash[:]); got != tc.want {
			t.Fatalf("%q: %s want %s", tc.key, got, tc.want)
		}
	}
	clock, err := CanonicalClock(map[string]int64{"n2": 2, "n1": 1, "n3": 0})
	if err != nil || hex.EncodeToString(clock) != "0000000200026e31000000000000000100026e320000000000000002" {
		t.Fatalf("clock=%x err=%v", clock, err)
	}
}

func TestCanonicalEquivalenceAndChanges(t *testing.T) {
	a := Version{"Mumbai", map[string]int64{"n1": 2}}
	b := Version{"Pune", map[string]int64{"n1": 1, "n2": 1}}
	original := []Version{a, b}
	base := mustHash(t, "city", original)
	if base != mustHash(t, "city", []Version{b, a}) {
		t.Fatal("sibling order changed hash")
	}
	zero := Version{"Mumbai", map[string]int64{"n1": 2, "n2": 0}}
	if base != mustHash(t, "city", []Version{zero, b}) {
		t.Fatal("zero counter changed hash")
	}
	// Equal clock bytes are sorted by value as a deterministic tie-breaker.
	x, y := Version{"x", a.Clock}, Version{"y", a.Clock}
	if mustHash(t, "city", []Version{x, y}) != mustHash(t, "city", []Version{y, x}) {
		t.Fatal("value tie-break failed")
	}
	for _, tc := range []struct {
		key      string
		versions []Version
	}{
		{"town", original},
		{"city", []Version{{"Delhi", a.Clock}, b}},
		{"city", []Version{{a.Value, map[string]int64{"n1": 3}}, b}},
		{"city", []Version{a, b, {"Chennai", map[string]int64{"n3": 1}}}},
	} {
		if base == mustHash(t, tc.key, tc.versions) {
			t.Fatal("state change did not change hash")
		}
	}
	if !reflect.DeepEqual(original, []Version{a, b}) || len(a.Clock) != 1 {
		t.Fatal("input mutated")
	}
	if mustHash(t, "ab", []Version{{"c", nil}}) == mustHash(t, "a", []Version{{"bc", nil}}) {
		t.Fatal("length-prefix ambiguity")
	}
	empty, _ := CanonicalClock(nil)
	zeros, _ := CanonicalClock(map[string]int64{"n1": 0})
	if !reflect.DeepEqual(empty, zeros) {
		t.Fatal("empty clock normalization")
	}
	// The hashing type cannot carry timestamps, including through field additions.
	if _, found := reflect.TypeOf(Version{}).FieldByName("Ts"); found {
		t.Fatal("timestamp must not be part of hash input")
	}
}

func TestHashInputValidation(t *testing.T) {
	for _, clock := range []map[string]int64{{"n1": -1}, {string([]byte{0xff}): 1}, {strings.Repeat("a", 65536): 1}} {
		if _, err := CanonicalClock(clock); err == nil {
			t.Fatal("accepted invalid clock")
		}
	}
	for _, tc := range []struct{ key, value string }{{string([]byte{0xff}), "ok"}, {"key", string([]byte{0xfe})}} {
		if _, err := KeyHash(tc.key, []Version{{tc.value, nil}}); err == nil {
			t.Fatal("accepted invalid UTF-8")
		}
	}
}

func TestBucketDistribution(t *testing.T) {
	counts := make([]int, BucketCount)
	for i := 0; i < 100000; i++ {
		counts[BucketOf(fmt.Sprintf("key-%d", i))]++
	}
	for bucket, count := range counts {
		// Mean is 24.4. This loose fixed limit catches a broken prefix extractor.
		if count == 0 || count > 80 {
			t.Fatalf("bucket %d has %d keys", bucket, count)
		}
	}
}
