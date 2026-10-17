// Package vectorclock compares and combines causal histories.
// Counters represent logical writes, not wall-clock time, and must be nonnegative.
package vectorclock

type VectorClock map[string]int64

// Relation describes the first clock relative to the second clock.
type Relation int

const (
	Equal Relation = iota
	Before
	After
	Concurrent
)

// Compare treats missing entries as zero and never changes either input.
func Compare(a, b VectorClock) Relation {
	less, greater := false, false

	for node, count := range a {
		if count < b[node] {
			less = true
		}
		if count > b[node] {
			greater = true
		}
	}
	// The first loop cannot see nodes present only in b.
	for node, count := range b {
		if _, exists := a[node]; !exists {
			if 0 < count {
				less = true
			}
			if 0 > count {
				greater = true
			}
		}
	}

	switch {
	case less && greater:
		return Concurrent
	case less:
		return Before
	case greater:
		return After
	default:
		return Equal
	}
}

// Merge returns a new map containing the elementwise maximum of both clocks.
// Neither input is modified, and callers can safely increment the returned map.
func Merge(a, b VectorClock) VectorClock {
	merged := make(VectorClock, len(a)+len(b))
	for node, count := range a {
		merged[node] = max(count, b[node])
	}
	for node, count := range b {
		merged[node] = max(a[node], count)
	}
	return merged
}
