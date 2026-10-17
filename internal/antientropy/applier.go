// Package antientropy defines the boundaries for background replica repair.
package antientropy

import "kvstore/internal/replication"

// Applier applies remote versions without generating new clocks. Implementations
// serialize with coordinator writes. An error may follow partial progress;
// changed counts versions that changed state before that error. A caller may
// continue with subsequent versions, and may safely retry successful versions.
type Applier interface {
	ApplyVersions(key string, versions []replication.Version) (changed int, err error)
}
