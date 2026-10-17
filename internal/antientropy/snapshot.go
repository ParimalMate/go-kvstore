package antientropy

import (
	"kvstore/internal/merkle"
	"kvstore/internal/store"
	"sync"
)

type SnapshotStore interface {
	Revision() uint64
	Range(func(key string, versions []store.Entry))
}

// SnapshotCache belongs to one Store lifetime. Its mutex coalesces rebuilds;
// published trees are immutable and can be read after releasing the mutex.
type SnapshotCache struct {
	source   SnapshotStore
	mu       sync.Mutex
	revision uint64
	tree     *merkle.Tree
}

func NewSnapshotCache(source SnapshotStore) *SnapshotCache {
	return &SnapshotCache{source: source}
}

func (c *SnapshotCache) Tree() (*merkle.Tree, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	before := c.source.Revision()
	if c.tree != nil && c.revision == before {
		return c.tree, nil
	}
	var builder merkle.Builder
	var buildErr error
	c.source.Range(func(key string, entries []store.Entry) {
		if buildErr != nil {
			return
		}
		versions := make([]merkle.Version, len(entries))
		for i, entry := range entries {
			versions[i] = merkle.Version{Value: entry.Value, Clock: entry.VC}
		}
		hash, err := merkle.KeyHash(key, versions)
		if err != nil {
			buildErr = err
			return
		}
		buildErr = builder.Add(key, hash)
	})
	if buildErr != nil {
		return nil, buildErr
	}
	tree := builder.Build()
	// Revision and Range are separate lock acquisitions. If a write overlapped
	// this build, return the consistent Range snapshot but do not label it with
	// an uncertain revision or retry forever under continuous writes.
	if c.source.Revision() == before {
		c.tree = tree
		c.revision = before
	}
	return tree, nil
}
