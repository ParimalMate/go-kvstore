package merkle

import (
	"fmt"
	"sort"
	"unicode/utf8"
)

const Fanout = 16
const Levels = 3
const BucketCount = 4096

// KeyDigest names a key and its hash. Go cannot name this type KeyHash because
// the package already exposes the KeyHash function.
type KeyDigest struct {
	Key  string `json:"key"`
	Hash Hash   `json:"hash"`
}

// Builder is mutable and not safe for concurrent use. Build makes an independent
// snapshot; callers may reuse the builder without changing earlier trees.
type Builder struct{ keys map[string]Hash }

func (b *Builder) Add(key string, hash Hash) error {
	if !utf8.ValidString(key) {
		return fmt.Errorf("key must be valid UTF-8")
	}
	if b.keys == nil {
		b.keys = make(map[string]Hash)
	}
	b.keys[key] = hash
	return nil
}

type Tree struct {
	nodes   [Levels + 1][]Hash
	buckets [BucketCount][]KeyDigest
}

func (b *Builder) Build() *Tree {
	tree := &Tree{}
	count := 1
	for level := 0; level <= Levels; level++ {
		tree.nodes[level] = make([]Hash, count)
		count *= Fanout
	}
	for key, hash := range b.keys {
		bucket := BucketOf(key)
		tree.buckets[bucket] = append(tree.buckets[bucket], KeyDigest{key, hash})
	}
	for i := range tree.buckets {
		keys := tree.buckets[i]
		sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })
		hashes := make([]Hash, len(keys))
		for j, key := range keys {
			hashes[j] = key.Hash
		}
		tree.nodes[Levels][i] = LeafHash(hashes)
	}
	for level := Levels - 1; level >= 0; level-- {
		for i := range tree.nodes[level] {
			var children [Fanout]Hash
			copy(children[:], tree.nodes[level+1][Fanout*i:Fanout*(i+1)])
			tree.nodes[level][i] = internalHash(children)
		}
	}
	return tree
}

func (t *Tree) Root() Hash { return t.nodes[0][0] }

func (t *Tree) Node(level, index int) (Hash, error) {
	if level < 0 || level > Levels || index < 0 || index >= len(t.nodes[level]) {
		return Hash{}, fmt.Errorf("invalid node (%d,%d)", level, index)
	}
	return t.nodes[level][index], nil
}

func (t *Tree) Children(level, index int) ([Fanout]Hash, error) {
	var result [Fanout]Hash
	if level < 0 || level >= Levels || index < 0 || index >= len(t.nodes[level]) {
		return result, fmt.Errorf("invalid parent (%d,%d)", level, index)
	}
	copy(result[:], t.nodes[level+1][Fanout*index:Fanout*(index+1)])
	return result, nil
}

func (t *Tree) BucketKeys(index int) ([]KeyDigest, error) {
	if index < 0 || index >= BucketCount {
		return nil, fmt.Errorf("invalid bucket %d", index)
	}
	result := make([]KeyDigest, len(t.buckets[index]))
	copy(result, t.buckets[index])
	return result, nil
}
