package merkle

import (
	"fmt"
	"math/rand"
	"sort"
	"unicode/utf8"
)

// NodeBatch retains protocol metadata so comparison can reject incompatible
// shapes. A bare []Hash response cannot express fanout/levels mismatches.
type NodeBatch struct {
	Fanout int    `json:"fanout"`
	Levels int    `json:"levels"`
	Hashes []Hash `json:"hashes"`
}

type Remote interface {
	Nodes(level int, indexes []int) (NodeBatch, error)
	Buckets(indexes []int) (map[int][]KeyDigest, error)
}

type CompareOptions struct {
	MaxKeys int           // Zero selects the default 1000; otherwise 1..100000.
	Pick    func(int) int // Optional uniform index source, e.g. rand.Intn.
}

type Diff struct {
	RootsEqual     bool
	RemoteHashes   map[string]Hash // Candidate absent here means local-only.
	Keys           []string
	Buckets        []int
	SkippedBuckets []int // Listing no longer matches the descended leaf hash.
	Truncated      bool
}

type Stats struct {
	NodeRequests   int
	HashesFetched  int
	BucketRequests int
	BucketsFetched int
	ChangedBuckets int
}

// Compare finds candidate keys, never applies data. Per-key reconciliation is
// authoritative; snapshots can change between requests. Remote calls must be
// bounded by the caller's transport/context. No HTTP or store access occurs here.
func Compare(local *Tree, remote Remote, opts CompareOptions) (Diff, Stats, error) {
	diff := Diff{Keys: []string{}, Buckets: []int{}, SkippedBuckets: []int{}, RemoteHashes: make(map[string]Hash)}
	var stats Stats
	if local == nil || remote == nil {
		return diff, stats, fmt.Errorf("local tree and remote are required")
	}
	if opts.MaxKeys == 0 {
		opts.MaxKeys = 1000
	}
	if opts.MaxKeys < 1 || opts.MaxKeys > 100000 {
		return diff, stats, fmt.Errorf("max keys must be 1..100000")
	}
	if opts.Pick == nil {
		opts.Pick = rand.Intn
	}
	indexes := []int{0}
	leaves := make(map[int]Hash)
	for level := 0; level <= Levels && len(indexes) > 0; level++ {
		next := []int{}
		for start := 0; start < len(indexes); start += 256 {
			end := min(start+256, len(indexes))
			batch := append([]int(nil), indexes[start:end]...)
			stats.NodeRequests++
			response, err := remote.Nodes(level, batch)
			if err != nil {
				return diff, stats, err
			}
			if response.Fanout != Fanout || response.Levels != Levels {
				return diff, stats, fmt.Errorf("incompatible remote tree shape")
			}
			if len(response.Hashes) != len(batch) {
				return diff, stats, fmt.Errorf("wrong node hash count")
			}
			stats.HashesFetched += len(response.Hashes)
			for offset, index := range indexes[start:end] {
				own, err := local.Node(level, index)
				if err != nil {
					return diff, stats, err
				}
				hash := response.Hashes[offset]
				if level == 0 {
					diff.RootsEqual = own == hash
				}
				if own == hash {
					continue
				}
				if level == Levels {
					leaves[index] = hash
					diff.Buckets = append(diff.Buckets, index)
				} else {
					for child := 0; child < Fanout; child++ {
						next = append(next, Fanout*index+child)
					}
				}
			}
		}
		indexes = next
	}
	// Fisher-Yates across buckets prevents low-numbered buckets monopolizing a
	// bounded cycle. Do not mutate caller-provided snapshots or remote listings.
	for i := len(diff.Buckets) - 1; i > 0; i-- {
		j := opts.Pick(i + 1)
		if j < 0 || j > i {
			return diff, stats, fmt.Errorf("invalid random index")
		}
		diff.Buckets[i], diff.Buckets[j] = diff.Buckets[j], diff.Buckets[i]
	}
	for start := 0; start < len(diff.Buckets); start += 64 {
		end := min(start+64, len(diff.Buckets))
		batch := append([]int(nil), diff.Buckets[start:end]...)
		stats.BucketRequests++
		listings, err := remote.Buckets(batch)
		if err != nil {
			return diff, stats, err
		}
		if len(listings) != end-start {
			return diff, stats, fmt.Errorf("wrong bucket listing count")
		}
		requested := make(map[int]bool, len(batch))
		for _, index := range diff.Buckets[start:end] {
			requested[index] = true
		}
		for index := range listings {
			if index < 0 || index >= BucketCount || !requested[index] {
				return diff, stats, fmt.Errorf("unexpected bucket index %d", index)
			}
		}
		for _, index := range diff.Buckets[start:end] {
			keys, ok := listings[index]
			if !ok {
				return diff, stats, fmt.Errorf("missing bucket %d", index)
			}
			stats.BucketsFetched++
			keys = append([]KeyDigest(nil), keys...)
			sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })
			hashes := make([]Hash, len(keys))
			remoteKeys := make(map[string]Hash, len(keys))
			for i, key := range keys {
				if !utf8.ValidString(key.Key) || BucketOf(key.Key) != index {
					return diff, stats, fmt.Errorf("key does not belong to bucket %d", index)
				}
				if i > 0 && keys[i-1].Key == key.Key {
					return diff, stats, fmt.Errorf("duplicate key in bucket %d", index)
				}
				hashes[i] = key.Hash
				remoteKeys[key.Key] = key.Hash
			}
			if LeafHash(hashes) != leaves[index] {
				diff.SkippedBuckets = append(diff.SkippedBuckets, index)
				stats.ChangedBuckets++
				continue
			}
			own, _ := local.BucketKeys(index)
			listedHashes := make(map[string]Hash, len(remoteKeys))
			for key, hash := range remoteKeys {
				listedHashes[key] = hash
			}
			candidates := []string{}
			for _, key := range own {
				other, found := remoteKeys[key.Key]
				if !found || other != key.Hash {
					candidates = append(candidates, key.Key)
				}
				delete(remoteKeys, key.Key)
			}
			for key := range remoteKeys {
				candidates = append(candidates, key)
			}
			sort.Strings(candidates)
			for _, key := range candidates {
				if len(diff.Keys) == opts.MaxKeys {
					diff.Truncated = true
					return diff, stats, nil
				}
				diff.Keys = append(diff.Keys, key)
				if hash, found := listedHashes[key]; found {
					diff.RemoteHashes[key] = hash
				}
			}
		}
	}
	return diff, stats, nil
}
