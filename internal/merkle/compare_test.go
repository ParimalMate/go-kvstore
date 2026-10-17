package merkle

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type fakeRemote struct {
	tree                           *Tree
	hashes, nodeBatch, bucketBatch int
	nodes                          func(int, []int) (NodeBatch, error)
	buckets                        func([]int) (map[int][]KeyDigest, error)
}

func (f *fakeRemote) Nodes(level int, indexes []int) (NodeBatch, error) {
	if len(indexes) > f.nodeBatch {
		f.nodeBatch = len(indexes)
	}
	f.hashes += len(indexes)
	if f.nodes != nil {
		return f.nodes(level, indexes)
	}
	batch := NodeBatch{Fanout: Fanout, Levels: Levels}
	for _, i := range indexes {
		h, err := f.tree.Node(level, i)
		if err != nil {
			return batch, err
		}
		batch.Hashes = append(batch.Hashes, h)
	}
	return batch, nil
}
func (f *fakeRemote) Buckets(indexes []int) (map[int][]KeyDigest, error) {
	if len(indexes) > f.bucketBatch {
		f.bucketBatch = len(indexes)
	}
	if f.buckets != nil {
		return f.buckets(indexes)
	}
	result := map[int][]KeyDigest{}
	for _, i := range indexes {
		keys, err := f.tree.BucketKeys(i)
		if err != nil {
			return nil, err
		}
		result[i] = keys
	}
	return result, nil
}

func TestComparePrunesEqualBranches(t *testing.T) {
	var b Builder
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := b.Add(key, mustHash(t, key, []Version{{"value", nil}})); err != nil {
			t.Fatal(err)
		}
	}
	local := b.Build()
	remote := &fakeRemote{tree: local}
	diff, stats, err := Compare(local, remote, CompareOptions{})
	if err != nil || len(diff.Keys) != 0 || stats.HashesFetched != 1 || remote.hashes != 1 || stats.BucketRequests != 0 {
		t.Fatal(diff, stats, err)
	}
	b.Add("key-5", mustHash(t, "key-5", []Version{{"changed", nil}}))
	remote = &fakeRemote{tree: b.Build()}
	diff, stats, err = Compare(local, remote, CompareOptions{})
	if err != nil || !reflect.DeepEqual(diff.Keys, []string{"key-5"}) || stats.HashesFetched != 49 || remote.hashes != 49 {
		t.Fatal(diff, stats, err)
	}
}

func TestCompareUnionBudgetAndBatchLimits(t *testing.T) {
	var a, b Builder
	a.Add("only-local", Hash{1})
	b.Add("only-remote", Hash{2})
	a.Add("different", Hash{3})
	b.Add("different", Hash{4})
	diff, _, err := Compare(a.Build(), &fakeRemote{tree: b.Build()}, CompareOptions{Pick: func(n int) int { return n - 1 }})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, key := range diff.Keys {
		got[key] = true
	}
	if !reflect.DeepEqual(got, map[string]bool{"only-local": true, "only-remote": true, "different": true}) {
		t.Fatal(diff)
	}
	for i := 0; i < 10000; i++ {
		b.Add(fmt.Sprintf("key-%d", i), Hash{1})
	}
	remote := &fakeRemote{tree: b.Build()}
	calls := 0
	diff, stats, err := Compare((&Builder{}).Build(), remote, CompareOptions{MaxKeys: 7, Pick: func(n int) int { calls++; return 0 }})
	if err != nil || len(diff.Keys) != 7 || !diff.Truncated || calls == 0 {
		t.Fatal(diff, stats, err)
	}
	if remote.nodeBatch > 256 || remote.nodeBatch != 256 || remote.bucketBatch > 64 || remote.bucketBatch != 64 {
		t.Fatal(remote.nodeBatch, remote.bucketBatch)
	}
}

func TestCompareChangingBucketAndInvalidRemote(t *testing.T) {
	empty := (&Builder{}).Build()
	var b Builder
	b.Add("key", Hash{1})
	tree := b.Build()
	bucket := BucketOf("key")
	for _, tc := range []struct {
		name          string
		change        func(*fakeRemote)
		wantErr, skip bool
	}{
		{"changed snapshot", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) { return map[int][]KeyDigest{bucket: {{"key", Hash{2}}}}, nil }
		}, false, true},
		{"wrong shape", func(f *fakeRemote) {
			f.nodes = func(int, []int) (NodeBatch, error) { return NodeBatch{Fanout: 2, Levels: 3, Hashes: []Hash{{1}}}, nil }
		}, true, false},
		{"wrong levels", func(f *fakeRemote) {
			f.nodes = func(int, []int) (NodeBatch, error) { return NodeBatch{Fanout: 16, Levels: 4, Hashes: []Hash{{1}}}, nil }
		}, true, false},
		{"missing hashes", func(f *fakeRemote) {
			f.nodes = func(int, []int) (NodeBatch, error) { return NodeBatch{Fanout: 16, Levels: 3}, nil }
		}, true, false},
		{"node error", func(f *fakeRemote) {
			f.nodes = func(int, []int) (NodeBatch, error) { return NodeBatch{}, errors.New("failed") }
		}, true, false},
		{"bucket error", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) { return nil, errors.New("failed") }
		}, true, false},
		{"wrong bucket", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) { return map[int][]KeyDigest{-1: nil}, nil }
		}, true, false},
		{"out of range", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) { return map[int][]KeyDigest{4096: nil}, nil }
		}, true, false},
		{"unrequested", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) { return map[int][]KeyDigest{(bucket + 1) % 4096: nil}, nil }
		}, true, false},
		{"missing bucket", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) { return map[int][]KeyDigest{}, nil }
		}, true, false},
		{"wrong key placement", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) {
				return map[int][]KeyDigest{bucket: {{"other-key", Hash{1}}}}, nil
			}
		}, true, false},
		{"duplicate key", func(f *fakeRemote) {
			f.buckets = func([]int) (map[int][]KeyDigest, error) {
				return map[int][]KeyDigest{bucket: {{"key", Hash{1}}, {"key", Hash{1}}}}, nil
			}
		}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := &fakeRemote{tree: tree}
			tc.change(remote)
			diff, stats, err := Compare(empty, remote, CompareOptions{})
			if (err != nil) != tc.wantErr {
				t.Fatal(err)
			}
			if tc.skip && (len(diff.Keys) != 0 || !reflect.DeepEqual(diff.SkippedBuckets, []int{bucket}) || stats.ChangedBuckets != 1) {
				t.Fatal(diff, stats)
			}
		})
	}
}

func TestWireHashLengthValidation(t *testing.T) {
	original := Hash{1, 2, 3}
	body, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Hash
	if err = json.Unmarshal(body, &decoded); err != nil || decoded != original {
		t.Fatal(decoded, err)
	}
	for _, bad := range []string{`null`, `""`, `"` + strings.Repeat("0", 62) + `"`, `"` + strings.Repeat("0", 66) + `"`, `"` + strings.Repeat("z", 64) + `"`, `[1,2]`} {
		decoded = original
		if err = json.Unmarshal([]byte(bad), &decoded); err == nil {
			t.Fatal("accepted bad hash", bad)
		}
		if decoded != original {
			t.Fatal("failed decoding mutated hash")
		}
	}
}

func TestCompareRejectsInvalidOptions(t *testing.T) {
	tree := (&Builder{}).Build()
	for _, budget := range []int{-1, 100001} {
		if _, _, err := Compare(tree, &fakeRemote{tree: tree}, CompareOptions{MaxKeys: budget}); err == nil {
			t.Fatal("bad budget accepted")
		}
	}
	if _, _, err := Compare(nil, &fakeRemote{tree: tree}, CompareOptions{}); err == nil {
		t.Fatal("nil tree accepted")
	}
	if _, _, err := Compare(tree, nil, CompareOptions{}); err == nil {
		t.Fatal("nil remote accepted")
	}
}
