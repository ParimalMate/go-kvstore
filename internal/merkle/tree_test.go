package merkle

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

func TestEmptyTreeGolden(t *testing.T) {
	tree := (&Builder{}).Build()
	root := tree.Root()
	if hex.EncodeToString(root[:]) != "de7aee71d98c6b43ccbb78d653b7aa3972f2b973a0a61ddcd68ea7f7bd0636ef" {
		t.Fatalf("root=%x", root)
	}
	for i := 0; i < BucketCount; i++ {
		leaf, err := tree.Node(Levels, i)
		keys, keyErr := tree.BucketKeys(i)
		if err != nil || keyErr != nil || leaf != (Hash{}) || len(keys) != 0 {
			t.Fatal(i, err, keyErr)
		}
	}
}

func TestTreeOrderAndSingleChange(t *testing.T) {
	var first, second Builder
	hashes := make([]Hash, 10000)
	for i := range hashes {
		key := fmt.Sprintf("key-%d", i)
		hashes[i] = mustHash(t, key, []Version{{"value", map[string]int64{"n1": 1}}})
		if err := first.Add(key, hashes[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, i := range rand.New(rand.NewSource(42)).Perm(len(hashes)) {
		if err := second.Add(fmt.Sprintf("key-%d", i), hashes[i]); err != nil {
			t.Fatal(err)
		}
	}
	a, b := first.Build(), second.Build()
	for level, count := 0, 1; level <= Levels; level, count = level+1, count*Fanout {
		for i := 0; i < count; i++ {
			x, _ := a.Node(level, i)
			y, _ := b.Node(level, i)
			if x != y {
				t.Fatalf("order changed (%d,%d)", level, i)
			}
		}
	}
	for i := 0; i < BucketCount; i++ {
		keys, _ := a.BucketKeys(i)
		other, _ := b.BucketKeys(i)
		if !reflect.DeepEqual(keys, other) {
			t.Fatal("listing order changed")
		}
		leafHashes := make([]Hash, len(keys))
		for j, key := range keys {
			if BucketOf(key.Key) != i || (j > 0 && keys[j-1].Key >= key.Key) {
				t.Fatal("bad bucket listing")
			}
			leafHashes[j] = key.Hash
		}
		node, _ := a.Node(Levels, i)
		if LeafHash(leafHashes) != node {
			t.Fatal("leaf cannot be recomputed")
		}
	}
	key := "key-123"
	if err := second.Add(key, mustHash(t, key, []Version{{"changed", nil}})); err != nil {
		t.Fatal(err)
	}
	changed := second.Build()
	for level, count := 0, 1; level <= Levels; level, count = level+1, count*Fanout {
		differences := 0
		for i := 0; i < count; i++ {
			x, _ := a.Node(level, i)
			y, _ := changed.Node(level, i)
			if x != y {
				differences++
			}
		}
		if differences != 1 {
			t.Fatalf("level %d: %d changed nodes", level, differences)
		}
	}
	if b.Root() != a.Root() {
		t.Fatal("builder reuse mutated previous snapshot")
	}
}

func TestTreeCopiesBoundsAndChildren(t *testing.T) {
	var builder Builder
	if err := builder.Add("key", Hash{1}); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(string([]byte{0xff}), Hash{}); err == nil {
		t.Fatal("invalid key accepted")
	}
	tree := builder.Build()
	keys, _ := tree.BucketKeys(BucketOf("key"))
	keys[0].Key = "changed"
	keys[0].Hash = Hash{2}
	again, _ := tree.BucketKeys(BucketOf("key"))
	if again[0] != (KeyDigest{"key", Hash{1}}) {
		t.Fatal("listing aliases snapshot")
	}
	for level, count := 0, 1; level < Levels; level, count = level+1, count*Fanout {
		for i := 0; i < count; i++ {
			children, err := tree.Children(level, i)
			if err != nil {
				t.Fatal(err)
			}
			for j, child := range children {
				node, _ := tree.Node(level+1, Fanout*i+j)
				if child != node {
					t.Fatal("wrong child addressing")
				}
			}
			parent, _ := tree.Node(level, i)
			if internalHash(children) != parent {
				t.Fatal("wrong parent hash")
			}
		}
	}
	for _, pair := range [][2]int{{-1, 0}, {4, 0}, {0, -1}, {0, 1}, {3, 4096}} {
		if _, err := tree.Node(pair[0], pair[1]); err == nil {
			t.Fatal("accepted invalid node", pair)
		}
	}
	for _, pair := range [][2]int{{-1, 0}, {3, 0}, {0, -1}, {2, 256}} {
		if _, err := tree.Children(pair[0], pair[1]); err == nil {
			t.Fatal("accepted invalid parent", pair)
		}
	}
	for _, i := range []int{-1, 4096} {
		if _, err := tree.BucketKeys(i); err == nil {
			t.Fatal("accepted invalid bucket")
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tree.Root()
				tree.Children(0, 0)
				tree.Node(3, j)
				tree.BucketKeys(j)
			}
		}()
	}
	wg.Wait()
}
