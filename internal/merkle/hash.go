// Package merkle hashes immutable key/version snapshots. It has no dependency
// on storage timestamps, persistence or networking.
package merkle

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"unicode/utf8"
)

type Hash [sha256.Size]byte

type Version struct {
	Value string
	Clock map[string]int64
}

// BucketOf uses the first 12 bits of SHA-256, independent of other stored keys.
func BucketOf(key string) int {
	sum := sha256.Sum256([]byte(key))
	return int(sum[0])<<4 | int(sum[1]>>4)
}

// CanonicalClock excludes zero counters and never mutates the caller's map.
func CanonicalClock(clock map[string]int64) ([]byte, error) {
	ids := make([]string, 0, len(clock))
	for id, counter := range clock {
		if counter < 0 {
			return nil, fmt.Errorf("negative clock counter for %q", id)
		}
		if counter == 0 {
			continue
		}
		if !utf8.ValidString(id) || len(id) > 1<<16-1 {
			return nil, fmt.Errorf("clock ID must be valid UTF-8 and fit uint16 length")
		}
		ids = append(ids, id)
	}
	if uint64(len(ids)) > 1<<32-1 {
		return nil, fmt.Errorf("too many clock entries")
	}
	sort.Strings(ids)
	result := binary.BigEndian.AppendUint32(nil, uint32(len(ids)))
	for _, id := range ids {
		result = binary.BigEndian.AppendUint16(result, uint16(len(id)))
		result = append(result, id...)
		result = binary.BigEndian.AppendUint64(result, uint64(clock[id]))
	}
	return result, nil
}

// KeyHash hashes a key and its full sibling set using the versioned 0x02 domain.
// Ts is intentionally absent from Version: replica-local time cannot affect it.
func KeyHash(key string, versions []Version) (Hash, error) {
	if !utf8.ValidString(key) || uint64(len(key)) > 1<<32-1 {
		return Hash{}, fmt.Errorf("invalid key text or length")
	}
	if uint64(len(versions)) > 1<<32-1 {
		return Hash{}, fmt.Errorf("too many versions")
	}
	type encodedVersion struct {
		value string
		clock []byte
	}
	ordered := make([]encodedVersion, 0, len(versions))
	for _, version := range versions {
		if !utf8.ValidString(version.Value) || uint64(len(version.Value)) > 1<<32-1 {
			return Hash{}, fmt.Errorf("invalid value text or length")
		}
		clock, err := CanonicalClock(version.Clock)
		if err != nil {
			return Hash{}, err
		}
		ordered = append(ordered, encodedVersion{version.Value, clock})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if order := bytes.Compare(ordered[i].clock, ordered[j].clock); order != 0 {
			return order < 0
		}
		return ordered[i].value < ordered[j].value
	})
	h := sha256.New()
	h.Write([]byte{0x02})
	h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(key))))
	h.Write([]byte(key))
	h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(ordered))))
	for _, version := range ordered {
		h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(version.value))))
		h.Write([]byte(version.value))
		h.Write(version.clock)
	}
	var result Hash
	copy(result[:], h.Sum(nil))
	return result, nil
}

// LeafHash expects key-state hashes in lexicographic key order, not hash order.
// Empty buckets have the special all-zero hash.
func LeafHash(sortedKeyHashes []Hash) Hash {
	if len(sortedKeyHashes) == 0 {
		return Hash{}
	}
	h := sha256.New()
	h.Write([]byte{0x00})
	for _, hash := range sortedKeyHashes {
		h.Write(hash[:])
	}
	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

func internalHash(children [Fanout]Hash) Hash {
	h := sha256.New()
	h.Write([]byte{0x01})
	for _, child := range children {
		h.Write(child[:])
	}
	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}
