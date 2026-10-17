package merkle

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Wire hashes are exactly 64 hexadecimal characters (32 bytes). Custom decoding
// avoids the silent truncation/zero-padding of JSON into a fixed Go array.
func (h Hash) MarshalJSON() ([]byte, error) { return json.Marshal(hex.EncodeToString(h[:])) }
func (h *Hash) UnmarshalJSON(data []byte) error {
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	if len(encoded) != 64 {
		return fmt.Errorf("hash must contain exactly 32 bytes")
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return err
	}
	copy(h[:], decoded)
	return nil
}
