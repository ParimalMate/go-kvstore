package gossip

import (
	"fmt"
	"regexp"
)

const MaxMembers = 256
const MaxBodyBytes = 64 * 1024

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func ValidID(id string) bool { return idPattern.MatchString(id) }

type WireMember struct {
	ID         string `json:"id"`
	Generation int64  `json:"generation"`
	Heartbeat  int64  `json:"heartbeat"`
}

func (m WireMember) Validate() error {
	if !ValidID(m.ID) {
		return fmt.Errorf("invalid member ID %q", m.ID)
	}
	if m.Generation < 1 {
		return fmt.Errorf("generation must be positive")
	}
	if m.Heartbeat < 0 {
		return fmt.Errorf("heartbeat must be nonnegative")
	}
	return nil
}

type GossipMessage struct {
	From    string       `json:"from"`
	Members []WireMember `json:"members"`
}

func (m GossipMessage) Validate() error {
	if !ValidID(m.From) {
		return fmt.Errorf("invalid sender ID %q", m.From)
	}
	if len(m.Members) > MaxMembers {
		return fmt.Errorf("too many members: maximum %d", MaxMembers)
	}
	seen := make(map[string]bool, len(m.Members))
	for _, member := range m.Members {
		if err := member.Validate(); err != nil {
			return err
		}
		if seen[member.ID] {
			return fmt.Errorf("duplicate member ID %q", member.ID)
		}
		seen[member.ID] = true
	}
	return nil
}
