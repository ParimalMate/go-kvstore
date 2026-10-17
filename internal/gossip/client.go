package gossip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (s *Service) exchange(ctx context.Context, seedAddr string) error {
	err := s.exchangeOnce(ctx, seedAddr)
	// Cancellation by shutdown is not evidence that a seed became unreachable.
	if ctx.Err() == nil {
		s.seedResult(seedAddr, err)
	}
	return err
}

func (s *Service) exchangeOnce(ctx context.Context, seedAddr string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	body, err := json.Marshal(GossipMessage{From: s.selfID, Members: s.table.Digest()})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+seedAddr+"/internal/gossip", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	// Read through EOF for valid bounded bodies (allows connection reuse).
	// One extra byte detects overflow; do not drain an unbounded hostile stream.
	payload, err := io.ReadAll(io.LimitReader(response.Body, MaxBodyBytes+1))
	if err != nil {
		return err
	}
	if len(payload) > MaxBodyBytes {
		return fmt.Errorf("gossip response exceeds 64 KiB")
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("gossip response status %d", response.StatusCode)
	}
	message, err := decodeMessage(payload)
	if err != nil {
		return fmt.Errorf("invalid gossip response: %w", err)
	}
	s.inspectIdentity(message)
	if message.From == s.selfID {
		for _, member := range message.Members {
			if member.ID == s.selfID && member.Generation != s.generation {
				// A distinct process with our ID is not an alias. Keep trying this seed
				// so the configuration error remains observable, without adopting it.
				return nil
			}
		}
		s.table.MarkSelfAlias(seedAddr)
		s.mu.Lock()
		report := !s.aliases[seedAddr]
		s.aliases[seedAddr] = true
		s.mu.Unlock()
		if report {
			s.logger.Printf("gossip: seed %s is an alias of this node", seedAddr)
		}
		return nil
	}
	s.merge(message.Members)
	s.table.NoteDirect(message.From, seedAddr)
	return nil
}
