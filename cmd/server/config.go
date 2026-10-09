package main

import (
	"fmt"
	"kvstore/internal/gossip"
	"net"
	"strconv"
	"strings"
	"time"
)

type config struct {
	ID             string
	Port           string
	DataFile       string
	Peers          []string
	W              int
	R              int
	GossipInterval time.Duration
	SuspectAfter   time.Duration
	DeadAfter      time.Duration
}

func defaultConfig() config {
	return config{
		ID: "n1", Port: "8080", DataFile: "kvlog", W: 2, R: 2,
		GossipInterval: time.Second, SuspectAfter: 10 * time.Second, DeadAfter: 20 * time.Second,
	}
}

func parsePeers(raw string) []string {
	var peers []string
	for _, peer := range strings.Split(raw, ",") {
		if peer = strings.TrimSpace(peer); peer != "" {
			peers = append(peers, peer)
		}
	}
	return peers
}

// validateConfig performs no I/O and never exits the process. main owns reporting.
func validateConfig(cfg config) error {
	if !gossip.ValidID(cfg.ID) {
		return fmt.Errorf("invalid --id %q: expected 1-64 letters, digits, dots, underscores or hyphens, starting with a letter or digit", cfg.ID)
	}
	selfPort, err := parsePort(cfg.Port)
	if err != nil {
		return fmt.Errorf("invalid --port: %w", err)
	}
	seen := make(map[string]bool, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		host, port, err := net.SplitHostPort(peer)
		if err != nil {
			return fmt.Errorf("invalid peer address %q: %w", peer, err)
		}
		peerPort, err := parsePort(port)
		if err != nil {
			return fmt.Errorf("invalid peer address %q: %w", peer, err)
		}
		if seen[peer] {
			return fmt.Errorf("duplicate peer address %q: replicas must be distinct", peer)
		}
		seen[peer] = true
		if peerPort != selfPort {
			continue
		}
		ip := net.ParseIP(host)
		isLocalhost := strings.EqualFold(host, "localhost") || host == ""
		isLoopback := ip != nil && ip.IsLoopback()
		isWildcard := ip != nil && ip.IsUnspecified()
		if isLocalhost || isLoopback || isWildcard {
			return fmt.Errorf("node cannot be its own peer: %q", peer)
		}
	}
	n := len(cfg.Peers) + 1
	if cfg.W < 1 || cfg.W > n || cfg.R < 1 || cfg.R > n {
		return fmt.Errorf("invalid quorum configuration: W=%d R=%d N=%d; W and R must each be between 1 and N", cfg.W, cfg.R, n)
	}
	if cfg.W <= n-cfg.R {
		return fmt.Errorf("invalid quorum configuration: W=%d R=%d N=%d; W+R must be greater than N", cfg.W, cfg.R, n)
	}
	if cfg.GossipInterval < 10*time.Millisecond || cfg.GossipInterval > time.Minute {
		return fmt.Errorf("--gossip-interval must be between 10ms and 1m")
	}
	if cfg.SuspectAfter < 3*cfg.GossipInterval {
		return fmt.Errorf("--suspect-after must be at least 3 times --gossip-interval")
	}
	if cfg.DeadAfter <= cfg.SuspectAfter {
		return fmt.Errorf("--dead-after must be greater than --suspect-after")
	}
	return nil
}

func parsePort(value string) (int, error) {
	// Reject service names and signs so configured HTTP addresses are unambiguous.
	if value == "" || strings.Trim(value, "0123456789") != "" {
		return 0, fmt.Errorf("port %q must be numeric", value)
	}
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("port %q must be between 1 and 65535", value)
	}
	return port, nil
}
