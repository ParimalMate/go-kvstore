package main

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidateConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*config)
		want   string
	}{
		{"valid defaults with two peers", func(c *config) {}, ""},
		{"standalone", func(c *config) { c.Peers = nil; c.W = 1; c.R = 1 }, ""},
		{"empty id", func(c *config) { c.ID = "" }, "--id"},
		{"bad leading character", func(c *config) { c.ID = "_n1" }, "--id"},
		{"id space", func(c *config) { c.ID = "n 1" }, "--id"},
		{"id newline", func(c *config) { c.ID = "n1\n" }, "--id"},
		{"id too long", func(c *config) { c.ID = strings.Repeat("n", 65) }, "--id"},
		{"id at limit", func(c *config) { c.ID = "N1._-" + strings.Repeat("a", 59) }, ""},
		{"unicode id", func(c *config) { c.ID = "नोड" }, "--id"},
		{"missing peer port", func(c *config) { c.Peers[0] = "localhost" }, "invalid peer"},
		{"nonnumeric peer port", func(c *config) { c.Peers[0] = "localhost:http" }, "invalid peer"},
		{"peer port out of range", func(c *config) { c.Peers[0] = "localhost:65536" }, "invalid peer"},
		{"duplicate peer", func(c *config) { c.Peers[1] = c.Peers[0] }, "duplicate peer"},
		{"self localhost", func(c *config) { c.Peers[0] = "localhost:8080" }, "own peer"},
		{"self ipv4", func(c *config) { c.Peers[0] = "127.0.0.1:8080" }, "own peer"},
		{"self other loopback", func(c *config) { c.Peers[0] = "127.0.0.2:8080" }, "own peer"},
		{"self ipv6", func(c *config) { c.Peers[0] = "[::1]:8080" }, "own peer"},
		{"self wildcard", func(c *config) { c.Peers[0] = "0.0.0.0:8080" }, "own peer"},
		{"self ipv6 wildcard", func(c *config) { c.Peers[0] = "[::]:8080" }, "own peer"},
		{"self empty host", func(c *config) { c.Peers[0] = ":8080" }, "own peer"},
		{"self port with leading zero", func(c *config) { c.Peers[0] = "127.0.0.1:08080" }, "own peer"},
		{"remote same port", func(c *config) { c.Peers[0] = "192.0.2.10:8080" }, ""},
		{"invalid own port", func(c *config) { c.Port = "abc" }, "--port"},
		{"zero own port", func(c *config) { c.Port = "0" }, "--port"},
		{"W below one", func(c *config) { c.W = 0 }, "between 1 and N"},
		{"W above N", func(c *config) { c.W = 4 }, "between 1 and N"},
		{"R below one", func(c *config) { c.R = 0 }, "between 1 and N"},
		{"R above N", func(c *config) { c.R = 4 }, "between 1 and N"},
		{"no guaranteed overlap", func(c *config) { c.W = 1; c.R = 2 }, "W+R"},
		{"interval too short", func(c *config) { c.GossipInterval = 10*time.Millisecond - time.Nanosecond }, "--gossip-interval"},
		{"interval too long", func(c *config) { c.GossipInterval = time.Minute + time.Nanosecond }, "--gossip-interval"},
		{"minimum interval", func(c *config) {
			c.GossipInterval = 10 * time.Millisecond
			c.SuspectAfter = 30 * time.Millisecond
			c.DeadAfter = 31 * time.Millisecond
		}, ""},
		{"maximum interval", func(c *config) {
			c.GossipInterval = time.Minute
			c.SuspectAfter = 3 * time.Minute
			c.DeadAfter = 4 * time.Minute
		}, ""},
		{"suspect too short", func(c *config) { c.SuspectAfter = 3*time.Second - time.Nanosecond }, "--suspect-after"},
		{"suspect at boundary", func(c *config) { c.SuspectAfter = 3 * time.Second }, ""},
		{"dead equals suspect", func(c *config) { c.DeadAfter = c.SuspectAfter }, "--dead-after"},
		{"dead before suspect", func(c *config) { c.DeadAfter = time.Second }, "--dead-after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.Peers = []string{"localhost:8081", "localhost:8082"}
			tc.change(&cfg)
			err := validateConfig(cfg)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestParsePeers(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want []string
	}{
		{"", nil},
		{" , ", nil},
		{" localhost:8081, ,127.0.0.1:8082 ", []string{"localhost:8081", "127.0.0.1:8082"}},
	} {
		if got := parsePeers(tc.raw); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("parsePeers(%q)=%v want=%v", tc.raw, got, tc.want)
		}
	}
}
