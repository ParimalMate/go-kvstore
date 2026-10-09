package gossip

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestMessageValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message GossipMessage
		valid   bool
	}{
		{"valid", GossipMessage{"n1", []WireMember{{"n2", 1, 0}}}, true},
		{"empty members", GossipMessage{"n1", nil}, true},
		{"bad sender", GossipMessage{"bad id", nil}, false},
		{"empty sender", GossipMessage{"", nil}, false},
		{"bad member", GossipMessage{"n1", []WireMember{{"_bad", 1, 0}}}, false},
		{"long id", GossipMessage{"n1", []WireMember{{strings.Repeat("a", 65), 1, 0}}}, false},
		{"zero generation", GossipMessage{"n1", []WireMember{{"n2", 0, 0}}}, false},
		{"negative generation", GossipMessage{"n1", []WireMember{{"n2", -1, 0}}}, false},
		{"negative heartbeat", GossipMessage{"n1", []WireMember{{"n2", 1, -1}}}, false},
		{"duplicate", GossipMessage{"n1", []WireMember{{"n2", 1, 0}, {"n2", 1, 1}}}, false},
		{"over limit", GossipMessage{"n1", make([]WireMember, MaxMembers+1)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.message.Validate(); (err == nil) != tc.valid {
				t.Fatalf("unexpected validation result: %v", err)
			}
		})
	}
}

func TestIntegerDecoding(t *testing.T) {
	for _, field := range []string{"generation", "heartbeat"} {
		for _, number := range []string{"1.5", "1e30"} {
			body := `{"id":"n1","` + field + `":` + number + `}`
			var member WireMember
			if err := json.Unmarshal([]byte(body), &member); err == nil {
				t.Fatalf("accepted %s", body)
			}
		}
	}
}

func TestDigestRoundTrip(t *testing.T) {
	table, _ := testTable(t)
	table.Merge([]WireMember{{"n3", 3, 9}, {"n2", 2, 4}})
	original := GossipMessage{"n1", table.Digest()}
	body, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded GossipMessage
	if err = json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if err = decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("round trip changed digest: %v", decoded)
	}
	for _, status := range []Status{Alive, Suspect, Dead} {
		body, err = json.Marshal(status)
		if err != nil || string(body) != `"`+string(status)+`"` {
			t.Fatalf("status JSON: %s %v", body, err)
		}
	}
}
