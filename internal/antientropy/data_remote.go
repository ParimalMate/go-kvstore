package antientropy

import (
	"encoding/json"
	"fmt"
	"kvstore/internal/replication"
	"kvstore/internal/vectorclock"
	"net/url"
)

func (r *HTTPRemote) pull(key string) ([]replication.Version, error) {
	body, err := r.request("GET", "/internal/kv/"+url.PathEscape(key), nil, replication.MaxReadBytes)
	if err != nil {
		return nil, err
	}
	var answers []struct {
		Value *string                 `json:"value"`
		VC    vectorclock.VectorClock `json:"vc"`
	}
	if err = json.Unmarshal(body, &answers); err != nil {
		return nil, r.invalid(err)
	}
	if answers == nil {
		return nil, r.invalid(fmt.Errorf("expected sibling array"))
	}
	versions := make([]replication.Version, 0, len(answers))
	for _, answer := range answers {
		if answer.Value == nil {
			return nil, r.invalid(fmt.Errorf("missing sibling value"))
		}
		version := replication.Version{Value: *answer.Value, VC: answer.VC}
		if err := replication.ValidateVersion(version); err != nil {
			return nil, r.invalid(err)
		}
		versions = append(versions, version)
	}
	return versions, nil
}

func (r *HTTPRemote) push(key string, version replication.Version) error {
	if err := replication.ValidateVersion(version); err != nil {
		return err
	}
	body, err := json.Marshal(version)
	if err != nil {
		return err
	}
	_, err = r.request("PUT", "/internal/kv/"+url.PathEscape(key), body, MaxResponseBytes)
	return err
}
