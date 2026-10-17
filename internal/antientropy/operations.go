package antientropy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"unicode/utf8"
)

func (s *Service) HandleStatus(w http.ResponseWriter, r *http.Request) {
	writeResponse(w, s.Status())
}

// HandleRun returns a list even for an explicit single peer. Client cancellation
// stops waiting; the shared worker owns the repair's lifetime.
func (s *Service) HandleRun(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			http.Error(w, "body exceeds 64 KiB", 413)
		} else {
			http.Error(w, "cannot read body", 400)
		}
		return
	}
	var request struct {
		Peer string `json:"peer"`
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if !utf8.Valid(body) || bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
			http.Error(w, "invalid request", 400)
			return
		}
		if err = json.Unmarshal(body, &request); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
	}
	peers := s.eligiblePeers()
	if request.Peer != "" {
		host, port, err := net.SplitHostPort(request.Peer)
		n, parseErr := strconv.Atoi(port)
		if err != nil || host == "" || parseErr != nil || n < 1 || n > 65535 {
			http.Error(w, "peer must be host:port", 400)
			return
		}
		configured := false
		for _, peer := range s.worker.options.Peers {
			if peer == request.Peer {
				configured = true
				break
			}
		}
		if !configured {
			http.Error(w, "peer must be a configured peer", http.StatusBadRequest)
			return
		}
		peers = []string{request.Peer}
	}
	reports := make([]Report, 0, len(peers))
	for _, peer := range peers {
		item, err := s.enqueue(peer)
		if err != nil {
			code := http.StatusServiceUnavailable
			if errors.Is(err, ErrQueueFull) {
				code = http.StatusTooManyRequests
			}
			http.Error(w, err.Error(), code)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-item.done:
			reports = append(reports, item.report)
		}
	}
	writeResponse(w, reports)
}
