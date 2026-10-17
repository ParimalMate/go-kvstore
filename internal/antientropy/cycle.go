package antientropy

import (
	"context"
	"errors"
	"fmt"
	"kvstore/internal/merkle"
	"kvstore/internal/replication"
	"kvstore/internal/store"
	"log"
	"net"
	"net/http"
	"time"
)

const CycleTimeout = 60 * time.Second

type Report struct {
	Peer            string    `json:"peer"`
	StartedAt       time.Time `json:"started_at"`
	DurationMS      int64     `json:"duration_ms"`
	RootsEqual      bool      `json:"roots_equal"`
	HashesFetched   int       `json:"hashes_fetched"` // Comparison hashes; excludes final verification root.
	Requests        int       `json:"requests"`       // All HTTP attempts, including data and verification.
	BucketsDiffed   int       `json:"buckets_diffed"`
	BucketsSkipped  int       `json:"buckets_skipped"`
	KeysPulled      int       `json:"keys_pulled"`
	KeysPushed      int       `json:"keys_pushed"`
	VersionsApplied int       `json:"versions_applied"`
	Collisions      int       `json:"collisions"`
	KeyErrors       int       `json:"key_errors"`
	Truncated       bool      `json:"truncated"`
	RootsEqualAfter bool      `json:"roots_equal_after"`
	Error           string    `json:"error"`
}

type Service struct {
	store   *store.Store
	applier Applier
	logger  *log.Logger
	maxKeys int
	cache   *SnapshotCache
	worker  *workerState
}

func NewService(s *store.Store, applier Applier, logger *log.Logger, maxKeys int, options ...Options) (*Service, error) {
	if s == nil || applier == nil || logger == nil {
		return nil, fmt.Errorf("store, applier and logger are required")
	}
	if maxKeys < 1 || maxKeys > 100000 {
		return nil, fmt.Errorf("max keys must be 1..100000")
	}
	worker, err := newWorker(options)
	if err != nil {
		return nil, err
	}
	return &Service{worker: worker, store: s, applier: applier, logger: logger, maxKeys: maxKeys, cache: NewSnapshotCache(s)}, nil
}

func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	NewTreeHandler(s.cache).RegisterRoutes(mux)
	mux.HandleFunc("POST /internal/ae/run", s.HandleRun)
	mux.HandleFunc("GET /internal/ae/status", s.HandleStatus)
}

// SyncWith repairs one peer, bounded by the caller and the 60s cycle deadline.
// It never deletes. Explicit calls do not consult gossip. The worker
// serializes scheduled calls; every remote write still goes through Applier.
func (s *Service) SyncWith(parent context.Context, peer string) (report Report, cycleErr error) {
	started := time.Now()
	report = Report{Peer: peer, StartedAt: started.UTC()}
	ctx, cancel := context.WithTimeout(parent, CycleTimeout)
	defer cancel()
	remote := NewHTTPRemote(ctx, peer)
	defer func() {
		report.DurationMS = time.Since(started).Milliseconds()
		report.Requests = remote.requests
		if cycleErr != nil {
			report.Error = cycleErr.Error()
		}
		s.recordReport(report)
		if cycleErr != nil || report.KeysPulled > 0 || report.KeysPushed > 0 || report.Collisions > 0 {
			s.logger.Printf("anti-entropy peer=%s pulled=%d pushed=%d applied=%d collisions=%d hashes=%d buckets=%d in %dms error=%q", peer, report.KeysPulled, report.KeysPushed, report.VersionsApplied, report.Collisions, report.HashesFetched, report.BucketsDiffed, report.DurationMS, report.Error)
		}
	}()
	if err := ctx.Err(); err != nil {
		return report, err
	}
	local, err := s.cache.Tree()
	if err != nil {
		return report, err
	}
	diff, stats, err := merkle.Compare(local, remote, merkle.CompareOptions{MaxKeys: s.maxKeys})
	report.RootsEqual = diff.RootsEqual
	report.RootsEqualAfter = diff.RootsEqual
	report.HashesFetched = stats.HashesFetched
	report.BucketsDiffed = len(diff.Buckets)
	report.BucketsSkipped = len(diff.SkippedBuckets)
	report.Truncated = diff.Truncated
	if err != nil {
		return report, err
	}
	var firstKeyError error
	for _, key := range diff.Keys {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		_, hasRemote := diff.RemoteHashes[key]
		err := s.repairKey(remote, key, hasRemote, &report)
		if err == nil {
			continue
		}
		report.KeyErrors++
		if unreachable(err) {
			return report, err
		}
		if firstKeyError == nil {
			firstKeyError = err
		}
		s.logger.Printf("anti-entropy peer=%s key=%q repair failed: %v", peer, key, err)
	}
	if report.VersionsApplied > 0 || report.KeysPushed > 0 {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		updated, err := s.cache.Tree()
		if err != nil {
			return report, err
		}
		root, err := remote.Nodes(0, []int{0})
		if err != nil {
			return report, err
		}
		report.RootsEqualAfter = updated.Root() == root.Hashes[0]
	}
	if firstKeyError != nil {
		return report, fmt.Errorf("%d key failures: %w", report.KeyErrors, firstKeyError)
	}
	return report, nil
}

func (s *Service) repairKey(remote *HTTPRemote, key string, hasRemote bool, report *Report) error {
	var received []replication.Version
	var peerHash merkle.Hash
	if hasRemote {
		versions, err := remote.pull(key)
		if err != nil {
			return err
		}
		received = versions
		report.KeysPulled++
		peerHash, err = hashVersions(key, received)
		if err != nil {
			return remote.invalid(err)
		}
		// Detect internally inconsistent remote siblings before partially applying.
		entries := make([]store.Entry, len(received))
		for i, v := range received {
			entries[i] = store.Entry{Value: v.Value, VC: v.VC}
		}
		if _, err = store.Reconcile(entries); err != nil {
			if errors.Is(err, store.ErrClockCollision) {
				s.collision(remote, key, received, report)
				return nil
			}
			return remote.invalid(err)
		}
		changed, err := s.applier.ApplyVersions(key, received)
		report.VersionsApplied += changed
		if err != nil {
			if errors.Is(err, store.ErrClockCollision) {
				s.collision(remote, key, received, report)
				return nil
			}
			return err
		}
	}
	current, err := s.store.GetSiblings(key)
	if err != nil {
		return err
	}
	versions := make([]replication.Version, len(current))
	for i, v := range current {
		versions[i] = replication.Version{Value: v.Value, VC: v.VC}
	}
	if hasRemote {
		localHash, err := hashVersions(key, versions)
		if err != nil {
			return err
		}
		// Compare with the authoritative pull rather than a possibly stale listing.
		if localHash == peerHash {
			return nil
		}
	}
	pushed := false
	for _, version := range versions {
		if err := remote.push(key, version); err != nil {
			var status *HTTPStatusError
			if errors.As(err, &status) && status.StatusCode == http.StatusConflict {
				// A key that looked local-only may have appeared remotely since descent.
				if received == nil {
					received, _ = remote.pull(key)
				}
				s.collision(remote, key, received, report)
				return nil
			}
			return err
		}
		if !pushed {
			report.KeysPushed++
			pushed = true
		}
	}
	return nil
}

func hashVersions(key string, versions []replication.Version) (merkle.Hash, error) {
	input := make([]merkle.Version, len(versions))
	for i, v := range versions {
		input[i] = merkle.Version{Value: v.Value, Clock: v.VC}
	}
	return merkle.KeyHash(key, input)
}

func (s *Service) collision(remote *HTTPRemote, key string, received []replication.Version, report *Report) {
	report.Collisions++
	local, _ := s.store.GetSiblings(key)
	localClocks := make([]any, 0, len(local))
	remoteClocks := make([]any, 0, len(received))
	for _, v := range local {
		localClocks = append(localClocks, v.VC)
	}
	for _, v := range received {
		remoteClocks = append(remoteClocks, v.VC)
	}
	s.logger.Printf("anti-entropy peer=%s key=%q clock collision local_clocks=%v remote_clocks=%v", remote.peer, key, localClocks, remoteClocks)
}

func unreachable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var network net.Error
	if errors.As(err, &network) {
		return true
	}
	var status *HTTPStatusError
	return errors.As(err, &status) && (status.StatusCode == 502 || status.StatusCode == 503 || status.StatusCode == 504)
}
