# Limitations


- No automatic conflict resolution or client-supplied causal context.
  Sequential writes through different coordinators can appear concurrent
  if the second coordinator has not learned the first write.
- No read repair or hinted handoff. Background anti-entropy repairs missed
  versions; convergence still depends on reachable peers and completed cycles.
- Static peer addresses: gossip learns IDs but does not discover new dialable
  addresses or change quorum membership. Liveness gates automatic repair selection but never quorum operations.
- Gossip thresholds assume a small cluster and sufficient network/scheduling
  margin; packet delays and partitions can cause false suspicions. There is no
  synchronized or authoritative cluster-wide alive/dead verdict.
- No leader election, consensus, or claim of linearizable operations.
- No authentication, authorization, or TLS; peer endpoints are not protected.
- No WAL compaction/rotation; recovery reads the WAL into memory.
- No general repair of corrupted complete WAL records.
- No request-ID deduplication, durable retry queue, or guaranteed background
  replication drain on shutdown.
- No automatic migration of timestamp-only Milestone 3 WALs.
- Sibling counts are not automatically capped or resolved; peer read bodies
  above 64 MiB are rejected rather than partially interpreted.
- Node IDs must be unique and retain their history across restarts. Reusing
  an ID after losing its WAL can cause clock collisions.

This is a learning implementation, not a production database. Background
anti-entropy can deliver missing histories, but cannot invent client causal
context or automatically choose among concurrent application values.

- Tree snapshots scan all keys after a changed revision and have a fixed 4096
  buckets. Values transfer per key/version, not as byte-level differences.
- No deletes or tombstones. Repair does not remove keys.
- Repair narrows stale-coordinator resolution failures after catch-up, but does
  not make a coordinator current before every new write.
- Failure detection assumes sufficient timing margin; generations use wall time
  and can regress if a machine's clock moves backward across process restarts.
- Chaos testing uses three loopback nodes and one sequential writer per key.
  SIGKILL is not a disk power-loss simulation. Seeds replay schedules, not
  operating-system interleavings.
