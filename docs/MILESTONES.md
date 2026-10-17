# Milestone history and implementation notes

This is the historical project diary, preserved from the pre-release README.
Current usage and status are in [the project README](../README.md).
Relative file paths in this diary are repository-root paths.

## Milestone history

| Milestone | Completed behavior |
| --- | --- |
| 1 | Local memory store, durable WAL, recovery, HTTP, shutdown |
| 2 | Concurrent peer replication, internal writes, failure logging |
| 3 | W/R quorums, early write responses, timestamp-based version selection |
| 4 | Vector clocks, sibling retention, cross-replica pruning, HTTP 300 conflicts |
| 5 | Gossip heartbeat evidence, local failure detection, restart/identity handling |

Milestone 3's last-write-wins timestamp policy could silently discard
independent updates. The current implementation instead preserves concurrent
histories and surfaces them to the caller. Timestamp-only internal payloads
and WALs belong to the historical implementation, not the current protocol.

## Vector clocks and conflict detection

The standalone `internal/vectorclock` package provides `VectorClock`,
`Compare` (Equal, Before, After, Concurrent), and `Merge` (elementwise max).
Missing counters mean zero. Merge returns a separate map; neither function
modifies its inputs. Counters must be nonnegative.

The store now holds a slice of exported `Entry` values per key, each with
`Value`, `VC`, and `Ts`. `PutWithClock` applies the causal dominance rule;
`GetSiblings` returns all versions as independent copies, including copied
clock maps. For clocked entries, Ts records the local first-storage time
and is preserved on replay. It is never used to choose between clocked
versions. Stale writes and identical duplicate deliveries leave the state
and WAL unchanged. Equal clocks carrying different values are rejected as
inconsistent input rather than silently choosing a value.

Live clocked writes and WAL recovery share `resolveSiblings`. The live path
appends and syncs the WAL before publishing changed memory; replay applies
the same rule without appending. Caller-owned and returned maps cannot be
used to mutate the store's clocks. Callers must not concurrently mutate an
input map while a store method is reading it.

### Coordinator writes and replication

`main` passes `--id` to the handler. For an external PUT, `coordinateWrite`
reads that key's local siblings, joins their clocks, increments only its
own node ID, and calls `PutWithClock`. A handler mutex covers this complete
sequence and internal peer writes, preventing same-node requests from
allocating the same next clock. Production uses one handler per store.
The mutex is released before any network operation. Counter overflow is
rejected instead of wrapping. Node IDs must be unique and stable across
restarts; use the same WAL with the same ID. Reusing an ID after losing its
history can create equal-clock collisions and is not handled automatically.

Replication sends the same value and clock to all peers. Peers call
`PutWithClock` without incrementing or joining the incoming clock. The W
acknowledgement rule, independent two-second lifetime, and background result
logging continue to work as in Milestone 3. Ts now records each replica's
local first-storage time; it is not sent to peers or compared by handlers.

### Breaking peer protocol change

Internal PUT now requires:

```json
{"value":"Chennai","vc":{"n1":3,"n2":1}}
```

Internal GET returns a sibling array, including all unresolved versions:

```json
[{"value":"Mumbai","vc":{"n1":2}},{"value":"Pune","vc":{"n1":1,"n2":1}}]
```

An absent key returns `[]` with status 200. Timestamp-only PUT payloads,
null clocks, negative counters, empty node IDs, and malformed payloads
are rejected. An empty value and an empty clock object are valid. Values
are limited to 1 MiB and encoded clocks to 64 KiB; internal PUT allows
JSON escaping overhead. Peer GET responses are limited to 64 MiB total;
a larger sibling response counts as a failed peer answer. This is an
intentional protocol version change; upgrade all nodes together.

### Quorum reads and conflict responses

A quorum GET collects R valid replica answers, combines their sibling
lists, and calls the store's pure `Reconcile` helper. That helper shares
the live-write/replay dominance rule, removing duplicates and dominated
versions without modifying any replica. It never compares timestamps.

- No surviving versions: 404.
- One surviving version: 200 with its plain-text value.
- Several concurrent versions: 300 Multiple Choices with the JSON sibling array.
- Fewer than R valid answers: 503.
- Equal clocks carrying different values in collected answers: 502.

A 300 response exposes the surviving alternatives; the caller decides
whether to select a value or merge values at the application level. The
server does not use timestamps to guess a winner. Concurrent versions that
reach the store or the collected read quorum are detected and preserved.

No read repair or client-provided causal context is implemented. A new external
PUT automatically supersedes every version its coordinator currently
knows, even if the client has not seen those siblings. Versions known only
to another replica may remain concurrent. Logs report peer clocks and
quorum decisions without logging the stored values.

### Known limitation: the coordinator knows only local history

The external PUT body is only the new value. It does not carry a vector
clock read by the client. The coordinator joins its local siblings, not
all versions the client has observed or all versions elsewhere in the
cluster. This is an intentional scope limit, not a guarantee of causal
consistency for clients that move between coordinators.

**Resolving through a stale coordinator.** After the partition experiment:

```text
n1: Mumbai {n1:2}
n2: Pune   {n1:1,n2:1}
n3: both versions
```

A quorum GET through n1 returns both versions, but does not store Pune on
n1. A subsequent PUT of `Resolved-via-n1` through n1 creates `{n1:3}`.
That clock is still concurrent with Pune's `{n1:1,n2:1}`, so a quorum read
that sees both returns 300 again. A resolving PUT through n3, which knows
both siblings, creates `{n1:2,n2:1,n3:1}` and supersedes both. This does
not mean n3 is a special leader: any coordinator that has learned both
histories can create such a superseding write.

**Sequential client writes through a lagging replica.** With N=3, W=2,
R=2, a PUT of `v1` through n1 can return 200 after n1 and n2 store
`v1 {n1:1}`, while n3 is still missing it. The same client then PUTs `v2`
through n3 before n3 learns v1. Because n3 has no local history for the
key, it creates `v2 {n3:1}`. A later quorum read can return 300 with both
v1 and v2 even though the client submitted its writes sequentially. The
server cannot infer that client-observed order from a plain value body.

Milestone 6 anti-entropy delivers missing versions after successful repair,
allowing a later write on a repaired coordinator to cover those histories.
It does not automatically resolve existing concurrent siblings or guarantee
that a coordinator is caught up at the moment of every new write. An
optional client-context protocol could carry the clocks returned by a read
into the following write, preserving the dependencies the client observed.
That protocol is not implemented. Until then, callers must account for the
coordinator's knowledge when resolving a conflict.

### Legacy data boundary

The timestamp-only `Put` and `PutAt` store methods have been removed. All
writes now supply a vector clock to `PutWithClock`; no version selection
compares timestamps, including during startup recovery.

A complete PUT record with a missing or null `vc` causes startup to fail
with an explicit message directing the user to a fresh `--data` path.
Startup validates and replays complete records before truncating an
incomplete tail, so a rejected legacy WAL (including a mixed-format WAL)
remains byte-for-byte unchanged. The file descriptor is closed on failure.
A valid clocked WAL still recovers an incomplete tail as before. An
explicit empty clock object `{}` is supported and differs from missing
clock metadata.

Preserve existing Milestone 3 WALs and use new paths for the Milestone 4
exercises, for example `--data n1-m4.wal`. Automatic legacy migration is
not implemented: wall-clock timestamps cannot reconstruct causal history.
The Milestone 3 tag remains available for reading old data with its original
implementation. `Ts` remains informational in new records, and the
single-value `Get`/`GetWithMeta` helpers reject multiple siblings instead
of choosing a winner.

### Verification

`go test -race -count=1 ./...` covers clock comparison, sibling dominance,
WAL recovery, exact clock replication, old-protocol rejection, concurrent
same-node PUTs, counters across restarts, counter overflow, conflict
responses, explicit resolution by a later PUT, and the existing W/R quorum
failure and timeout behavior. `scripts/check-quorum-cluster.py` now reads
vector-clock metadata so the three-process pause/outage/stale-read checks
remain usable with the new protocol.

### Real three-node partition verification

Run from the repository root:

```bash
python3 scripts/check-vectorclock-cluster.py
```

This uses three actual server processes with N=3, W=2, R=2, temporary WALs,
and six directed loopback HTTP proxies. Each proxy represents one peer
link. Blocking n1-to-n2 and n2-to-n1 returns a simulated link failure (503)
without changing the host firewall. Both nodes can still reach n3, so both
independent writes can achieve W=2. This is an application-level link-failure
experiment, not a physical network disconnection.

The script first verifies ordinary sequential writes and converges `city`
to `Delhi` with clock `{n1:1}` on every node. It then blocks the two
coordinator links, writes `Mumbai` through n1 and `Pune` through n2, and
checks the exact local versions before restoring communication:

| Node | Versions during partition |
| --- | --- |
| n1 | Mumbai `{n1:2}` |
| n2 | Pune `{n1:1,n2:1}` |
| n3 | Both Mumbai and Pune |

n3 accepts both as concurrent siblings but does not forward internal PUTs.
The script explicitly checks that n2 still has Delhi before creating Pune;
thus n2 has not learned Mumbai's history through the third node.

Observed on 2026-10-08: both partitioned PUTs returned 200. After restoring
both blocked links, all 15 quorum GETs (five through each coordinator)
returned 300 with exactly those two versions and clocks. Every possible
R=2 read set in this state contains both histories: either n1 and n2
provide one each, or n3 provides both. This verifies the conflict response
without relying on a favorable response order.

Internal GETs afterward confirmed the same local state as before the
reads: reconnection and quorum reads did not repair missing siblings.
The separate sequential key returned 200 with its second value through
all three coordinators both before and after the partition (six checks).
The script cleans up its servers, proxies, and temporary data on exit.

Milestone 4 implementation and verification are complete, including the
real three-node partition experiment.

## Milestone 5 preparation (complete)

The starting baseline is the `milestone-4` tag (`cc8873c`). On 2026-10-09,
`go test -race -count=1 ./...`, `bash checkpoint.sh`, and both cluster scripts
passed before creating the `milestone-5` branch.

Startup validation now lives in `cmd/server/config.go`, with table-driven
tests in `config_test.go`. Invalid configuration exits before opening the
WAL or starting the server. Node IDs must match
`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`. Validation also checks numeric ports,
peer address syntax, duplicate peer addresses, local self-peers, quorum
bounds, and `W+R>N`. Self-peer detection covers localhost, loopback and
wildcard addresses at the listening port; it does not resolve DNS aliases.

Three duration flags are accepted and validated:

| Flag | Default | Rule |
| --- | --- | --- |
| `--gossip-interval` | `1s` | From `10ms` through `1m` |
| `--suspect-after` | `10s` | At least three gossip intervals |
| `--dead-after` | `20s` | Greater than suspect-after |

These flags configure the running gossip service. Gossip observes liveness;
it does not change quorum membership or replication behavior.

The directed proxies in `scripts/check-vectorclock-cluster.py` now forward
POST as well as GET and PUT. A blocked link returns 503 for all three.
`scripts/check-quorum-cluster.py` uses direct peer connections, not proxies.
Run `python3 scripts/test_gossip_proxy.py` to verify POST forwarding,
blocking without delivery, and delivery after a link is restored.

The standalone `internal/gossip` package now implements the evidence table
and wire message types. `NewTable` validates its arguments and inserts self
with heartbeat zero. `Merge` accepts strictly newer (generation, heartbeat)
evidence, skips invalid entries, caps membership at 256 including self, and
returns changes for the caller to log after unlocking. Self evidence is never
adopted; a higher incoming self generation reports an ID conflict.

`Beat` advances self, `Refresh` reports locally derived status transitions,
and `Digest` returns a sorted independent copy for exchange. Refresh reports
recovery exactly once as well: Merge updates observation time without erasing
the previous status used for transition detection. Every time-dependent table
operation uses an injected clock. With a normal `time.Now` clock, subtraction
preserves Go's monotonic elapsed-time behavior. No remote timestamps or remote
liveness verdicts are exchanged.

Generation will be chosen from wall-clock Unix nanoseconds at process startup;
a backwards wall-clock jump between restarts can produce a generation peers
reject as stale. Unknown entries relayed from stale tables initially appear
alive for one suspect-after window (a ghost period), then age toward dead
unless newer evidence arrives. Entries are never deleted.

Wire validation checks sender/member IDs, positive generations, nonnegative
heartbeats, duplicate IDs and the 256-member limit. The exchange handler enforces
the 64 KiB body limit. Integer decoding
rejects fractions and overflowing numbers.

`NoteDirect(id, addr)` records the identity returned by a successful validated
exchange at the address actually dialed, including test proxy addresses. The
last answer for an address wins, and each member's `via` holds its latest direct
address. This local bookkeeping does not refresh heartbeat observation times
and is never gossiped. Learning an ID second-hand does not match it to a seed.
`MarkSelfAlias` excludes an address discovered to point back to this process.

`SelectPeer` uses an injected random-index function (such as `rand.Intn`) to
choose one configured address uniformly, excluding self-aliases. Dead and
unreached peers remain eligible. With no eligible addresses it returns no peer.
The caller's random function executes outside the table lock.

`View(seeds)` returns sorted member snapshots with status, generation, heartbeat,
`last_change_ms_ago`, and `via`, plus `unreached_seeds` for addresses not yet
matched to an ID. It derives status using the current local time without
consuming the transitions reported by Refresh. Returned slices are independent
copies. Neither View nor seed selection removes dead members.

Fake-clock tests cover comparison rules, exact age boundaries, restart and
recovery, self protection, capacity and snapshot independence. A 50-goroutine
test exercises Merge, Beat, Refresh, Digest, View and seed bookkeeping/selection
together under the race detector. Deterministic selection tests cover every
eligible index, dead peers, self-aliases and an empty candidate list.

All eight preparation items are complete. Nothing in the data path consults
this table.

## Gossip membership and failure detection

`internal/gossip.Service` owns the HTTP handlers and exchange client.
`RegisterRoutes` installs Go 1.22 method patterns for
`POST /internal/gossip` and `GET /internal/members`. They are tested on local
HTTP servers and are now registered in the application server alongside the
existing key/value routes.

An exchange validates the complete body before merging. Oversized requests
return 413; malformed or invalid messages return 400. The sender must include
its own member entry, which supplies its generation for identity checks.
Successful responses contain the responder's digest after merging. The members
endpoint refreshes status, puts self first, and includes top-level `self` and
`generation`, member details and unreached seeds.

The dedicated exchange client uses a timeout of the gossip interval clamped to
200ms through 2s. It requires status 200 and a valid response no larger than
64 KiB before changing evidence or direct-address bookkeeping. Valid bodies
are consumed through EOF; oversized streams are closed rather than drained
without a bound. Redirects are not followed. Seed failures are logged initially
and then only on reachability changes, not on every failed attempt.

Same-ID responses with the same generation mark a self-alias; different
generations report an ID conflict on both sides without replacing self state.
Conflict logs are throttled to once per offending generation per 30 seconds.
This assumes process generations differ: equal timestamp-derived generations
for two processes with the same ID cannot be distinguished from an alias.

HTTP tests cover push-pull, validation and limits, status views, method rejection,
seed failure/recovery logging, both directions of duplicate-ID detection,
self-alias handling and cancellation/timeouts.

`main` captures the process generation once, creates the table and service after
the store, and starts gossip after launching the HTTP server goroutine. With
configured seeds, Start runs a round immediately and then on each interval:
Beat, Refresh, select one seed, launch an exchange in a tracked goroutine.
A slow peer cannot hold up future rounds. Without seeds no loop is started;
standalone key/value behavior is unchanged and membership endpoints still work.

Stop cancels the loop and its requests and waits for all outgoing workers. It
is idempotent, safe with concurrent lifecycle calls, and runs before HTTP
shutdown and store closure. A stopped Service is not restarted; a process
restart constructs a new instance. Status transitions are logged once per
detected change, and `OnChange` optionally receives each transition outside
table and service locks. Hooks must return promptly and enqueue repair work;
they must not synchronously call Stop on the notifying worker. The application registers a non-blocking anti-entropy recovery hook when
automatic repair is enabled.

Lifecycle tests cover continued heartbeats and aging during stalled exchanges,
prompt cancellation, repeated/concurrent Stop calls, parent cancellation and
standalone behavior.

The multi-node HTTP harness reserves all addresses before wiring services and
can replace a handler at the same address. It uses 50ms rounds, 400ms suspect
and 800ms dead thresholds, with polling deadlines rather than fixed startup
sleeps. Scenarios cover:

- Three-node convergence and increasing heartbeat evidence.
- A stopped node returning 503, with exactly one alive-to-suspect and one
  suspect-to-dead callback on each surviving observer.
- Restart at the same address after the old heartbeat exceeded 20, accepting
  a newer generation with a lower heartbeat and reporting recovery.
- Indirect liveness over n1-to-n2-to-n3, with no direct n1-to-n3 seed or `via`.
- Loss of that middle node, aging of the disconnected nodes, and recovery
  after the middle node restarts.
- Five seconds of idle healthy operation with zero suspect transitions.
- A self-alias contacted once and excluded from subsequent running rounds.

Run these with `go test -race -count=5 ./internal/gossip`.

### Protocol and local merge rules

Each round exchanges a full digest with one uniformly random configured peer,
including peers considered dead. The request and response have the same shape:

```json
{"from":"n1","members":[{"id":"n1","generation":1791000000000000000,"heartbeat":42}]}
```

Generation identifies one process lifetime; heartbeat increases each local
round. Compare generation first, then heartbeat within a generation. A higher
generation replaces older evidence even with a lower heartbeat. Equal or older
evidence never refreshes the observation time. An unknown valid ID is inserted
if the table has room (256 entries including self). Entries are never deleted.
Self is never overwritten; differing directly observed self generations are
reported as ID conflicts. Generation depends on wall-clock time at startup:
clock regression across restarts may make fresh evidence look stale.

Only evidence is shared, never another node's alive/dead verdict or observation
timestamp. Each observer measures elapsed local monotonic time since newer
evidence: age below suspect-after is alive, age below dead-after is suspect,
otherwise dead. Self is always alive. Unknown stale entries learned indirectly
have an initial ghost-alive window before they age out. These thresholds assume
small N; large clusters require analysis of dissemination delay and tuning.
Use suspect-after of at least eight round intervals when links may fail
partially; the shipped default uses ten (1s rounds, 10s suspect, 20s dead).
Random selection still probes unreachable peers, which can delay indirect
evidence. A wider margin reduces false suspicions but cannot eliminate them.
The validation floor remains three intervals, not a reliability recommendation.
A future extension could favor recently reachable peers while still probing
unreachable ones; this milestone retains uniform random selection.

Inspect a running node with `curl localhost:8081/internal/members`. The response
includes self and generation, self-first members with local status and age,
the latest direct address (`via`), and configured addresses not yet matched to
an ID (`unreached_seeds`). An empty `via` can mean the evidence was learned only
indirectly. Endpoints are unauthenticated and intended for trusted test peers.
Membership remains static: learning an unlisted ID does not discover its
address or add it to peer selection, replication or quorum accounting.

### Real-process verification

`scripts/check-gossip-cluster.py` builds a temporary binary and starts three
processes behind directed HTTP proxies, using 200ms rounds, 2s suspect and
4s dead thresholds. It polls membership every 100ms and checks:

- Initial convergence and kill-9 detection through suspect then dead within
  dead-after plus 600ms (4.6s total).
- Restart using the same ID and WAL, a higher generation and recovered data.
- SIGSTOP/SIGCONT detection and recovery with the same generation.
- Continued indirect liveness through n2 while n1/n3 links are blocked for
  longer than dead-after; isolating n1 then makes it report both peers dead.
- Recovery after healing, with continuous successful quorum PUT/GET probes
  through a coordinator that retains two reachable replicas.

The script resumes paused processes during cleanup and removes all temporary
servers, proxies and files. Gossip does not repair missed data or resolve
conflicts; no quorum or replication path consults the liveness table.

Milestone 5 implementation and verification are complete.

## Milestone 6 preparation (complete)

The clean starting baseline is `milestone-5` (`82b1d78`). The full race suite,
checkpoint script, all three real-process cluster scripts and proxy test
passed before creating `milestone-6` on 2026-10-10.

The first preparation step closes the invalid-UTF-8 boundary hole. Previously,
bytes `61 62 ff fe 63 64` could be accepted into memory but replayed from JSON
as `61 62 ef bf bd ef bf bd 63 64`. New invalid-text writes are rejected before
changing memory or the WAL. Existing data already coerced by JSON cannot have
its original bytes recovered by this validation change.

`internal/merkle` now provides canonical hashing and immutable trees independently
of the store. Hashes use SHA-256 and big-endian fixed-width length prefixes:

- A key's bucket is the first 12 bits of SHA-256(key), in 0..4095.
- Clocks omit zero counters and sort IDs, encoding uint32 count followed by
  uint16 ID byte length, ID bytes and uint64 counter per entry.
- Versions encode uint32 value byte length, value bytes and canonical clock.
  Siblings sort by clock bytes and then value bytes.
- Key state hashes 0x02, uint32 key byte length, key bytes, uint32 sibling
  count and the ordered versions. Timestamps are absent from the input type.
- An empty leaf is 32 zero bytes; other leaves hash 0x00 followed by key-state
  hashes in sorted key order. Internal nodes hash 0x01 and 16 child hashes.

Every tree has levels 0 through 3: 1 root, 16 children, 256 intermediate nodes
and 4096 leaf buckets. Shape never depends on which keys exist. A zero-value
Builder accepts key/hash pairs (adding an existing key replaces its hash);
Build makes an independent snapshot. Root, Node and Children return hashes by
value, and BucketKeys returns a sorted copy. Invalid indexes return errors.
`KeyDigest` names a listing entry because Go cannot use `KeyHash` for both
the hashing function and a type in the same package.

Tests lock three independently calculated golden key hashes and an empty-tree
root; cover zero-counter normalization, sibling order, length boundaries and
100,000-key bucket distribution; compare every node of two 10,000-key trees;
and verify a one-key update changes exactly one hash at each level. Concurrent
snapshot reads are checked under the race detector.

Store now exposes `Revision()` and `Range(fn)`. Revision starts at 1 after
startup replay and increments only after a durable write changes the sibling
set. Stale writes, identical duplicates, invalid input, clock collisions and
WAL failures do not advance it. This counter is local to the current Store
instance; it is not persisted and is not a version to compare across replicas.

Range visits every key once in unspecified order under the store lock, so a
write cannot interleave with the scan. Its callback must be fast, must not call
Store methods, and must not mutate or retain borrowed slices/maps. Callers must
copy data they want to retain. Revision and Range are separate calls; the snapshot
cache accounts for writes between them rather than assume one atomic call.

Tests cover revision changes and replay, range coverage, concurrent scans and
writes, and 20 deterministic randomized delivery sets applied to two stores in
different orders with duplicate retries. Final sibling sets and canonical
key hashes match, excluding informational timestamps. This convergence check
assumes equal clocks carry equal values; clock collisions remain invalid input.

`Handler.ApplyVersions` is now the shared remote-write path used by internal
PUT and exposed through `antientropy.Applier`. It holds the same write mutex
as the coordinator's read/join/increment/store sequence, validates each remote
version and applies the existing dominance rule. The returned count measures
versions that changed this key's state, not the number of removed siblings.
On error it returns partial progress; earlier writes remain durable and the
caller may continue with later versions in a subsequent call. Tests cover
collision recovery, stale/duplicate counts and 50 goroutines mixing repairs
with coordinator PUTs without losing clock increments or remote history.

`Table.PeerByAddr` and `Table.AddrOf`, also exposed by Service, resolve directly
observed peer identities and addresses. Status is derived at lookup time;
lookups do not consume Refresh transitions. Unknown members, indirectly learned
addresses and self aliases are not fabricated into repair targets. OnChange
hooks must return promptly and only enqueue work, never run repair inline.

`merkle.Compare` now walks only mismatching branches, requesting at most 256
node hashes or 64 bucket listings per batch. Its Remote interface uses
`Nodes(level, indexes) (NodeBatch, error)` and
`Buckets(indexes) (map[int][]KeyDigest, error)`. NodeBatch includes fanout and
levels as well as hashes: a bare hash slice cannot communicate a shape mismatch.
Hashes are fixed 32-byte values; their JSON form is exactly 64 hexadecimal
characters with strict decoding, so malformed hash lengths cannot be silently
padded or truncated by Go's array decoder.

After descent, bucket order is shuffled with an injectable index picker. The
engine validates listing indexes, key placement and duplicate keys, sorts keys
and recomputes each leaf. A listing that no longer matches the descended leaf
is skipped as changed during comparison. Local-only, remote-only and differing
keys are candidates, bounded by MaxKeys (default 1000, range 1..100000). Diff
reports candidates, divergent/skipped buckets and truncation; Stats reports
request/hash/bucket counts. No writes occur. A fake remote verifies identical
trees require one hash and a single changed key among 10,000 requires 49 hashes.
Malformed remote responses return errors rather than panics.

The worker uses these validated flags:

| Flag | Default | Validation |
| --- | --- | --- |
| `--ae-interval` | `30s` | `0` disables scheduling; otherwise `1s` through `1h` |
| `--ae-max-keys` | `1000` | `1` through `100000` |
| `--ae-initial-delay` | `5s` | Nonnegative; at most the interval when enabled |

When scheduling is disabled, initial delay is ignored, so `--ae-interval 0`
works without also overriding the default delay. The quorum, vector-clock and
gossip cluster scripts explicitly disable anti-entropy to preserve their
existing stale-replica and membership assertions while background repair is enabled in normal application runs.

All eight preparation items are complete.

## Milestone 6: Merkle-tree anti-entropy

The first implementation batch adds `antientropy.SnapshotCache`, `TreeHandler`
and `HTTPRemote`. The cache hashes key/version state inside Store.Range and
builds an immutable tree. A cache mutex coalesces simultaneous stable-revision
requests into one build. If revision changes across the scan/build, the result
is still a consistent scan snapshot but is not cached under an uncertain
revision; the next request can rebuild. This avoids retrying indefinitely
under continuous writes. Callbacks only hash/add entries, without logging,
networking or re-entering the store.

TreeHandler registers POST `/internal/ae/nodes` and `/internal/ae/buckets`.
Requests are capped at 64 KiB (413), with invalid JSON, missing fields,
invalid levels/indexes, duplicate indexes and excess indexes returning 400.
Node batches allow 1..256 indexes; bucket batches allow 1..64. Responses retain
request order and always include fanout/levels. Buckets contain sorted key/hash
pairs. Responses exceeding 8 MiB are rejected rather than truncated.

HTTPRemote implements the pure comparison engine's Remote interface with a
dedicated client, a 5s per-request timeout and an 8 MiB response cap. Valid
bodies are consumed through EOF; oversized streams are closed without an
unbounded drain. Redirects are refused. Non-200 responses become HTTPStatusError;
invalid shape, counts, hash lengths, bucket order, duplicate/unsorted keys or
wrong key placement become InvalidResponseError. Caller cancellation interrupts
requests. Leaf-versus-listing verification remains the comparison engine's job.

Tests cover real HTTP comparison, bounded request/response validation, typed
errors, cancellation, 50 simultaneous cache callers, duplicate-write cache
reuse and a write overlapping a scan.

`antientropy.Service.SyncWith(ctx, peer)` now executes a bounded repair cycle.
It compares a cached local tree, pulls peer-present candidate keys using the
existing internal GET, and applies versions through Applier. If the resulting
local state differs from the freshly pulled peer state, it pushes every local
sibling through internal PUT. Local-only candidates are pushed directly.
The comparison result retains validated peer hashes to distinguish local-only
keys without additional listing requests. A pull's actual versions supersede
the earlier tree hint when deciding whether a push is needed. Nothing is deleted.

Each candidate consumes one key-budget slot even if it fails. Clock collisions
are counted and logged once per key with local and remote clocks, without
logging values or aborting other keys. Other key failures are counted and the
cycle continues; network unreachability, cancellation and unavailable-peer
statuses (502/503/504) stop that peer's cycle. A cycle uses its caller's context
with a 60s maximum; requests use the dedicated 5s client. Existing sibling
payload validation and the 64 MiB internal-read limit apply to key transfers;
tree responses retain their separate 8 MiB limit.

Report includes peer, start time, elapsed milliseconds, roots_equal,
hashes_fetched, requests, buckets_diffed/skipped, keys_pulled/pushed,
versions_applied, collisions, key_errors, truncated, roots_equal_after and error.
Keys pulled count successful GETs; keys pushed count keys with at least one
successful version PUT. Versions applied counts actual local state changes.
Hashes fetched counts comparison hashes (at most 49 for a single-key branch);
requests counts all HTTP attempts, including the final verification root.
After any successful apply/push, a fresh revision-checked local tree and one
remote root request verify convergence. A differing final root is reported,
not treated as failure: writes may continue during a cycle. Equal-root cycles
make exactly one request, fetch one hash and produce no cycle log line.

Real-HTTP cycle tests cover missed writes, no-op WAL sizes, efficient descent,
both-direction transfer, sibling preservation followed by conflict resolution,
three budgeted cycles for 300 keys, collision/error isolation, lying/changing
tree responses and caller cancellation. Worker triggers, operational endpoints
and application wiring are implemented. The real-process cluster script also
checks restart catch-up, conflict convergence and disabled scheduling.


### Scheduling and operational endpoints

One worker serializes repairs from a queue with 16 waiting slots. Requests for
an already queued or active peer share that cycle. The periodic trigger first
fires after `--ae-initial-delay`, then waits the interval with ±20% jitter. It
chooses a configured peer at random, skipping known self aliases and peers that
gossip currently reports suspect or dead; unknown peers remain eligible.
A dead-to-alive gossip transition enqueues a repair without waiting for it.

`POST /internal/ae/run` accepts `{"peer":"localhost:8082"}` to repair an explicit
configured address regardless of its gossip status. Addresses must match a
configured `--peers` entry exactly; other addresses return HTTP 400. An empty
body or `{}` runs against all currently eligible configured peers sequentially. The response is an array of
cycle reports, including errors for failed cycles. A full queue returns 429;
a stopped worker returns 503. A disconnected caller stops waiting, while the
worker retains ownership of the repair. If a multi-peer request encounters a
full queue, earlier cycles in that request may already have completed.

`GET /internal/ae/status` returns `totals` and `last_reports` keyed by peer.
Totals include completed cycles, keys pulled/pushed, versions applied,
collisions and failed cycles (`errors`). Returned status maps are copies.
Equal-root cycles update counters without producing a log line.

`--ae-interval 0` disables both automatic triggers. Tree endpoints and manual
runs remain available. With no seeds there is no timer and an untargeted manual
run returns an empty list. Shutdown cancels anti-entropy and waits for its worker,
then stops gossip, shuts down HTTP, and finally closes the store. Quorum reads
and writes do not consult gossip or wait for anti-entropy.


### Verification and remaining limitations

Run `python3 scripts/check-antientropy-cluster.py` to launch three real binaries
with temporary WALs, dynamically allocated ports and directed HTTP proxies.
It uses 2s repair rounds, a 1s initial delay, and gossip timings of 200ms/2s/4s.
The script checks 20-key convergence and equal-root idle reports; kills n3,
writes five new keys and overwrites two existing keys, then restarts n3 on its
existing WAL and verifies all seven changes through local internal GETs.

The conflict scenario blocks n1–n2 traffic while both can write through n3.
Tree exchanges are briefly paused during conflict creation so repair cannot
relay the first version before the second write. After healing, local internal
GETs must show both siblings on every node without any quorum read driving
repair. A resolving PUT through n1 then leaves one version everywhere. A
separate run with `--ae-interval 0` confirms downtime gaps remain unrepaired.
The older quorum, vector-clock and gossip scripts retain repair-disabled
settings so their original assertions remain meaningful.

- No deletes or tombstones, read repair, hinted handoff, or authentication.
- A changed revision rebuilds the snapshot by scanning every key. Hashing and
  sorting costs also depend on key/value sizes and sibling counts; this is not
  an incremental tree updated at write time.
- The shape is fixed at 4096 leaf buckets. Transfer happens per key/version,
  not as byte-level patches. Large listings are bounded by the response cap.
- A comparison is a hint across snapshots taken at different moments. Changed
  bucket listings are skipped; later cycles retry. Concurrent writes can leave
  final roots unequal without indicating an unsafe repair.
- The key budget bounds candidates touched per cycle, not the number of bytes
  transferred. Random bucket order avoids always favoring the same branches.
- Repair narrows the stale-coordinator conflict-resolution limitation: once a
  coordinator has learned all siblings, its next write can supersede them.
  It does not guarantee catch-up before every write or provide client context.
- A wiped node must not reuse its old ID before catching up. Reusing an ID with
  lost causal history can create equal-clock/different-value collisions; repair
  reports those collisions rather than choosing a value silently.

Verification passed: `go test -race -count=5 ./internal/antientropy`,
`go test -race -count=3 ./...`, a single-threaded anti-entropy race run,
`go vet ./...`, `checkpoint.sh`, and all four real-process cluster scripts.
Milestone 6 closeout verification on 2026-10-12 (macOS arm64) passed:

- Five race-enabled runs of `./internal/antientropy`.
- One race-enabled run with `GOMAXPROCS=1`.
- Three consecutive runs of `scripts/check-antientropy-cluster.py`.
- `go vet ./...`, anti-entropy formatting and diff whitespace checks.

Coverage includes five-key catch-up, unchanged WALs on a one-hash no-op,
2,000 shared keys plus one divergent key with at most 49 comparison hashes,
concurrent sibling preservation and later resolution, bidirectional repair,
three 100-key cycles for a 300-key gap, collision isolation, hostile peers,
a recovery hook completing a cycle within 500ms, and cancellation of an
in-flight cycle with repeated Stop calls within one second.

The `milestone-6` tag records this implemented and tested checkpoint.
Independent review of the worker, cycles and endpoints is complete. Manual
repair requests now reject addresses outside configured `--peers` with HTTP 400.
