# Distributed Key-Value Store in Go

An educational distributed key-value store with durable per-node write-ahead
logs, concurrent replication, configurable read/write quorums, and vector
clocks for detecting conflicting versions, and background gossip failure detection.

**Project status:** Milestones 1–5 are implemented and verified. Concurrent
versions are preserved as siblings and returned with HTTP 300 Multiple Choices.
Gossip tracks node liveness independently of quorum operations. Background
anti-entropy and automatic replica repair are not implemented yet.

All nodes in a cluster must use the current vector-clock protocol. Do not
mix Milestone 3 and Milestone 4 binaries or reuse timestamp-only WAL files
with the current binary. Existing old WALs are rejected without alteration.

## Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Project structure](#project-structure)
- [Build and run](#build-and-run)
- [HTTP API](#http-api)
- [Write-Ahead Log](#write-ahead-log)
- [Concurrency and failures](#concurrency-and-failures)
- [Testing and verification scripts](#testing-and-verification-scripts)
- [Limitations](#limitations)
- [Milestone history](#milestone-history)
- [Vector clocks and conflict detection](#vector-clocks-and-conflict-detection)
- [Gossip membership and failure detection](#gossip-membership-and-failure-detection)

## Overview

Each node holds a map from a string key to a list of sibling versions. Each
version contains a string value, a vector clock, and a local informational
storage timestamp. A local WAL allows versions to survive a restart.

Any node can coordinate a client request. Writes persist locally before
being sent to every configured peer. The client receives success after W
acknowledgements, including the local write. Reads collect R valid replica
answers, combine their sibling lists, and remove duplicates and causally
dominated versions. Concurrent versions are returned to the caller rather
than silently resolved by wall-clock time.

The standard three-node configuration is N=3, W=2, R=2. Startup requires
1 <= W,R <= N and W+R>N, where N=len(peers)+1. The overlap condition applies
to the same fixed set of distinct replicas. It does not provide consensus,
linearizability, or automatic conflict resolution. Configure unique stable
node IDs and list each actual peer once, consistently across the cluster.

## Features

- External HTTP PUT and quorum GET endpoints.
- Concurrent all-peer replication with an early response after W acks.
- Vector-clock comparison, joining, and concurrent sibling retention.
- HTTP 300 responses exposing unresolved versions and their clocks.
- A shared dominance rule for live writes, recovery, and read reconciliation.
- JSON-lines WAL, Sync before publishing accepted local writes, and torn-tail recovery.
- Store health checks after write or sync failures.
- Locks protecting the store and coordinator clock creation.
- Signal-driven HTTP shutdown and independent two-second replication attempts.
- Background push-pull gossip, restart detection and a local membership endpoint.
- Unit tests, race-detector tests, and real-process cluster verification scripts.

## Architecture

```text
Client PUT -> coordinator handler
              read local siblings -> join clocks -> increment own counter
                        |
                        v
                  PutWithClock
              compare -> WAL + Sync -> memory
                        |
                        v
           parallel internal PUTs to all peers
              each peer applies the same rule
                        |
                        v
          respond after W total acknowledgements

Client GET -> local siblings + parallel internal GETs
              collect R valid replica answers
                        |
                        v
           combine versions and prune by vector clock
                        |
               404 / 200 single value / 300 siblings
```

A read computes its response without updating the local or peer stores.
An internal PUT never forwards the write again and never increments its
clock. W=1 can respond immediately after the local write, while peer
attempts continue. R=1 reads only local state.

## Requirements

- Go compatible with the version declared in `go.mod`.
- `curl` for examples and quorum timing checks.
- Bash and POSIX signals for `checkpoint.sh` and cluster scripts.
- Python 3 for the two real-cluster verification scripts; no third-party
  Python packages are required.

## Project structure

```text
cmd/server/main.go              flags, startup validation, routing, shutdown
internal/api/handlers.go        coordinator writes, quorum reads, peer handlers
internal/replication/client.go  HTTP peer protocol and concurrent fan-out
internal/store/store.go        sibling state, dominance, WAL and recovery
internal/vectorclock/           Compare, Merge, and isolated clock tests
internal/api/*_test.go          HTTP, quorum, conflict, and concurrency tests
internal/replication/*_test.go   peer writes and replication-lifetime tests
internal/store/*_test.go         persistence, siblings, recovery, legacy rejection
scripts/check-quorum-cluster.py pause/outage/stale-read checks
scripts/check-vectorclock-cluster.py  directed partition/conflict checks
checkpoint.sh                  standalone persistence and shutdown checks
run-cluster.sh                 interactive three-node launcher
```

## Build and run

Run from the repository root:

```bash
go build -o server ./cmd/server
```

### Standalone node

Without peers, N=1, so override the default quorum sizes:

```bash
./server --id n1 --port 8080 --data n1-m4.wal --w 1 --r 1
```

### Three-node cluster

Run each command in a separate terminal, using fresh Milestone 4 WAL paths:

```bash
./server --id n1 --port 8081 --data n1-m4.wal --peers localhost:8082,localhost:8083 --w 2 --r 2
```

```bash
./server --id n2 --port 8082 --data n2-m4.wal --peers localhost:8081,localhost:8083 --w 2 --r 2
```

```bash
./server --id n3 --port 8083 --data n3-m4.wal --peers localhost:8081,localhost:8082 --w 2 --r 2
```

Each node must own a different WAL file. On restart, keep the same node ID
and WAL path. Press Ctrl+C for HTTP shutdown. The existing `run-cluster.sh`
launcher also starts three nodes, but uses `n1.log`, `n2.log`, and `n3.log`:
if those contain Milestone 3 data, preserve them and use the explicit fresh
paths above instead.

Two nodes with no configured peers and W=R=1 are independent stores. Nodes
replicate only to the peers explicitly listed. Gossip can learn node IDs
indirectly but does not discover or dial new addresses outside the seed list.

## HTTP API

The following examples target the three-node cluster above.

```bash
curl -i -X PUT http://localhost:8081/kv/name --data-binary 'Parimal'
curl -i http://localhost:8082/kv/name
```

External PUT accepts a raw string, limited to 1 MiB. It returns 200 with
`OK` when W acks are reached; insufficient acknowledgements return 503 with
`replicated` and `failed` lists. Those lists describe results collected so
far and may omit attempts still in flight. A 503 does not roll back the
local write or successful replicas.

External GET returns:

| Situation | Status and body |
| --- | --- |
| One surviving version | 200, plain value |
| Multiple concurrent versions | 300, JSON array of `{value, vc}` |
| All R valid answers report absence | 404 |
| Fewer than R valid answers | 503 |
| Local store unhealthy | 500 |
| Equal clocks with different values in read answers | 502 |

For example, a conflict can return:

```json
[{"value":"Mumbai","vc":{"n1":2}},{"value":"Pune","vc":{"n1":1,"n2":1}}]
```

The caller chooses or combines application values. Sending a new PUT to a
coordinator that knows both histories supersedes both. A coordinator that
knows only one may create another concurrent version; see the concrete
examples under [known limitations](#known-limitation-the-coordinator-knows-only-local-history).

Peer-facing endpoints use the same key path under `/internal/kv/{key}`.
Internal PUT accepts `{value, vc}`; internal GET returns the full sibling
array, or `[]` for an absent key. Invalid payloads return 400; oversized
bodies/values return 413. Internal equal-clock/different-value collisions
return 409. Internal GET never performs another fan-out.

## Write-Ahead Log

Accepted state-changing writes append one JSON record followed by a newline:

```json
{"op":"PUT","key":"name","value":"Parimal","ts":123456789,"vc":{"n1":1}}
```

The key and clock are persisted along with the value. `ts` records this
node's first-storage time for the version. It is informational only, is
not replicated, and never determines which version wins. Duplicate and
stale deliveries change neither memory nor the WAL.

The store computes the proposed siblings, appends and syncs the incoming
record, and only then publishes the new in-memory list. Recovery applies
the same dominance rule without appending records again. Historical WAL
records can remain even when their versions are no longer current siblings.

Startup validates/replays complete records before truncating an incomplete
final fragment. A complete PUT without a vector clock fails startup and
leaves the old file unchanged. Malformed complete JSON records are skipped;
this is not general corruption repair. Reading the WAL into memory and
unbounded WAL growth are current limitations.

## Concurrency and failures

The store mutex protects in-memory data and WAL writes. A separate handler
mutex covers the entire local read/join/increment/store operation and
incoming internal writes. It is released before network calls. A third,
request-local mutex protects the write-quorum collector's result lists.

Write and read fan-outs use bounded independent contexts. Buffered result
channels let background collectors finish and log attempts after a quorum
response. Background replication is not a durable queue: process exit can
stop attempts, and shutdown does not wait for every outstanding collector.

WAL write, short-write, or sync errors mark the store unhealthy. A restart
is required before serving from recovered state. A failed sync is ambiguous:
the record may or may not survive. A network/quorum failure can also leave
a partially persisted write visible to later reads. Identical replication
deliveries are deduplicated by clock and value; retrying an external PUT
creates a new clock and is not request-ID deduplication.

## Testing and verification scripts

All test files and verification scripts live in the repository.

```bash
go test -race -count=1 ./...
go vet ./...
```

The suite covers causal ordering, independent map copies, dominance and
sibling collapse, duplicate/stale writes, WAL recovery and torn tails,
legacy-file preservation, wire validation, 200/300 responses, quorum
failure/deadline behavior, same-node concurrent writes, and restart counters.
Closed-WAL tests exercise write failure; there is no full power-loss or
hardware-failure simulation.

Run real-process checks:

```bash
bash checkpoint.sh
python3 scripts/check-quorum-cluster.py
python3 scripts/check-vectorclock-cluster.py
python3 scripts/check-gossip-cluster.py
```

`checkpoint.sh` runs two intentionally independent W=R=1 nodes on ports
5011 and 5012 to test persistence and shutdown. The Python scripts choose
loopback ports dynamically, build temporary binaries, use temporary WALs,
and clean up their own processes and files. The quorum script uses curl to
measure healthy, paused-peer, and dead-peer writes and checks stale reads.
The vector-clock script blocks the two coordinator links with local HTTP
proxies and verifies both concurrent versions survive quorum reads.

## Limitations

- No automatic conflict resolution or client-supplied causal context.
  Sequential writes through different coordinators can appear concurrent
  if the second coordinator has not learned the first write.
- No read repair, anti-entropy, or automatic catch-up after a missed write.
- Static peer addresses: gossip learns IDs but does not discover new dialable
  addresses or change quorum membership. Liveness is observational only.
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

This is a learning implementation, not a production database. Planned
anti-entropy can deliver missing histories, but cannot invent client causal
context or automatically choose among concurrent application values.

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

Planned Milestone 6 anti-entropy will eventually deliver missing versions,
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
they must not synchronously call Stop on the notifying worker. No hook is
registered by the application yet.

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
