# Design and trade-offs

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

## Repair safety and scheduling

The Merkle tree finds candidate differences; actual key exchange remains
authoritative. Local and remote snapshots may represent different instants.
Repair uses the existing vector-clock dominance rule, so retries and reordered
versions preserve the maximal sibling set (assuming equal clocks carry equal
values). Clock collisions are reported and skipped, never guessed away.

A revision counter changes only when the store changes. A snapshot cache holds
one immutable tree, coalesces rebuilds with a mutex, and avoids caching under an
uncertain revision if a write overlaps the scan. Rebuilds scan the entire store;
this deliberately favors simplicity over incremental hashing.

One worker owns queued cycles. Timer selection skips gossip-suspect/dead peers,
but keeps unknown addresses. A dead-to-alive hook only enqueues work. Manual
runs can target any configured peer irrespective of gossip status. Pending
requests for the same peer share one cycle, and the queue has 16 waiting slots.
The automatic timer is jittered by ±20%; zero interval disables timer and hook.

Cycles have a 60s caller-derived deadline and 5s request deadlines. A key budget
bounds work and shuffled bucket order avoids fixed branch preference. Per-key
errors do not discard unrelated repairs. An equal-root cycle costs one hash
request and leaves both WALs unchanged. No read repair is performed.

Shutdown cancels and waits for anti-entropy, then gossip, then HTTP, then closes
the store. This prevents a repair from writing after the store closes.
