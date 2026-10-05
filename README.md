# Persistent Key-Value Store in Go

A simple single-node key-value store built in Go. It exposes an HTTP API
for storing and retrieving string values, keeps data in memory for fast
access, and uses a Write-Ahead Log (WAL) to recover data after a
restart.

> **Project status:** Milestone 1 (single-node persistence) is
> implemented. The project is a learning implementation and is not
> intended to be a production database.

## Contents

-   [Overview](#overview)
-   [Features](#features)
-   [Architecture](#architecture)
-   [Requirements](#requirements)
-   [Project structure](#project-structure)
-   [Build and run](#build-and-run)
-   [HTTP API](#http-api)
-   [Write-Ahead Log](#write-ahead-log)
-   [Concurrency and failures](#concurrency-and-failures)
-   [Testing](#testing)
-   [Automated checkpoints](#automated-checkpoints)
-   [Limitations](#limitations)
-   [Milestone 2 direction](#milestone-2-direction)

## Overview

The store maps string keys to string values. Data is held in an
in-memory Go map and persisted to a local WAL file. When the server
starts, it replays valid WAL records to reconstruct the map.

The current implementation is a **single-node** store. Running two
instances creates two independent stores when each instance uses a
different port and WAL file. The instances do not replicate data to each
other.

## Features

-   HTTP endpoints to put and get key-value pairs.
-   In-memory map for reads.
-   JSON-encoded, newline-delimited WAL records.
-   WAL replay during startup.
-   `Sync()` before acknowledging a successful write.
-   Mutex-based synchronization for concurrent access.
-   Detection and truncation of an incomplete final WAL record.
-   Unhealthy-state handling after WAL write or sync failures.
-   Graceful shutdown on `SIGINT` and `SIGTERM`.
-   Configurable listening port and WAL path.
-   Unit tests, concurrency tests, and race-detector checks.

## Architecture

``` text
                   HTTP request
                        |
                        v
                 Go HTTP server
                        |
                        v
                 Store methods
                        |
               +--------+--------+
               |                 |
               v                 v
          In-memory map      WAL file
            (data)        (append records)
               ^                 |
               |                 v
               +---------- Sync()
```

A successful `PUT` follows this order:

1.  Acquire the store mutex.
2.  Reject the operation if the store is marked unhealthy.
3.  Encode the operation as a WAL record.
4.  Append the record to the WAL.
5.  Call `Sync()` on the WAL file.
6.  Update the in-memory map.
7.  Return success.

The ordering is important: the in-memory state is changed only after the
WAL write and sync report success.

## Requirements

-   Go installed and available on `PATH`.
-   `curl` for the HTTP examples and checkpoint script.
-   A Unix-like shell (such as macOS Terminal, Linux shell, or the VS
    Code integrated terminal) to run `checkpoint.sh`.

Check your Go installation:

``` bash
go version
```

## Project structure

The exact structure may vary as the project evolves. A typical layout
is:

``` text
kvstore/
├── main.go
├── store_test.go
├── checkpoint.sh
├── go.mod
└── README.md
```

WAL files are created at the path passed through the `-data` flag. The
automated checkpoint script uses a temporary directory for its WAL and
output log files and removes that directory when it exits.

## Build and run

Run commands from the project directory---the directory containing
`go.mod`.

### Run with default settings

``` bash
go run .
```

If your `main.go` is the only entry point, `go run main.go` also works.

### Configure port and WAL file

``` bash
go run . -port 5001 -data store-a.log
```

-   `-port` selects the HTTP port.
-   `-data` selects the WAL file path.

The WAL file is created if it does not already exist. Use a stable path
if you want the data to be available after restarting the server.

### Run two independent instances

Open two terminals in the project directory.

Terminal 1:

``` bash
go run . -port 5001 -data store-a.log
```

Terminal 2:

``` bash
go run . -port 5002 -data store-b.log
```

Each process has its own memory and WAL file. A write to one instance
will not automatically appear in the other.

**Important:** Do not run two instances against the same WAL file. This
implementation is not designed for multiple processes to concurrently
own one WAL.

### Stop the server

Press `Ctrl+C` in the terminal running the server. The application
handles `os.Interrupt` and `SIGTERM` to initiate graceful shutdown.

## HTTP API

The examples below assume the handlers use the `/kv/{key}` route and a
successful `PUT` returns HTTP `200`, as implemented in the project.

### Put a value

``` bash
curl -i -X PUT http://localhost:5001/kv/name \
  -H "Content-Type: text/plain" \
  -d "Parimal"
```

A successful request returns an HTTP success status and the handler's
success response.

### Get a value

``` bash
curl -i http://localhost:5001/kv/name
```

The response body contains the stored value.

### Missing key

``` bash
curl -i http://localhost:5001/kv/unknown
```

A key that is not present returns `404 Not Found`.

### Example using two servers

Write to Server A:

``` bash
curl -i -X PUT http://localhost:5001/kv/name \
  -H "Content-Type: text/plain" \
  -d "Parimal"
```

Read from Server A:

``` bash
curl http://localhost:5001/kv/name
```

Read from Server B:

``` bash
curl -i http://localhost:5002/kv/name
```

If the key has not been written to Server B, it should return
`404 Not Found`. This illustrates that the instances are independent,
not replicated.

## Write-Ahead Log

The WAL stores each mutation as a JSON record followed by a newline. For
example:

``` json
{"op":"PUT","key":"name","value":"Parimal"}
```

The newline acts as a record delimiter. During startup, the store scans
the WAL and replays supported records into the in-memory map.

### Why write the WAL first?

If the process stops after a successful WAL sync but before updating the
in-memory map, the next startup can replay the record and restore the
value. The WAL is therefore the persistence source used to rebuild
in-memory state.

### Startup recovery

At startup, the implementation:

1.  Opens the configured WAL file.
2.  Removes an incomplete, unterminated tail fragment, if present.
3.  Reads the WAL records.
4.  Decodes supported records and replays `PUT` operations into the map.
5.  Starts serving requests after recovery completes.

For repeated writes to the same key, replaying records in order leaves
the latest value in the map.

### Incomplete tail recovery

A process may stop while a record is being appended, leaving a final
fragment without a newline. The current `recoverWALTail` helper
truncates bytes after the last newline. This handles an incomplete final
record, but it is not a general-purpose corruption repair mechanism.

## Concurrency and failures

### Mutex

The store uses a `sync.Mutex` to serialize access to shared state and
the WAL. This prevents concurrent goroutines from modifying the map or
appending WAL records at the same time through the store methods.

### Sync failures and ambiguous outcomes

A successful `Write()` does not by itself prove that data has reached
persistent storage. `Sync()` asks the operating system to flush file
changes to stable storage, but it can return an error.

If `Write()` succeeds and `Sync()` fails, the record might still be
present after a restart---or it might not be. The client receives an
error, but the final outcome can be uncertain. This is commonly called
an **ambiguous commit**.

The current implementation responds conservatively:

-   Marks the store unhealthy after a WAL write, short-write, or sync
    error.
-   Rejects later `Put` and `Get` calls while unhealthy.
-   Requires a restart so the store can reconstruct state from the WAL.

An error from `Sync()` should therefore not be interpreted as proof that
the write was absent. Repeating a `PUT` of the same key and value is
effectively idempotent for this simple API, but more complex operations
may need unique request IDs or deduplication.

### HTTP errors

The handlers report errors to the client using HTTP error statuses.
Check the handler implementation for the precise response body and
status mapping in your current revision.

## Testing

Run the unit tests:

``` bash
go test ./...
```

Run tests with the Go race detector:

``` bash
go test -race ./...
```

The current test suite covers:

-   Basic put/get behavior.
-   Recovery from the WAL after creating a new store.
-   Concurrent access.
-   Recovery from an incomplete WAL tail.
-   Rejection of reads and writes when the store is marked unhealthy.

The unhealthy-state test sets the failure flag directly. It verifies the
store's fail-closed behavior, but does not simulate an actual
operating-system disk write or sync failure.

## Automated checkpoints

The `checkpoint.sh` script performs practical integration checks using
two temporary server instances and separate temporary WAL files.

Run it from the project directory:

``` bash
chmod +x checkpoint.sh
./checkpoint.sh
```

The script builds the server and checks that:

1.  Two servers can start on separate ports.
2.  Each server stores independent data.
3.  A server can restart and recover its data from its WAL.
4.  A server stops responding after receiving `SIGTERM`.
5.  The other server remains operational independently.

The script uses ports `5011` and `5012` and a temporary directory. It
cleans up the temporary test files when it exits. Ensure those ports are
available before running it.

These integration checks complement, rather than replace, the unit tests
and race-detector run. They do not test replication, real disk failures,
power loss, or every possible shutdown race.

## Limitations

This is an educational single-node implementation. Current limitations
include:

-   No replication between instances.
-   No leader election, consensus protocol, or distributed coordination.
-   No authentication, authorization, or TLS.
-   No compaction or WAL segment rotation; the WAL grows as writes
    accumulate.
-   The simple tail-recovery approach reads the WAL into memory, which
    is not suitable for arbitrarily large files.
-   It truncates an incomplete final fragment but does not robustly
    repair malformed complete records or corruption in the middle of the
    WAL.
-   No injected disk-failure testing.
-   No request-ID deduplication for operations with non-idempotent
    effects.
-   No production-grade backup, metrics, or operational tooling.

## Milestone 2 direction

Milestone 2 can extend this foundation into a distributed KV store.
Topics to design and implement incrementally include:

-   Node-to-node communication.
-   Replication of writes and data recovery across nodes.
-   Handling timeouts, unreachable nodes, and partial failures.
-   Choosing a coordination model, such as a leader-based design.
-   Defining consistency and acknowledgement guarantees.
-   Preventing duplicate application of retried requests.

The current two-server checkpoint only proves that two independent
processes can run at once. It does **not** establish distributed storage
or replication.

## Replication Semantics

### Milestone 3 preparation: quorum configuration

The server accepts `--w` and `--r` flags, both defaulting to `2`.
`N` is the number of configured peers plus the local node. Startup rejects
configurations unless `1 <= W <= N`, `1 <= R <= N`, and `W + R > N`.
Validation happens before opening the WAL or starting the HTTP listener.

For the three-node cluster, the defaults give `N=3, W=2, R=2`.
For a standalone node without peers, explicitly pass `--w 1 --r 1`:

```bash
go run ./cmd/server --w 1 --r 1
```

Both values are now passed into the API handler. Writes use W to decide
when to acknowledge success; reads collect R valid answers, including
the local lookup, before selecting a value. The overlap condition guarantees that
a read quorum and a successful write quorum share at least one replica
when drawn from the same replica set; it does not by itself guarantee
strong consistency or resolve concurrent writes.

Writes are applied to the receiving node locally before replication is
attempted. The local write is appended to the WAL and synced before the
node forwards the value to its configured peers.

Milestone 2 required every configured peer to acknowledge. The first
Milestone 3 increment now counts the durable local write as one ack,
attempts every configured peer, and returns 200 as soon as W total acks
are collected. With N=3 and W=2, one successful peer is enough even if the
other peer is slow or unavailable. W=1 returns after local persistence
while still attempting all peers.

A background collector drains and logs every result, including results
arriving after the HTTP response. A mutex protects its success/failure
lists, and sync.Once closes a quorum notification channel exactly once.
The handler waits for that notification or the independent two-second
replication context. If all attempts finish without a quorum, it can fail
earlier. Insufficient acks produce 503 with the same `replicated`/`failed`
JSON fields as Milestone 2. At a deadline, this is a snapshot of results
collected so far; still-pending peers may not yet appear in either list.

A replication failure does not roll back successful writes. The receiving
node and any peers that successfully processed the request retain the
value. Therefore, a partial replication failure can temporarily leave
nodes with different state.

A node that was unavailable during a write does not automatically receive
the missed value when it restarts. Repairing this temporary inconsistency
is outside Milestone 2 and will be handled by later anti-entropy
functionality.

### Milestone 3 preparation: timestamp storage

The local store now retains a value and an `int64` timestamp per key.
`PutAt(key, value, ts)` persists the supplied timestamp in memory
and in the WAL unless a higher timestamp is already stored. Such an older
write is acknowledged without replacing the newer durable version. `GetWithMeta` returns it alongside the value and existence
flag. The existing `Put` generates a timestamp with `time.Now().UnixNano()`;
`Get` returns only the value, existence flag, and error as before.
Old WAL records without `ts` recover with timestamp zero.

Live writes and WAL replay retain the higher timestamp, so delayed older
writes cannot erase a newer version. Equal timestamps still use arrival
order locally; quorum reads retain the first encountered value on a tie.
Equal-timestamp conflicting values are an unresolved milestone limitation. For an external PUT, the coordinator now
generates one timestamp and uses `PutAt` locally. It passes that same
timestamp through `FanOut` and `Replicate` to every peer.

Internal PUT now accepts JSON `{"value":"Parimal","ts":1720000000000000123}`
and persists it using `PutAt`, without generating a new timestamp or
forwarding again. Both fields are required and must be non-null; `ts` must
be an `int64` integer. Empty values and timestamp zero are accepted.
Malformed payloads return 400. External PUT still accepts the raw value.
Both endpoints limit the decoded value to 1 MiB; internal PUT allows up to
6 MiB plus 1024 bytes of JSON body to accommodate escaping and metadata.
Oversized requests return 413. All running peers must use this JSON
protocol; the internal endpoint no longer accepts the old raw body format.

Physical-clock timestamps are a deliberate Milestone 3 simplification.
They do not eliminate clock skew between coordinators, clock adjustments,
or equal timestamps, and cannot reliably describe causal ordering.
Milestone 4 is planned to introduce vector clocks to track causal order
and identify concurrent versions; resolving those conflicts remains a
separate policy decision.

Concurrent writes from two coordinators use last-write-wins by timestamp:
the higher timestamp wins without detecting a conflict, even when the two
values represent independent updates. That information loss is a known
Milestone 3 boundary; vector clocks in Milestone 4 will make concurrency
detectable rather than silently treating both writes as an ordered pair.

### Milestone 3 preparation: internal reads

`GET /internal/kv/{key}` reads only the receiving node's store and returns
HTTP 200 with JSON containing `value`, `ts`, and `exists`:

```json
{"value":"Parimal","ts":1720000000000000123,"exists":true}
```

A missing key is a successful lookup with HTTP 200 and
`{"value":"","ts":0,"exists":false}`. An unhealthy store returns HTTP 500.
This lets quorum reads distinguish a responding replica with no
value from a failed lookup. The external `GET /kv/{key}` still returns
plain text for an existing key and HTTP 404 when all R valid answers
report absence. It returns 503 when fewer than R valid answers arrive.

### Milestone 3: quorum reads

External GET reads local metadata first. When R > 1, `ReadFanOut` queries
all peers concurrently through the shared HTTP client and a two-second
background context. Querying all peers allows another healthy peer to
answer when one fails; the handler stops waiting after R valid answers
including its local lookup. Missing keys count as answers; network errors,
non-200 statuses, and malformed metadata do not. Among those R answers,
the handler selects the highest timestamp that has `exists=true`.
R=1 needs only the local lookup. An unhealthy local store returns 500.

A buffered collector logs every attempted peer's outcome, latency, and
reported timestamp, including attempts that finish after the response.
The handler logs the quorum decision and selected timestamp. Reads do not
repair stale replicas.

For the same fixed set of N distinct replicas, W+R>N forces every
R-replica read set to overlap every W-replica successful write set: there
are only N-W replicas outside that write set, fewer than R. With versions
retained by timestamp, the read therefore sees that write's timestamp or
a higher one at an overlapping replica. This is a timestamp-order claim,
not a guarantee of real-time ordering across skewed clocks. Reads may also
observe writes whose clients received 503 after partial persistence.
Configure each actual replica exactly once and use consistent membership
on every node; address aliases are not a replica identity mechanism.

### Milestone 3 preparation: independent replication lifetime

After the local write succeeds, external PUT creates a two-second context
from `context.Background()` and passes it to `replication.FanOut`. Its
background result collector owns cancellation and releases the timer when
collection finishes. The handler does not defer cancellation when it
returns an early quorum response. Client cancellation therefore does not
cancel the peer writes. `StartFanOut` remains an independently timed helper
for callers that only need a results channel.

The results channel has room for one result per peer. Replication remains
in-process work, not a durable background queue: process exit stops any
remaining attempts. Tracking outstanding replication during shutdown is
not implemented in this increment.

### Milestone 3 incremental validation

The write-quorum tests cover a fast peer, a peer delayed by 900ms, and a
failing peer. With W=2 they require the response before 700ms and verify
that the delayed success is still logged afterward. Three simulated peers
plus the coordinator means N=4 in that test; it uses R=3 to preserve
W+R>N. Other tests cover W=1, insufficient acks, timeout snapshots, local
write failure, and client cancellation. Read tests cover freshest-version
selection, missing and empty values, malformed replies, failed peers,
R=1, R=3, early responses, and deadlines. Store tests cover delayed older
writes and out-of-order WAL replay. The final commit and milestone tag
remain upcoming steps.

### Repeatable real-cluster checks

Run from the repository root (requires Go, Python 3, curl, and POSIX signals):

```bash
python3 scripts/check-quorum-cluster.py
```

The script builds a temporary binary and launches three real server
processes with N=3, W=2, R=2, separate temporary WALs, and dynamically
chosen loopback ports. It uses curl's `time_total` to time requests. It
pauses only its own n3 process with SIGSTOP, resumes it with SIGCONT, then
separately kills and restarts n3 to create a persistent replication gap.
It cleans up its processes and temporary files, including on failure.

Observed on 2026-10-06 (five PUTs per scenario):

| Scenario | Successful PUTs | Median latency | Maximum latency |
| --- | --- | --- | --- |
| All nodes healthy | 5/5 | 8.270 ms | 8.371 ms |
| n3 paused | 5/5 | 6.029 ms | 6.457 ms |
| n3 killed | 5/5 | 5.828 ms | 7.641 ms |

These small local samples demonstrate that the unavailable third node did
not impose its two-second timeout on the client; the timing differences
are normal variation, not evidence that failure improves performance.
Milestone 2 required all peer successes and would return 503 for a failed
peer. Milestone 3 can return 200 after the local write and one peer succeed.
The historical Milestone 2 binary was not rerun for this measurement.

For the stale-replica check, all nodes first stored `old`. While n3 was
down, n1 and n2 stored `fresh` with the same higher timestamp. After n3
restarted, internal GETs confirmed `[fresh, fresh, old]`. All 15 external
quorum GETs (five per coordinator) returned `fresh`. Internal GET on n3
still returned `old` afterward, confirming selection without read repair.
The script also prints peer timestamps and selected-timestamp log examples.

## Learning note

The central Milestone 1 idea is the relationship between memory and
durable storage:

-   The in-memory map makes ordinary reads fast.
-   The WAL records changes so state can be rebuilt after restart.
-   `Sync()` is used before acknowledging successful writes.
-   Recovery replays the log to reconstruct the map.

Understanding these boundaries---especially what the system can and
cannot promise when failures occur---is the foundation for the
distributed milestone.
