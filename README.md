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
