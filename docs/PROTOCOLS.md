# Protocols

## HTTP API

The following examples target the three-node cluster above.

Keys and values must be valid UTF-8 text; binary values are unsupported.
Both PUT endpoints reject invalid UTF-8 with 400 before persistence. Valid
Unicode, empty values, NUL and newline characters are supported and survive
WAL replay. Internal PUT checks raw JSON bytes before decoding so invalid
bytes cannot silently become replacement characters. The store also returns
`ErrInvalidText` for invalid keys or values when called directly.

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
examples under [known limitations](DESIGN.md#known-limitation-the-coordinator-knows-only-local-history).

Peer-facing endpoints use the same key path under `/internal/kv/{key}`.
Internal PUT accepts `{value, vc}`; internal GET returns the full sibling
array, or `[]` for an absent key. Invalid payloads return 400; oversized
bodies/values return 413. Internal equal-clock/different-value collisions
return 409. Internal GET never performs another fan-out.

## Gossip

`POST /internal/gossip` accepts and returns:

```json
{"from":"n1","members":[{"id":"n1","generation":1234,"heartbeat":42}]}
```

The response digest is taken after merging the request. Bodies are capped at
64 KiB, messages at 256 members. IDs match
`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`; generation is positive, heartbeat nonnegative,
and duplicate IDs are invalid. The sender must include its own entry.

Evidence compares `(generation, heartbeat)` lexicographically. Only strictly
newer evidence refreshes local last-change time. Self entries are never adopted
from peers. Same self ID/generation indicates an alias; a different generation
indicates a duplicate ID. Status is local evidence age, not a remote verdict.

`GET /internal/members` returns self identity, generation, member status/age/via,
and unreached seeds. Indirectly learned identities need not have a direct address.

## Merkle tree and canonical hashing

Fanout is 16 with levels 0 through 3 (4096 leaves). Children of `(level,index)`
are `(level+1,16*index ... 16*index+15)`. Hashes use SHA-256; integers in
canonical inputs use big-endian fixed widths. JSON hashes are 64 hex characters.

| Item | Canonical form |
|---|---|
| Bucket | First 12 bits of SHA-256(key), 0–4095 |
| Clock | Nonzero entries sorted by ID: uint32 count, then uint16 ID byte length, ID, uint64 counter |
| Version | uint32 value byte length, value, clock bytes |
| Key hash | SHA-256(0x02, uint32 key byte length, key, uint32 sibling count, sorted versions) |
| Leaf | Empty: 32 zero bytes; otherwise SHA-256(0x00, key hashes in sorted key order) |
| Internal node | SHA-256(0x01, sixteen child hashes) |

Versions sort by canonical clock bytes then value. Timestamps are excluded;
missing clock entries and explicit zero counters have the same identity.

## Anti-entropy endpoints

`POST /internal/ae/nodes` takes `{"level":0,"indexes":[0]}` and returns
`{"fanout":16,"levels":3,"hashes":["..."]}` in request order. Levels are
0–3; each request contains 1–256 distinct, in-range indexes.

`POST /internal/ae/buckets` takes `{"indexes":[0]}` and returns
`{"fanout":16,"levels":3,"buckets":[{"index":0,"keys":[]}]}`. Nonempty
key listings contain `{key,hash}` objects sorted by key. Requests contain 1–64
distinct bucket indexes. Both endpoints cap requests at 64 KiB and responses
at 8 MiB. Invalid input returns 400; oversized bodies return 413.

The remote validates shape, order, hash representation, key placement and
listing uniqueness. A listing must recompute to the leaf used during descent;
if it changed during comparison, that bucket is skipped until a later cycle.

`POST /internal/ae/run` accepts an empty body or `{}` for eligible configured
peers, or `{"peer":"host:port"}` for one explicitly configured address.
An explicit address must match a `--peers` entry exactly (otherwise 400), but
it bypasses gossip liveness gating. Queue saturation returns 429 and a stopped
worker returns 503. Completed requests return a JSON array of reports; a cycle
failure is in its report's `error`, so HTTP 200 alone does not prove convergence.

`GET /internal/ae/status` returns `totals` and `last_reports`. Reports include
root equality before/after, comparison hashes, total HTTP requests, divergent
and skipped buckets, pulled/pushed keys, applied versions, collisions,
per-key errors, truncation and duration. Comparison hashes exclude the final
verification root; request counts include it. Totals are process-local and reset
on restart.
