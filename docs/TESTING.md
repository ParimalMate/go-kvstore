# Testing

Four layers, from fast to realistic. `scripts/verify-all.sh` runs all of them and prints a
PASS / FAIL / SKIP table (`--quick` runs the first two plus the checkpoint script).

| Layer | What it covers | Run it |
|---|---|---|
| Go unit and integration tests | store, clocks, merkle tree, gossip, anti-entropy, handlers, config; in-process clusters with real HTTP | `go test -race ./...` |
| Script unit tests (Python 3.10+) | invariant checker, fault schedule, proxy and node helpers | `cd scripts && python3 -m unittest -q $(ls test_*.py \| sed 's/\.py$//')` |
| Real-process scripts | three real servers behind proxies: quorum, vector-clock partitions, gossip, anti-entropy | `python3 scripts/check-<name>-cluster.py` |
| Chaos harness | randomized faults on a real cluster, then invariant checks | `python3 scripts/chaos-cluster.py --seed 1 --duration 60` |

## The chaos harness

Three server processes run behind per-direction proxies, built with the race detector so a data race
kills the node. Writers and readers run continuously while a scheduler injects faults:

- `kill -9` and restart on the same WAL, graceful `SIGTERM` restart,
- `SIGSTOP` / `SIGCONT` pauses (never longer than 3 s),
- cutting a link in both directions or in one direction only,
- adding 100-800 ms of delay to a link.

At most 2 of the 3 nodes are down or paused at once. When chaos ends everything is healed, the cluster
settles, and the harness checks:

| Check | When | Rule |
|---|---|---|
| Read after acknowledgement | every successful read, during chaos | After a write is acknowledged, a 200 or 300 read must return that version or a later one; a 404 is a violation. This follows from `W+R>N` plus durable writes. |
| No invented values | every read and the final state | Every value must exactly equal one the harness wrote. |
| No server errors | every response | PUT never returns 500; GET never returns 500 or 502. |
| Convergence | after healing | All root hashes are equal and stable, then every key has identical siblings on every node. |
| No lost acknowledged write | final state | Per key, a surviving version is at or above the highest acknowledged one. |
| Restart replay | after convergence | All nodes stop cleanly (exit 0); restarted on the same WALs, state is identical. |
| Process health | whole run | No unexpected exit, no `panic`, no `fatal error`, no `DATA RACE`. |

A write that times out or loses its connection is **indeterminate**: it may still apply later (for
example a request stuck behind a paused node), so an older-sequence indeterminate attempt can explain a value arriving after
a newer acknowledged write. A write that received an explicit error status gets no such allowance.

### What a seed replays

A seed fixes the fault schedule (and its hash is printed) and each writer's operation sequence. The
processes are real, so the exact interleaving differs between runs of the same seed. A seed is a
replayable schedule, not a deterministic simulation. If a seed fails, rerun it a few times and read
`chaos-results/seed-N/events.jsonl` next to the node logs in `chaos-results/seed-N/logs/`.

### Results

`python3 scripts/chaos-matrix.py --seeds 1-10 --duration 60 > docs/chaos-results.md` runs the seeds and
writes the table, with the machine, Go version and commit recorded automatically.
See [the local 60-second matrix](chaos-results.md) and
[the extended runs](chaos-extended-results.md). Every number in it
comes from the harness's own `summary.json` files.

## How we know the harness can fail

A test that cannot fail proves nothing, so the harness is checked from three directions.

1. **The checker is tested on fake histories.** `scripts/test_chaos_invariants.py` has 21 cases, one or
   more per rule. In the supplied independent review, breaking the checker in ten different ways (never flag a lost write, drop the
   indeterminate allowance, off-by-one on the floor, allow a 404 after an ack, stop normalizing zero
   clock entries, ignore GET 502, accept prefix-only value matches, count an ack that ended exactly at
   read start, skip the cross-node check, forget PUT 500) was caught by the tests every time.
2. **Two controls run on a real cluster** and succeed only if the problem is detected:
   - `--control no-repair`: anti-entropy off, one node misses writes; the harness must report
     non-convergence and the disagreeing keys.
   - `--mutate-final`: after collecting the real final state, remove the newest version of one key
     everywhere and mangle another value; the checker must report a lost write and an invented value.
3. **Injected product bugs.** Edit the server, run a seed, and revert. Expected outcomes:
   - *Premature acknowledgement*: in `PutHandler` change `if h.w == 1 {` to `if true {` and
     `if acks >= h.w {` to `if acks >= 1 {`. Run `python3 scripts/chaos-cluster.py --seed 301 --duration 30`.
     Expect read-after-ack violations (`lost acknowledged write ... required seq >= N`).
   - *Repair that never applies or pushes*: in the anti-entropy cycle make both `ApplyVersions` and
     `remote.push` no-ops. Expect non-convergence, disagreeing keys, a lost write and a replay mismatch.
   - *Repair that never applies pulled data, but still pushes*: **not detected, by design.** The node that
     holds the missing data pushes it from its own cycle, so the same divergence is still repaired. The
     harness checks outcomes, not mechanisms, and this is an equivalent mutant.

## Bugs found in the supplied harness review

- The crash monitor reported a deliberate graceful stop as a crash: it read a node's state before its
  exit status, so a stop that began between the two reads looked like an unexpected exit. The state is now
  set before the signal is sent and the exit status is read first. Covered by
  `NodeStateTests` in `scripts/test_clusterlib.py`.

## Known limits

- One machine and loopback networking; no real packet loss, reordering or clock skew.
- `kill -9` loses the process, not the page cache, so this does not test power-loss durability.
- Three nodes with `N=3, W=2, R=2` only; keys are small and each key has a single writer at a time.
- Anti-entropy runs on a 2 s interval here so convergence is observable within a test; production
  defaults are slower.


## Local verification record

On 2026-10-17, the supplied harness was installed with the large-payload test
fix. `scripts/verify-all.sh --quick` passed; the full verifier then passed all
Go/Python checks, four cluster scripts, both negative controls and seeds 1–3
at 45 seconds each, ending with `ALL CHECKS PASSED` (no skips).
The 60-second matrix was run separately and its original summaries were kept
in `chaos-results/matrix-60s/`, so verifier reruns did not overwrite that evidence.
Control logs are in `verify-logs/chaos-control-no-repair.log` and
`verify-logs/chaos-control-mutate.log`; these are local generated artifacts.
