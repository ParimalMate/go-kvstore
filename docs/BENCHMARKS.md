# Loopback benchmarks

Indicative educational measurements, not production capacity claims. Sequential Python HTTP clients open a connection per request; peer traffic uses direct HTTP connections, without fault proxies. Server builds omit the race detector. Writes append and fsync the WAL; latency includes client, OS and storage costs. No throughput extrapolation is made.

- Date (UTC): 2026-10-17T17:00:20.475540+00:00
- OS: macOS-27.0.1-arm64-arm-64bit-Mach-O
- CPU: Apple M4 (10 logical CPUs)
- Go: go version go1.27.1 darwin/arm64
- Python: 3.14.0
- Commit: 30e548a
- Percentiles: nearest rank; no client concurrency or warmup exclusion.


## B1: 1 node(s), W=1, R=1

| Case | Requests | p50 ms | p95 ms | p99 ms | HTTP outcomes |
|---|---:|---:|---:|---:|---|
| PUT, 64 bytes | 2000 | 3.993 | 5.153 | 8.074 | 200: 2000 |
| GET | 2000 | 0.099 | 0.148 | 0.189 | 200: 2000 |

## B2: 3 node(s), W=2, R=2

| Case | Requests | p50 ms | p95 ms | p99 ms | HTTP outcomes |
|---|---:|---:|---:|---:|---|
| PUT, 64 bytes | 2000 | 11.151 | 12.465 | 16.136 | 200: 2000 |
| GET | 2000 | 0.159 | 0.255 | 0.468 | 200: 2000 |

## B3: quorum payoff with one paused peer

Remeasured on 2026-10-26 with the corrected ten-request paused W=3 sample.
Other benchmark sections retain their original measurements.

Latency includes failed requests. With W=3 and a paused peer every write waits out the 2s quorum deadline and returns 503, so only 10 such writes are measured: a paused process keeps accepting connections into its listen queue (gossip and replication add several per second), and once that queue is full some operating systems refuse new connections immediately, which would turn later requests into fast failures that say nothing about quorum behaviour.

| Case | Requests | p50 ms | p95 ms | p99 ms | HTTP outcomes |
|---|---:|---:|---:|---:|---|
| W=2, R=2, healthy | 200 | 9.848 | 12.209 | 15.175 | 200: 200 |
| W=2, R=2, n3 paused | 200 | 8.023 | 9.279 | 10.706 | 200: 200 |
| W=3, R=1, healthy | 200 | 10.875 | 12.917 | 13.502 | 200: 200 |
| W=3, R=1, n3 paused | 10 | 2006.179 | 2008.158 | 2008.158 | 503: 10 |

W=3 writes that waited out the full 2000 ms quorum deadline: 10 of 10.

## B4: Merkle comparison after 10,000 shared keys

Automatic repair is disabled; manual cycles isolate the measured transfer.
Hashes exclude the final verification root; requests include it.

| Divergent keys | Hashes fetched | Requests | Duration ms | Pulled | Roots equal after | Naive key comparisons |
|---:|---:|---:|---:|---:|---|---:|
| 1 | 49 | 7 | 30 | 1 | yes | 10000 |
| 10 | 289 | 16 | 65 | 10 | yes | 10000 |
| 100 | 1473 | 111 | 348 | 100 | yes | 10000 |

## B5: automatic catch-up after 100 missed writes

Repair interval 2s/initial delay 1s; gossip 200ms/2s/4s. Elapsed time starts before restart and ends at the first equal-root poll.

| Repetition | Recovery seconds |
|---:|---:|
| 1 | 0.427 |
| 2 | 0.933 |
| 3 | 1.169 |
| 4 | 1.222 |
| 5 | 1.438 |

Median recovery: **1.169s**.
