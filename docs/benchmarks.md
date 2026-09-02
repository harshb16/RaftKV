# Measured benchmarks

Measured using `go1.27.1` on `darwin/arm64`.
All runs are local on this development machine; no cross-machine comparison or
production throughput claim is intended. The report is an example measurement,
not a statistically repeated campaign. Raw results: [benchmark.json](benchmark.json).

## Method

```
make build
./bin/raftkv-bench -operations 300 -output run/benchmark.json
```

Each workload completes 300 operations, with 1 or 8 concurrent clients, on a
3- or 5-node disk-backed cluster. The snapshot threshold is 64 applied entries.
The workload is one shared key and uses the production engine and real fsyncs.
Transport is the in-process simulated RPC implementation with faults disabled.
The benchmark excludes RESP parsing, HTTP serialization and kernel/network RTT.
Percentiles use nearest rank over successful operations; errors are reported
separately. Reported throughput divides successful completions by wall time.

Reads enter the log and have the same disk/quorum cost as writes. Synchronous
full-checkpoint persistence amplifies I/O; there is no group commit. Do not
compare these figures to Redis's leader-local in-memory command benchmarks.

## Throughput and latency

| Nodes | Clients | Operation | Successful | Errors | Ops/s | p50 ms | p95 ms | p99 ms |
|---:|---:|:---|---:|---:|---:|---:|---:|---:|
| 3 | 1 | SET | 300 | 0 | 39.6 | 24.95 | 31.11 | 48.77 |
| 3 | 1 | GET | 300 | 0 | 38.9 | 25.02 | 33.44 | 51.46 |
| 3 | 8 | SET | 300 | 0 | 110.9 | 69.04 | 98.94 | 115.18 |
| 3 | 8 | GET | 300 | 0 | 109.7 | 70.89 | 108.00 | 119.33 |
| 5 | 1 | SET | 300 | 0 | 27.3 | 34.99 | 51.34 | 73.09 |
| 5 | 1 | GET | 300 | 0 | 26.5 | 36.79 | 48.85 | 88.00 |
| 5 | 8 | SET | 300 | 0 | 79.8 | 93.88 | 141.31 | 158.60 |
| 5 | 8 | GET | 300 | 0 | 78.8 | 89.98 | 146.03 | 163.91 |

## Failure recovery

| Nodes | Leader crash to successful write ms | Replay catch-up ms | Snapshot catch-up ms |
|---:|---:|---:|---:|
| 3 | 303.38 | 30.60 | 46.99 |
| 5 | 300.00 | 31.29 | 43.00 |

Leader failure measures removal/Stop of the current leader through reelection
and one successful SET. This is a harness crash, not OS process-kill overhead.
Replay catch-up disconnects one follower for five writes, then times healing
through convergence. Both measured runs confirmed no new snapshot was needed.
Snapshot catch-up crashes a follower, performs 150 writes (forcing compaction),
then times restart through convergence; both runs confirmed its snapshot index
advanced. Restart time includes WAL recovery and follower catch-up.

Convergence requires every configured node to have the same committed/applied
index, no uncommitted tail and identical KV state. Catch-up measurements include
polling granularity (20 ms), scheduler variability and storage latency. They are
single observations. Re-run for a distribution before drawing tuning conclusions.

A separate real HTTP/RESP process demo verified redis-cli, SIGKILL failover,
restart, identical state digests and indexes. Its observed failover is recorded
in [verification.md](verification.md); it is not mixed into the harness tables.
