# Verification record

Verified locally using Go 1.27.1 on macOS/arm64.

## Checks

Passed:

```
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
test -z "$(gofmt -l cmd internal tests)"
```

Also passed five repetitions of the three- and five-node automated chaos test
under the race detector. Tests were re-run after recovery and protocol changes.
The CI workflow runs the suite, race detector, vet and a formatting check on
Ubuntu. CI has been configured; no remote CI run is claimed.

## Real processes

Three- and five-process clusters were launched on separate loopback ports.
Both demos used redis-cli for SET, GET, MSET and INCR, killed the leader with
SIGKILL, issued a successful write through the newly elected leader, restarted
the old leader and verified identical committed/applied/last indexes and KV
state SHA256 across all nodes.

Observed crash-to-successful-write times in the final demo runs:

| Processes | Time | New leader |
|---:|---:|:---|
| 3 | 525 ms | node1 |
| 5 | 429 ms | node4 |

Both converged at index 8 with identical state hashes. These observations were
made alongside other verification work and include process-tool overhead; they
are demonstrations, not a controlled latency distribution.

Real HTTP InstallSnapshot was verified separately: one three-node follower was
SIGKILLed, 150 INCR commands were acknowledged by the surviving majority, the
leader compacted at index 128, and the restarted follower installed a snapshot
at index 128. Every node converged and GET returned counter value 150.
An entire process-cluster stop/start subsequently verified snapshot recovery.

The redis-cli --pipe test identified its ECHO completion probe and blank separator.
Binary-safe local ECHO and blank-separator support were added and tested; pipelined redis-cli then completed without
errors. All eight running nodes' /metrics endpoints were checked for command
counters, snapshot indexes and latency histogram output. Status/info tooling
was exercised against the real cluster.

## Recorded chaos histories

Standalone ten-second seed-42 runs:

| Nodes | Recorded operations | Completed | Unknown outcomes | Rejected | Checker | Convergence |
|---:|---:|---:|---:|---:|:---|:---|
| 3 | 324 | 271 | 52 | 1 | Ok | yes |
| 5 | 399 | 357 | 41 | 1 | Ok | yes |

Both histories were checked again using `raftkv-chaos -check`. The three-node
long run occurred earlier in implementation; final uncached tests also exercised
three-node chaos with the hardened persistence/replication code. The five-node
long run included 38 recorded crash/network/restart events. Local artifacts are
in `run/history-3.json`, `run/history-5.json` and `run/events-5.jsonl`; these
runtime files are intentionally ignored by Git. Generate fresh histories with
the documented CLI commands.

Unknown outcomes were retained as pending possibilities, including interrupted
requests on stopped nodes. No timed-out writes were silently omitted. A checker
regression caught an incorrect stopped-request classification during development;
it was corrected and tested. Separate model tests reject deliberate stale reads
and duplicate increments.

## Measured performance and source review

The benchmark tool completed eight 300-operation workloads (3/5 nodes, 1/8
clients, SET/GET) with zero errors. Both replay measurements used no new snapshot;
both long-disconnection measurements required snapshot catch-up. The complete
report is committed as [benchmark.json](benchmark.json), with methodology and
limitations in [benchmarks.md](benchmarks.md).

Protocol review produced targeted regression checks for current-term-only commit,
heartbeat handling of divergent tails, term fencing of stale responses, stale
snapshot rejection, persisted votes, uncommitted restart tails, corruption of
WAL headers and forward conflict hints at follower snapshot boundaries.

Passing tests are evidence, not a formal proof or a production reliability claim.
The fault runner is an in-process disk-backed cluster; OS crash behavior was
verified separately by the process demos. Authentication, TLS, dynamic membership,
ReadIndex optimization and granular/group-commit storage are outside this build.
