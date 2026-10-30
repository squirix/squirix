# Bounded keyed-lock entry reuse

Date: 2026-10-30. Measured base: `22aa3b1049b3dec02b67dce5aebe1d6c4546c3c3` (`develop`).
Local candidate branch: `perf/keyed-lock-reuse`.

## Change and ownership invariants

`KeyedAsyncLock<TKey>` previously constructed an `Entry`, its `AsyncLock` and the lock's synchronization object
each time a key first entered an empty table slot. The entry was discarded after its last user left.
`DurableMutationExecutor` uses this table to protect mutations of a `CacheKey`, including replicated applies
when journaling is enabled.

The candidate retains at most 64 idle entries per table in a preallocated stack. An entry is returned only
when its user count reaches zero, after removing the key from the active dictionary. The count includes
holders and queued acquisitions, including a canceled acquisition until its failure cleanup leaves the table.
The table lock still protects lookup, reference counting, queue admission and the idle stack.

The `AsyncLock` instance and its generation counter are retained together. Neither generation reset nor lock
disposal occurs on return. A copied stale lease must first release its own generation successfully before
it can decrement the table's user count. After reuse for another key, that generation no longer owns the lock;
the stale copy cannot release the new holder or remove the new key. Existing FIFO handoffs and cancellation
registration behavior are unchanged. `Count` continues to report only held/awaited keys, excluding idle entries.

The stack retains no keys. The idle bound does not cap active keys or shrink the dictionary's backing storage.
A burst with more than 64 simultaneously idle entries drops the excess; later bursts can allocate new entries.
The tradeoff is bounded idle retention and the stack's startup allocation in exchange for lower hot-path churn.
Queued waiter and cancellation allocations remain. This is an allocation optimization, not removal of every lock.

## Fresh RF=3 contention profile

Three actual host processes run with persistence and internode mTLS, with the SDK in a fourth process.
256 closed-loop worker tasks share one SDK client and its existing per-peer concurrency limit of 64.
All processes share one physical Windows machine; each server and the client use `DOTNET_PROCESSOR_COUNT=4`.
After five seconds of warmup, successful scalar sets with unique keys run for 25 seconds. Every server is
traced for 15 seconds with `dotnet-sampled-thread-time` and `Microsoft-Windows-DotNETRuntime:0x4000:5`.
The parser pairs contention starts/stops by thread and groups the start stack using the stop duration.

Across all three traces there were 159 paired lock waits totaling 15.877 ms. Four stacks included
`KeyedAsyncLock`, totaling 0.028 ms. This does not establish a significant blocking table-lock bottleneck
in this workload. Contention events do not account for uncontended locking or spinning. The candidate
therefore keeps the table lock and addresses measured allocation churn without introducing table shards.
The profiled 9898 writes/s is diagnostic context, not a candidate/control performance comparison.

## Controlled primitive benchmark

`KeyedAsyncLockBenchmarks.DistinctKeysAsync` creates synchronized dedicated workers sharing one keyed table.
Each worker advances through disjoint, preallocated integer key values; no two workers wait for the same key.
Every invocation executes 65536 total acquire/release pairs split between workers. Time and allocation are
normalized by this total, including worker creation and coordination. These are wall-time costs per total pair,
not per-worker latency or RPC latency. Entry recycling is measured with 1, 8 or 24 active workers, below the idle bound.

Both variants use the same benchmark assembly and dependencies, replacing only the server DLL/PDB between
processes. BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K. Tiered JIT/PGO overrides
are absent. One in-process job uses three warmups, five measurements and a requested 500 ms iteration time.
No builds, tests or trace conversion overlap performance measurement. Two sequential control/candidate rounds
are retained. Between the candidate rounds only XML documentation changed; runtime logic is identical.

| Workers | Control round 1 (ns) | Candidate round 1 (ns) | Control round 2 (ns) | Candidate round 2 (ns) | Control allocated | Candidate allocated |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 89.66 | 80.64 | 89.10 | 80.59 | 136 B | approximately 0 B |
| 8 | 231.99 | 220.10 | 254.15 | 227.57 | 136 B | approximately 0 B |
| 24 | 294.18 | 236.26 | 224.66 | 245.84 | 136 B | approximately 0 B |

The single-worker pair improves by about 10% in both rounds. Multi-worker timing depends on scheduling and
table-lock contention; at 24 workers the direction reverses between rounds. A reliable 24-worker timing gain
is not established. Entry allocation elimination is consistent at every measured worker count. BDN renders
candidate normalized allocation as `-`; worker setup still allocates, and the rounded value is not an assertion
of zero allocation for the complete invocation. Cold starts, pool overflow and queued waits can still allocate.

## Unprofiled RF=3 writes

The control host/dependencies are frozen before production editing. The candidate replaces only its server
DLL/PDB; the SDK remains identical. Every run uses three fresh host processes and fresh data directories,
five seconds of warmup and 15 seconds of successful scalar sets with unique keys. The order at each load
is control, candidate, candidate, control. No profiler runs during these measurements.

48 or 256 worker tasks share one SDK client; they are not independent SDK connections. The loopback diagnostic
client bypasses development-certificate validation; internode mTLS validates the generated CA and node identities.
All hosts share the same machine and storage. The workload grows the cache and retained operation records,
so it is not a steady-state capacity or saturation study. CPU below sums all three server processes,
excludes the client, and is normalized by successful writes. Startup, warmup and teardown are excluded.

| Order | Workers | Variant | Writes | Throughput (op/s) | p50 (ms) | p99 (ms) | Server CPU (us/op) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 48 | baseline | 112518 | 7499 | 4.4854 | 26.9164 | 545.6 |
| 2 | 48 | candidate | 118063 | 7869 | 4.4974 | 26.0476 | 586.0 |
| 3 | 48 | candidate | 121996 | 8131 | 4.4209 | 25.8615 | 588.8 |
| 4 | 48 | baseline | 129067 | 8602 | 4.4024 | 25.0521 | 561.4 |
| 1 | 256 | baseline | 158527 | 10556 | 19.6853 | 91.9841 | 463.8 |
| 2 | 256 | candidate | 89663 | 5964 | 37.0825 | 105.0302 | 479.4 |
| 3 | 256 | candidate | 152370 | 10143 | 20.5368 | 91.9697 | 519.8 |
| 4 | 256 | baseline | 88773 | 5903 | 36.3376 | 109.7768 | 490.5 |

At 48 workers, controls span 7.50-8.60 thousand writes/s and candidates 7.87-8.13 thousand. At 256 workers,
controls span 5.90-10.56 thousand and candidates 5.96-10.14 thousand. Both variants have a slow 256-worker
sample, with p50 moving from about 20 to 36-37 ms; the cause of that variation is not established here.
The throughput and latency ranges overlap. Candidate CPU/op is higher in both 48-worker samples; the
256-worker CPU/op ranges overlap. These data establish neither a general RPC speedup nor reduced server CPU,
and do not provide a general non-regression guarantee. Two samples per variant/load on shared hardware
are insufficient to attribute the observed CPU differences to the pool. The supported benefit is lower
entry allocation churn and about 10% lower single-worker primitive cost in both microbenchmark rounds.

## Validation and limits

- Release server unit-test build: zero warnings/errors with repository analyzers enabled.
- Focused keyed-lock tests: 15 passed, zero failed/skipped (six existing and nine new cases).
- Full server unit suite: 4478 passed, zero failed; one macOS-only test skipped on Windows (4479 total).
- Full server integration suite: 322 passed, zero failed/skipped.
- New tests cover reuse across keys, rejection of copied stale ownership, canceled/pre-canceled acquisition
  cleanup, a burst exceeding the retention bound, and concurrent churn with 16 workers and copied releases
  across one/eight keys, with/without queued cancellation.
- Local manual review checked release-before-reference-decrement interleavings, pending handoffs, cancellation
  cleanup and reuse for another key. External CodeRabbit CLI was not run.

Tests and measurements cover these local concurrency and successful workload scenarios. They are not an
exhaustive interleaving proof or a crash, network-partition or recovery campaign. Journal ordering and durability
protocol code are unchanged.

## Reproduction

Ignored local artifacts under `artifacts/keyed47/` retain the frozen `baseline-host`, `candidate-host`,
`micro-harness`, initial candidate binaries, both control/candidate BDN rounds, `baseline-profile-256`, and
the eight `write-*` directories. They include full BDN JSON, trace/parser output, workload results and logs.
`reproduction/` retains the diagnostic probe, runner and measurement/parser helpers. These diagnostic scripts
refer to this local worktree; adjust their paths and output tags before running elsewhere or repeating runs.
Test certificates and persisted workload data remain ignored.

SHA-256 (final candidate DLL matches the production output and unit-test dependency):

- Control server DLL: `e5c5e5ac10d66517f1c6faf46abb2581b5b7bf7e4507b2fe72854499329d0bdd`.
- Final candidate server DLL: `b05a172ca3b90f84bc9cecf36dce1971c927162f85c81419fd4681698605cbd1`.
- Initial candidate server DLL: `cda57b843a784da1aa11774daeacc5dc7629f6adbf441d3089dd3ae554dcf867`.
- Final candidate keyed-lock source: `b2500eee2aa771f30d66fad20481d9b98d711fe2a313390c184e7fd03601b3f0`.

Build the Release test project, then run the focused cases using Microsoft.Testing.Platform:

```powershell
dotnet tests/squirix.server/squirix.server.unit-tests/bin/Release/net10.0/Squirix.Server.UnitTests.dll --treenode-filter "/*/*/KeyedAsyncLock*/*"
```

BDN arguments for the fixed harness, swapping only the server DLL/PDB between variants:

```text
--filter '*KeyedAsyncLockBenchmarks*' --inProcess --warmupCount 3 --iterationCount 5 --iterationTime 500 --exporters json
```
