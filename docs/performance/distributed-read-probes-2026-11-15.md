# Distributed read retained-outcome probes, 2026-11-15

Two `HasRetainedOperation` calls around a live entry read materially reduce scaling in this isolated single-copy experiment. A separate sampled-thread-time profile identifies
the shared store lock as the dominant waiting path. This does **not** establish that this lock causes the reported FullStackHero deficit against Redis/Valkey, or quantify an
application improvement from changing it.

## Revision and environment

- Production sources: develop `622b6843c0421426dd4a509f130f7c8b0b2bd54a`, including PR #1764; measurement runner added in this PR.
- Debian GNU/Linux 13, AMD EPYC 7763 host, 9 processors visible to .NET. The shared Cloud host and scheduling are not controlled; 32 workers oversubscribe the visible
  processors.
- SDK 11.0.100, target net10.0, runtime .NET 10.0.0; `DOTNET_TieredCompilation=0`. No debugger or profiler during timing runs.
- Two sequential process launches, five paired samples per cell per launch. The table pools ten samples per mode per cell and reports medians; every raw sample is retained in
  [run 1](distributed-read-probes-2026-11-15/run-1.md) and [run 2](distributed-read-probes-2026-11-15/run-2.md).

## What is measured

Each worker owns a different cache key and repeatedly reads an immutable 1 KiB payload with an absolute deadline one day ahead, through the real `PhysicalCache.GetEntry`. The
probe mode calls the real `RpcMutationIdempotencyStore.HasRetainedOperation` immediately before and after that read. Both modes validate that every entry stays live and keeps
the seeded value. Outcome counts are checked after every timed sample, including complete removal of the expired fixture in probe mode.

Workers are created once per worker-count cell and reused. Each worker executes 131,072 reads per timed sample. Each has 1,024 preallocated absent operation-ID strings which
cycle; IDs are never inserted. Fixture setup, full collections, thread creation and console output are outside timing. A paired sample alternates execution order; both
fixtures are rebuilt before each sample.

- `empty`: zero retained outcomes.
- `retained`: 60,000 completed, unexpired outcomes restored through `RestoreSnapshotRecords`.
- `expired`: 60,000 completed outcomes restored with creation dates one hour in the past. The first probe sweeps them; the rest of the batch reads the empty store. This is a
  **one-off restored-expiry burst**, not sustained expiry throughput or a tail-latency measurement.

The no-probe mode is a synthetic lower bound. Removing these probes from production would discard required replay/race checks and is **not** a proposed optimization.

## Results

`ns/op` is batch elapsed time divided by the **aggregate** reads of all workers; it is not individual request latency. CPU ns/op is process CPU time divided by that same
count. Process-wide allocation deltas include small barrier/measurement overhead.

| Workers | Store fixture | Read only, ns/op | With probes, ns/op | Read only, M ops/s | With probes, M ops/s | With probes, CPU ns/op |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | empty | 76.74 | 287.27 | 13.03 | 3.48 | 287.28 |
| 1 | retained | 85.81 | 398.78 | 11.65 | 2.51 | 392.36 |
| 1 | expired | 85.54 | 371.77 | 11.69 | 2.69 | 371.89 |
| 8 | empty | 21.34 | 427.00 | 46.87 | 2.34 | 1343.00 |
| 8 | retained | 23.61 | 577.26 | 42.36 | 1.73 | 1388.88 |
| 8 | expired | 22.66 | 475.00 | 44.14 | 2.11 | 1326.15 |
| 32 | empty | 26.73 | 366.75 | 37.42 | 2.73 | 634.49 |
| 32 | retained | 27.37 | 483.53 | 36.55 | 2.07 | 642.01 |
| 32 | expired | 27.83 | 403.37 | 35.94 | 2.48 | 638.76 |

All modes allocate approximately 72 B/read (72.00–72.01 in these batches), consistently with entry materialization. Probe overhead here is CPU/synchronization, not additional
per-read allocation. Fresh ID/request allocation, protobuf encoding and payload response copies are deliberately excluded.

In the retained fixture, adding workers from 1 to 8 reduces probe-mode aggregate throughput from about 2.51 to 1.73 M reads/s, while the lower bound rises from 11.65 to 42.36
M reads/s. Empty-store probes also lose scaling. Results at 32 workers are affected by oversubscription; no monotonic worker-count model or production capacity is inferred.

## Separate profile

A separate 32-worker launch uses 65,536 reads per worker, one measured pair per fixture plus warmups. Its timing is **excluded** from the table. Startup EventPipe collection
avoids requiring a diagnostic attach socket:

```sh
DOTNET_TieredCompilation=0 \
DOTNET_EnableEventPipe=1 \
DOTNET_EventPipeConfig='Microsoft-Windows-DotNETRuntime:0x4000:5,Microsoft-DotNETCore-SampleProfiler:0x0:5' \
DOTNET_EventPipeOutputPath=probes-profile.nettrace \
dotnet benchmarks/squirix.server.benchmarks/bin/Release/net10.0/Squirix.Server.Benchmarks.dll \
  distributed-read-probes --workers 32 --operations 65536 --samples 1 --prefill 60000

dotnet-trace report probes-profile.nettrace topN -n 25 --inclusive -v
```

The [complete top-25 report](distributed-read-probes-2026-11-15/profile-top-25.txt) gives:

- `HasRetainedOperation`: 67.4% inclusive sampled thread time.
- `Lock.EnterAndGetCurrentThreadId` / `Lock.TryEnterSlow`: 65.9% inclusive, beneath that lookup.
- `WaitHandle.WaitOneNoCheck`: 62.93% inclusive across the process.

These percentages include blocked thread time, setup and barriers; they are **not CPU utilization**, application latency shares or a predicted speedup. Inclusive parent and
child percentages must not be added together. The profile supports contention attribution inside this harness, not the FullStackHero application.

## Reproduce the timing runs

```sh
dotnet build benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj -c Release

DOTNET_TieredCompilation=0 dotnet \
  benchmarks/squirix.server.benchmarks/bin/Release/net10.0/Squirix.Server.Benchmarks.dll \
  distributed-read-probes --workers 1,8,32 --operations 131072 --samples 5 --prefill 60000
```

Run the last command twice in separate processes without overlapping workloads. The runner defaults to 65,536 reads per worker when `--operations` is omitted. It has no
network dependencies after build and does not modify production code.

## Limits and next decision

This isolates the two probes together: lock acquisition, clocks, lookup, expiry-order maintenance and cleanup. It does not separately price each of them. Retained fixtures are
restored records, whose expiry-order representation differs from records created during normal execution. There are no concurrent mutations, background sweeper, actual
retained-ID replays, network calls, admission, owner routing, JSON/protobuf serialization, sliding renewals, persistence or quorum read fences. Process CPU and scheduling on
this shared machine have limited precision. No p95/p99 was measured.

The FullStackHero fixture/configuration and original traces were not available in the checkout or discoverable related repository. Before attributing its 20–23% gap, obtain an
application trace and establish replica count, expiration policy, actual L2 Get rate, forwards/Get and renewals/Get. Under replication, distributed access uses its group
outcome registry instead of this single-copy store; this result does not diagnose that path.

The next production optimization candidate is a cheaper absent-ID probe/expiry-maintenance path **if the actual single-copy workload shows this wait**. Both before/after
visibility checks, replay identity matching, retention across clock steps and concurrent mutation semantics must be preserved and regression-tested. This experiment does not
justify deleting either check, weakening read fences, or changing sliding expiration semantics.

## Validation

Release benchmark build with the repository analyzers: zero warnings/errors. Both timing launches and the separate profile completed successfully; every read/value and final
outcome-count assertion passed. `git diff --check` passed. Production sources are unchanged; no new correctness claim for a modified cache implementation is made.
