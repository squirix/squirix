# RPC error guard and idle policy completion measurements

Measured on 2026-10-30, incrementally against `3c09703fd`, based on `develop` commit `d365b930a`.

## Change

The remaining `RemoteCacheRpc.AwaitRpcGuardedAsync` wrapper uses `PoolingAsyncValueTaskMethodBuilder<T>`.
Its caller consumes the returned value once. The public task-based cache API and RPC-to-domain error mapping
are unchanged.

When the active operation count reaches zero, `CallPolicy` now enters its disposal lock only if disposal has
already started. Normal sequential calls no longer acquire this lock on every completion. The counter decrement
still happens before the volatile disposal check. If disposal starts later, the disposer observes the idle counter
under its own lock. If disposal has already published its state, the final exiting call takes the lock and completes
the existing semaphore-disposal check. Calls entering after disposal retain the existing rejection path.

This does not pool cancellation sources, combine timers or change cancellation classification.

## Measurements

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K, in-process toolchain.
Both comparisons used five warmups, ten measurement iterations, a requested 500 ms iteration time and
`DOTNET_TieredCompilation=0`. Runs were sequential.

| Isolated operation | Mean before | Mean after | Allocated before | Allocated after |
| --- | --- | --- | --- | --- |
| Completed policy call without an operation budget | 266.5 ns | 196.9 ns | 160 B | 160 B |

The 99.9% confidence intervals were 265.223-267.862 ns and 195.694-198.195 ns. The policy-only benchmark does
not execute the remote RPC error guard, so it isolates the idle-completion path. Its synchronous action is not
a representative remote-request latency workload. Contention under saturation was not measured.

`WireScalarAllocBenchmarks.GetValueAsync` reads pre-seeded scalar values through the exported client and a real
local gRPC server in the same process. `SQUIRIX_E2E_BENCHMARK_IN_PROCESS=1` was set for both wire runs.

| Server mode | Allocated before | Allocated after | Mean before | Mean after |
| --- | --- | --- | --- | --- |
| Ephemeral | 10.87 KiB | 10.65 KiB | 169.2 microseconds | 114.4 microseconds |
| Persistence | 10.88 KiB | 10.66 KiB | 150.3 microseconds | 120.7 microseconds |

Managed allocations decreased by approximately 0.22 KiB, or 2%, per read. The totals include both client and
server allocations. Mean times were lower in this single before/after pair, but prior runs showed substantial
variation. This is not evidence of a repeatable production latency improvement or the absence of a regression.
Normal tiered-JIT runs, per-request p99 and multi-node saturation throughput remain unmeasured for this increment.
The benchmark does not establish mutation durability, crash safety or behavior during network partitions.

## Validation

- Release builds with analyzers: client unit tests, client integration tests, internal and end-to-end benchmarks;
  no warnings or errors. Unit tests were also fully rebuilt without incremental compilation.
- Client unit tests: 165 passed, zero failed or skipped.
- Client integration tests: one passed, zero failed or skipped.
- New deterministic tests start disposal before and after the final active call exits. The existing concurrent
  dispose/execute regression also passed.
- The ambiguous-commit response test now covers both immediate failure and a transport response held behind
  an explicit gate. Both preserve the original exception and operation identity without retries.
- CodeRabbit remains unavailable without account authentication; no automated CodeRabbit review was performed.

## Reproduction and artifacts

Build the internal benchmark project in Release. Set `DOTNET_TieredCompilation=0` and run its assembly with
`--filter '*PolicyOverheadBenchmarks.CallPolicyCompleted*' --inProcess --warmupCount 5 --iterationCount 10 --iterationTime 500`.

For the wire comparison, build the end-to-end benchmark project in Release, set
`SQUIRIX_E2E_BENCHMARK_IN_PROCESS=1` and `DOTNET_TieredCompilation=0`, then run its assembly with
`--filter '*WireScalarAllocBenchmarks.GetValueAsync*' --warmupCount 5 --iterationCount 10 --iterationTime 500`.
Use identical settings for the unchanged baseline and candidate, with no overlapping benchmark runs.

Local artifacts in the implementation checkout:

- `artifacts/perf-policy-idle-before/results/` and `artifacts/perf-policy-idle-after/results/`.
- `artifacts/perf-wire-policy-guard-before/results/` and `artifacts/perf-wire-policy-guard-after/results/`.

## Entry read follow-up

The subsequent change folds the single-use internal entry-read method into the exported `GetEntryAsync` method.
It removes one task and asynchronous state machine from entry reads. The result still distinguishes missing keys
from present entries and rebases a server-reported remaining lifetime on the client clock after the response arrives.

The baseline production code is `c59315cc8`; `5ac89678f` changes only a disposal test. The same wire settings above
were used with `--filter '*WireScalarAllocBenchmarks.GetEntryAsync*'`. No builds or tests overlapped the final
changed measurement.

| Server mode | Allocated before | Allocated after | Mean before | Mean after |
| --- | --- | --- | --- | --- |
| Ephemeral | 11.14 KiB | 10.88 KiB | 154.7 microseconds | 115.4 microseconds |
| Persistence | 11.11 KiB | 10.88 KiB | 165.8 microseconds | 115.8 microseconds |

This is approximately 2% less managed allocation per entry read. The timing improvement in this pair is not
a production latency claim: normal tiering, saturation, per-request p99 and repeated alternating control runs
were not measured. The earlier changed run partly overlapped an integration-project build and is retained only
as an exploratory result.

Validation: the client unit project was fully rebuilt with analyzers; 168 tests passed, zero failed or skipped.
The integration project built and its single test passed. The end-to-end benchmark project built and both entry-read
server modes completed. Three gated-response cases cover misses, original wire timestamps, and remaining-lifetime
rebasing after advancing the client clock.

Artifacts: `artifacts/perf-entry-before/results/`, `artifacts/perf-entry-after-clean/results/` and the exploratory
`artifacts/perf-entry-after/results/` in the implementation checkout.
