# Explicit client deadline budget measurements

Measured on 2026-10-30 against `984d20676`, after rebasing the reliability-wrapper change onto `eae45a326`.
The control checkout and changed checkout use the same NuGet versions. These results are incremental to the
[wrapper allocation measurements](client-policy-optimization-notes.md).

## Change

The client no longer stores its operation deadline in `AsyncLocal`. `EndpointFailover` passes a `DeadlineBudget`
explicitly to the per-peer policy. The transport receives `CallOptions` rebuilt from that budget on every attempt.
The budget retains its original monotonic start across endpoint failover and retries. Endpoint expiration checks
also use the monotonic remaining time, so wall-clock corrections do not prematurely stop or extend failover.

The exported client API, operation identity, retry classification, cancellation scopes and server deadline context
remain unchanged. This change does not combine the operation and attempt timers or change client-pool lookups.

## Measurements

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K, in-process toolchain.
The wire benchmark runs sequential reads against a real local gRPC server in the same process. It does not measure
replicated mutation throughput, crash recovery or network partitions.

The final control and changed wire runs were sequential, with five warmups, ten measurement iterations,
a requested 500 ms iteration time and `DOTNET_TieredCompilation=0` in both processes.

| Server mode | Allocated control | Allocated changed | Mean control | Mean changed |
| --- | --- | --- | --- | --- |
| Ephemeral | 11.19 KiB | 10.87 KiB | 178.5 microseconds | 197.0 microseconds |
| Persistence | 11.19 KiB | 10.87 KiB | 170.1 microseconds | 162.0 microseconds |

Managed allocations fell by approximately 0.32 KiB, or 2.9%, per read in both modes. This includes allocations in
both the client and server. Timing varied between runs: an earlier changed run measured 190.6 / 206.4 microseconds
and 10.88 KiB in ephemeral / persistence modes. The final timing confidence intervals overlap in both modes;
these runs establish neither a latency improvement nor the absence of a regression. Per-request p99 and
multi-node saturation throughput were not measured. Normal tiered-JIT measurements remain necessary before
making a production latency claim.

The synthetic `AsyncPolicyBenchmarks` uses `Task.Yield`, two warmups and five iterations, with default tiering.
It also exposes a tradeoff from carrying the larger explicit state through async methods:

| Scenario | Allocated control | Allocated changed | Mean control | Mean changed |
| --- | --- | --- | --- | --- |
| Policy without a budget | 688 B | 752 B | 1.188 microseconds | 1.365 microseconds |
| Failover and policy with a budget | 1287 B | 1232 B | 1.596 microseconds | 1.681 microseconds |

The path without an operation budget allocates 64 B more in this synthetic run; the production remote-cache path
always supplies a budget. Scheduling can let the yielded action finish before every outer wrapper suspends, so
the exact synthetic allocation totals depend on scheduling. These results do not establish a wrapper speedup.

## Validation

- Release builds with analyzers: client unit tests, client integration tests, internal and end-to-end benchmarks;
  no warnings or errors.
- Client unit tests: 160 passed, zero failed or skipped. Tests cover budget isolation between concurrent calls,
  wall-clock corrections, spent budgets, time retained across failover, per-attempt and operation timeout clocks,
  queued cancellation, outgoing UTC deadlines and stable mutation identities across retries.
- Client integration tests: one passed, zero failed or skipped.
- Obsolete ambient-scope lifecycle tests were replaced by budget behavior tests.
- CodeRabbit CLI was signed out, so no automated CodeRabbit review was performed.

## Reproduction and local artifacts

Build `benchmarks/squirix.benchmarks/Squirix.Benchmarks.csproj` in Release, then run its assembly with
`--filter '*AsyncPolicyBenchmarks*' --artifacts artifacts/perf-explicit-result`.

Build `benchmarks/squirix.e2e.benchmarks/Squirix.E2EBenchmarks.csproj` in Release. Set
`SQUIRIX_E2E_BENCHMARK_IN_PROCESS=1` and `DOTNET_TieredCompilation=0`, then run its assembly with
`--filter '*WireScalarAllocBenchmarks.GetValueAsync*' --warmupCount 5 --iterationCount 10 --iterationTime 500`.
Use identical settings in the control and changed checkouts and run them sequentially.

Local result directories:

- Changed checkout: `artifacts/perf-explicit-before/results/` and `artifacts/perf-explicit-after/results/` for
  the synthetic comparison.
- Control checkout: `artifacts/perf-wire-explicit-control/results/` for the final wire control.
- Changed checkout: `artifacts/perf-wire-explicit-repeat/results/` for the final wire result.
- Changed checkout: `artifacts/perf-wire-explicit-before/results/` and `artifacts/perf-wire-explicit-after/results/`
  for the earlier exploratory wire comparison.
