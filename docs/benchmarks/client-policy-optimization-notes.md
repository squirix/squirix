# Client reliability wrapper allocation measurements

Measured on 2026-10-30 against baseline `38c692498`, in a separate working tree. Package versions match the baseline;
the separate NuGet update is not part of this comparison.

## Change

The retry loop now runs inside the concurrency-gate method, and attempt execution runs inside the attempt-timeout
method. This removes two nested asynchronous wrappers. The four remaining hot asynchronous methods in `CallPolicy`
and `EndpointFailover` use `PoolingAsyncValueTaskMethodBuilder<T>`.

Each returned `ValueTask` is consumed once by its caller. The public cache API still exposes its existing task-based
contract. Semaphore release, active-operation tracking, drain behavior, retries, failure classification and timeout
scope disposal keep their existing ordering.

## Isolated wrapper measurements

`AsyncPolicyBenchmarks` uses `Task.Yield` to exercise suspension without a network transport. This is a synthetic
allocation comparison, not a remote-call latency measurement. Its action can finish before every outer wrapper
suspends, so the exact allocation total also depends on scheduling.

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows, InProcessEmitToolchain, two warmups and five measurement iterations.
Tiered compilation uses its default setting for these runs.

| Scenario | Allocated before | Allocated after | Mean before | Mean after |
| --- | --- | --- | --- | --- |
| Policy without an operation budget | 1.13 KiB | 688 B | 1.307 microseconds | 0.996 microseconds |
| Bootstrap failover and policy with a budget | 1.76 KiB | 1288 B | 1.579 microseconds | 1.479 microseconds |

The timing confidence intervals overlap; these results establish reduced allocations, not a proven speedup.

## Remote read measurements

`WireScalarAllocBenchmarks.GetValueAsync` runs the exported client against a real local gRPC server, sequentially.
The server and client share a process. Both ephemeral and persistence configurations read pre-seeded scalar values.
This read benchmark does not exercise mutation fsync throughput or a replicated write workload.

Short initial runs with default tiered compilation were sensitive to warmup. A control comparison used a separate,
unchanged baseline checkout, five warmups, ten measurement iterations, a requested 500 ms iteration time and
`DOTNET_TieredCompilation=0` in both processes. The table reports that control comparison.

| Server mode | Allocated before | Allocated after | Mean before | Mean after |
| --- | --- | --- | --- | --- |
| Ephemeral | 12.48 KiB | 11.19 KiB | 156.40 microseconds | 155.99 microseconds |
| Persistence | 12.49 KiB | 11.20 KiB | 155.15 microseconds | 171.57 microseconds |

Allocations fell by approximately 10% in both modes. The ephemeral timing was essentially unchanged; the persistence
run was slower on average and had greater variance. Its before/after 99.9% confidence intervals overlap
(152.77-157.53 versus 149.43-193.72 microseconds), so these runs do not establish either a latency improvement or
the absence of a regression. Production conclusions require repeatable measurements with the normal JIT settings.
Per-request p99 and multi-node saturation throughput were not measured.

## Remaining timeout work

This change does not remove timers. The current implementation already reuses the operation-budget token when
the budget is no longer than the per-attempt timeout. Otherwise, the budget timer covers queuing and retries,
while the attempt timer bounds the current attempt. Their clocks may differ, so replacing both with one timer
would require a separate design that preserves deadline accounting and cancellation classification.

The new tests independently advance the budget clock and attempt clock while an operation is suspended and verify
that each produces its existing failure detail. Removing the ambient deadline context and changing client-pool
lookups are also outside this change.

## Validation

- Release builds of client unit tests, client integration tests, internal benchmarks and end-to-end benchmarks
  succeeded with analyzers and no warnings or errors.
- Client unit tests: 162 passed, zero failed or skipped, including both new independent-clock cases.
- Client integration tests: one passed, zero failed or skipped.
- Internal and real-gRPC benchmarks completed before and after the change.

Local measurement artifacts, relative to each checkout:

- `artifacts/perf-client-before/results/` and `artifacts/perf-client-after/results/` contain the isolated results
  in the implementation checkout.
- `artifacts/perf-wire-baseline-steady/results/` contains the control baseline in the baseline checkout.
- `artifacts/perf-wire-after-steady/results/` contains the control result in the implementation checkout.
- Earlier exploratory wire runs are retained under `artifacts/perf-wire-before/`, `artifacts/perf-wire-after/`,
  `artifacts/perf-wire-baseline-long/` and `artifacts/perf-wire-after-long/` in their respective checkouts.

## Reproduction

Build the internal benchmark project in Release, then run its assembly with
`--filter '*AsyncPolicyBenchmarks*' --artifacts artifacts/perf-client-result`.

For the wire control comparison, build the end-to-end benchmark project in Release. Set
`SQUIRIX_E2E_BENCHMARK_IN_PROCESS=1` and `DOTNET_TieredCompilation=0`, then run its assembly with
`--filter '*WireScalarAllocBenchmarks.GetValueAsync*' --warmupCount 5 --iterationCount 10 --iterationTime 500`.
Use identical settings in the unchanged baseline and implementation checkouts, and run them sequentially.
