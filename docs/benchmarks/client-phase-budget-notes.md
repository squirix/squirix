# Client budget timer prototype

Measurements use client code at `c151a3654` (`develop`, including PR #1587).
The publication branch was subsequently rebased onto `930c5d15e`; intervening changes do not modify client production code.
The earlier colocated slowdown was investigated with an identical harness, isolated variants, thread profiles and
an unchanged server in a separate process. The allocation reduction repeats; separate-process reads do not show
the earlier sustained slowdown. No specific cause of the colocated excursions or guaranteed latency fix is proven.
See [the investigation and full results](client-phase-budget-investigation.md). The original tables below are
historical measurements of the pre-rebase candidate at `7b0f9ba0a`.

## Change and boundaries

An uncontended call whose budget and attempt use the same `TimeProvider` instance uses one policy timer for
`min(attempt timeout, remaining operation budget)`, instead of overlapping attempt and operation timers.
The budget retains its original monotonic start. A retry delay is capped to the remaining operation budget.

Calls that must wait for the concurrency semaphore retain an operation-wide budget source, as do calls whose
budget and attempt use different clocks. This avoids creating additional phase timers for queued calls whose
remaining budget is shorter than the attempt timeout. Attempt sources are disposed before retry backoff starts.
The existing gRPC deadline and server-side timers are unchanged; this is not one timer for an entire RPC stack.

An attempt timer that bounds the budget records budget expiry even if its callback precedes the next timestamp
observation. Caller cancellation keeps its existing precedence. An infinite per-attempt timeout is still bounded
by a finite operation budget.

The candidate also preserves the original ambiguous-commit exception when a budget timer has already fired.
Converting that terminal response into a retryable deadline failure would lose its no-retry contract. Tests cover
both shared and independent clocks and the RPC-to-domain error mapping after a deferred transport failure.

## Synthetic allocation comparison

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K, in-process toolchain, default JIT
settings, two warmups and five measurement iterations. `AsyncPolicyBenchmarks` uses `Task.Yield`; scheduling
can affect how many wrappers suspend.

| Scenario | Allocated before | Allocated candidate | Mean before | Mean candidate |
| --- | --- | --- | --- | --- |
| No operation budget | 751 B | 775 B | 1.191 microseconds | 1.186 microseconds |
| Failover and policy with a budget | 1231 B | 951 B | 1.520 microseconds | 1.401 microseconds |

The budget path allocates approximately 23% less. The internal path without a budget allocates 24 B more;
production remote-cache calls always supply a budget. Timing confidence intervals overlap, so this does not
establish a wrapper speedup.

## Real gRPC reads

The unchanged control and candidate use normal tiered compilation and tiered PGO, eight warmups, ten measurement
iterations and a requested 750 ms iteration time. The exported client and server share a process and read
pre-seeded values over loopback. No builds or tests overlap the runs in this table.

| Server mode | Control allocation, runs 1 / 2 | Candidate allocation, runs 1 / 2 | Control means, runs 1 / 2 | Candidate means, runs 1 / 2 |
| --- | --- | --- | --- | --- |
| Ephemeral | 10.56 / 10.57 KiB | 10.31 / 10.30 KiB | 52.26 / 45.29 microseconds | 86.03 / 77.29 microseconds |
| Persistence | 10.57 / 10.58 KiB | 10.27 / 10.26 KiB | 45.78 / 48.48 microseconds | 68.52 / 46.82 microseconds |

Allocation falls by approximately 2-3% per read. However, the candidate's ephemeral delay increase persisted when
the control was rerun and the candidate repeated. The microbenchmark gain does not justify shipping this version.
This was the reason for the follow-up investigation linked above; it is not the current separate-process result.
Persistence timing also varies. Saturation, per-request p99, crashes and network partitions were not measured.

## Validation

- Release builds with analyzers: client unit tests, integration tests, internal and end-to-end benchmarks;
  no warnings or errors. The unit project was also rebuilt without incremental compilation.
- Client unit tests: 183 passed, zero failed or skipped; client integration tests: one passed.
- Tests cover one policy timer on the uncontended shared-clock path, queued budget retention without additional
  timers, independent clocks, retry budget retention, bounded HTTP/gRPC backoffs, caller cancellation, infinite
  attempt timeouts, spent budgets, early timer callbacks and terminal ambiguous-commit errors.
- Publication rebase validation: 182 unit tests and 1 integration test passed; the new base removed one architecture test.
- These are local validation results; CI results are reported on the accompanying pull request.

## Reproduction and artifacts

For the synthetic comparison, build the internal benchmark project in Release and run its assembly with
`--filter '*AsyncPolicyBenchmarks*'`. Baseline artifacts in the timer checkout are
`artifacts/perf-timers-before/results/`; the final candidate is `artifacts/perf-timers-retained-queue/results/`.

For wire reads, build the end-to-end benchmark project in Release, set
`SQUIRIX_E2E_BENCHMARK_IN_PROCESS=1`, clear tiered compilation and PGO overrides, and run its assembly with
`--filter '*WireScalarAllocBenchmarks.GetValueAsync*' --warmupCount 8 --iterationCount 10 --iterationTime 750`.

Control checkout: `artifacts/perf-timers-wire-before/results/` and `artifacts/perf-timers-wire-before-repeat/results/`.
Candidate checkout: `artifacts/perf-timers-wire-after-final/results/` and `artifacts/perf-timers-wire-after-repeat/results/`.
Other `perf-timers-*` directories contain earlier exploratory versions; the tables use the final retained-queue version.
