# Client pool lookup and active endpoint measurements

Measured on 2026-10-30, incrementally against `f93c5ea63`, after
[explicit deadline propagation](client-explicit-deadline-notes.md).

## Change

The pool registers the reusable gRPC client and its call policy together as a `NodeClient` value. Each remote
attempt resolves this pair in one case-insensitive dictionary lookup, replacing separate client and policy lookups.
Channels retain their existing ownership, warmup and disposal paths. Policies still drain before disposal, and
all policies are disposed before channels.

`EndpointFailover` reads and publishes its active index with `Volatile.Read` and `Volatile.Write`. Only the integer
index is mutable; configured endpoints and their ordering remain fixed. No compound state needs the former lock.
Successful reroutes retain the existing publication rule, including when operations complete concurrently.

The exported API, endpoint ordering, retry rules, operation budgets and mutation identities are unchanged.

## Isolated lookup comparison

`PoolLookupBenchmarks` resolves the same node repeatedly from a two-node pool and consumes both references.
Before the change it calls `ForNode` and `PolicyFor`; afterward it calls `ForNode` once and consumes the two fields.
It does not connect channels. Each invocation batches 16,384 resolutions.

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K, in-process toolchain, default tiering,
two warmups and five measurement iterations. Runs were sequential.

| Operation | Mean before | Mean after | Managed allocation |
| --- | --- | --- | --- |
| Resolve client and policy | 8.599 ns | 4.057 ns | None measured in either run |

The 99.9% confidence intervals were 8.520-8.677 ns before and 4.003-4.111 ns after. This establishes a small
lookup improvement for this configured pool, not a network request speedup or a result for arbitrary pool sizes.
The remaining lookup is retained; binding failover indices to pool arrays would introduce more coupling for
a small remaining cost.

## Active index and wire checks

The synchronous failover microbenchmark measured 105.6 ns before and 98.6 ns after, with standard deviations
of approximately 42 ns in both runs. BenchmarkDotNet warned about short iterations. These runs do not establish
a speedup from removing the index lock. Lock contention under a saturated workload was not measured.

A subsequent real local gRPC read run completed in both server modes:

| Server mode | Mean | Managed allocation per read |
| --- | --- | --- |
| Ephemeral | 134.9 microseconds | 10.80 KiB |
| Persistence | 117.6 microseconds | 10.78 KiB |

The wire check used five warmups, ten measurement iterations, a requested 500 ms iteration time,
`SQUIRIX_E2E_BENCHMARK_IN_PROCESS=1` and `DOTNET_TieredCompilation=0`. Client and server shared a process.
There was no immediately paired unchanged wire control for this increment. Earlier runs showed substantial
timing variation, so the lower wire means and small allocation differences cannot be attributed to these edits.
Per-request p99, multi-node saturation throughput, crashes and network partitions were not measured.

## Validation

- Release builds with analyzers: client unit tests, client integration tests, internal and end-to-end benchmarks;
  no warnings or errors.
- Client unit tests: 162 passed, zero failed or skipped.
- Client integration tests: one passed, zero failed or skipped.
- Tests check case-insensitive client-policy pairing, distinct node resources, channel reuse, disposal behavior,
  retention of the successful failover endpoint and 16 suspended reroutes completing together.
- Diff whitespace checks passed. CodeRabbit was unavailable without account authentication.

## Reproduction and artifacts

Build the internal benchmark project in Release, then run its assembly with
`--filter '*PoolLookupBenchmarks*' --artifacts artifacts/perf-pool-result`.

The baseline uses `f93c5ea63` plus the benchmark version that consumes `pool.ForNode("node-a")` and
`pool.PolicyFor("node-a")` separately. Run the changed version with the same settings.

For the failover microbenchmark, run the internal benchmark assembly with
`--filter '*PolicyOverheadBenchmarks.FailoverCompleted*' --inProcess --warmupCount 2 --iterationCount 5`.

Local artifacts in the implementation checkout:

- `artifacts/perf-pool-before/results/` and `artifacts/perf-pool-after/results/`: lookup comparison.
- `artifacts/perf-index-before/results/` and `artifacts/perf-pool-index-after/results/`: exploratory failover results.
  The combined changed run also contains duplicate in-process lookup jobs; the table uses the standalone lookup run.
- `artifacts/perf-wire-pool-index-after/results/`: local gRPC check.
