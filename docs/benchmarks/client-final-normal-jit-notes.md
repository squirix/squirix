# Final client comparison with normal JIT settings

Measured on 2026-10-30: unchanged `develop` at `d365b930a` versus client changes at `7b0f9ba0a`.
Both checkouts use matching NuGet dependencies and benchmark source. This comparison includes all client wrapper,
deadline propagation, lookup, idle-completion and entry-read changes.

## Method

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K. The server and exported client share
a process and exchange real gRPC requests over loopback. Reads use pre-seeded scalar entries in both ephemeral
and persistence configurations.

Tiered compilation and tiered PGO use their default settings: the `DOTNET_TieredCompilation`, `DOTNET_TieredPGO`,
`COMPlus_TieredCompilation` and `COMPlus_TieredPGO` overrides were absent. The in-process benchmark toolchain
used eight warmups, ten measurement iterations, a requested 750 ms iteration time and one launch per case.
The four separate benchmark processes ran sequentially in control/candidate/candidate/control order (A1/B1/B2/A2),
without overlapping builds, tests or benchmarks. Each pass ran all four cases below.

## Results

Allocation ranges cover both passes for each version. Mean times are listed individually rather than pooled.

| Read and server mode | Control KiB/op | Candidate KiB/op | Control means A1 / A2, microseconds | Candidate means B1 / B2, microseconds |
| --- | --- | --- | --- | --- |
| Value, ephemeral | 12.30-12.32 | 10.56-10.58 | 64.13 / 47.85 | 55.66 / 45.21 |
| Value, persistence | 12.31 | 10.58 | 61.82 / 44.50 | 48.27 / 44.88 |
| Entry, ephemeral | 12.67-12.68 | 10.81-10.82 | 52.62 / 57.92 | 50.02 / 92.00 |
| Entry, persistence | 12.67-12.69 | 10.81-10.83 | 48.42 / 46.10 | 48.44 / 48.85 |

The comparison confirms approximately 14-15% less managed allocation per read across the complete change set
under normal JIT settings. Totals include both client and server allocations.

Timing does not show a consistent improvement. In particular, the second candidate ephemeral entry-read pass
was slower, with a 92.00 microsecond mean and a 44.182 microsecond half-width of the 99.9% confidence interval.
Normal-JIT runs still varied substantially despite warmup and alternating execution. These measurements do not
establish either a repeatable latency improvement or the absence of a latency regression.

This is a sequential, single-node, same-process read workload. Per-request p99, saturation throughput, separate
processes or machines, replicated mutations, crashes and network partitions were not measured.

## Validation and artifacts

The code revision `7b0f9ba0a` passed its GitHub CI success check. Local validation of that revision included
168 client unit tests and one integration test, all passing, and Release builds with analyzers. No inline review
threads or submitted PR reviews were present when checked.

Control checkout: `artifacts/perf-final-normal-a1/results/` and `artifacts/perf-final-normal-a2/results/`.
Candidate checkout: `artifacts/perf-final-normal-b1/results/` and `artifacts/perf-final-normal-b2/results/`.
Each directory includes CSV, Markdown and full JSON BenchmarkDotNet results.

To reproduce, build the end-to-end benchmark project in Release in both checkouts, clear the four JIT overrides
listed above, and set `SQUIRIX_E2E_BENCHMARK_IN_PROCESS=1`. Run its assembly with
`--filter '*WireScalarAllocBenchmarks.GetValueAsync*' '*WireScalarAllocBenchmarks.GetEntryAsync*' --warmupCount 8 --iterationCount 10 --iterationTime 750`.
Use separate artifact directories for the four alternating passes.
