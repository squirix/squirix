# CI performance baselines

Managed allocations have fixed limits measured on Ubuntu and checked on PRs.
A separate nightly workflow compares execution time with the pinned preview.10 sources rebuilt on the same runner.

## Managed allocation limits

The allocation job checks eight public gRPC Get/Set cases and five server read cases.
The eight wire limits were refreshed from three independent successful Ubuntu CI runs using preview.10 runtime sources:
[41172022646](https://github.com/squirix/squirix/actions/runs/41172022646),
[41171427258](https://github.com/squirix/squirix/actions/runs/41171427258) and
[41167725313](https://github.com/squirix/squirix/actions/runs/41167725313).
Each stored value is their rounded arithmetic mean; the existing 5% tolerance is unchanged.
The previous limits below are previous CI budgets, not a newly measured preview.9 comparison.

| Wire case | Previous B/op | Refreshed B/op |
| --- | ---: | ---: |
| Scalar Get, ephemeral | 12874 | 10469 |
| Scalar Get, persistent | 12734 | 10361 |
| Scalar Set, ephemeral | 14163 | 11685 |
| Scalar Set, persistent | 18386 | 14342 |
| Structured Get, ephemeral | 14242 | 11895 |
| Structured Get, persistent | 14236 | 11882 |
| Structured Set, ephemeral | 15025 | 12609 |
| Structured Set, persistent | 19323 | 15255 |

Four server value-read paths retain their strict zero-allocation budget.
The local entry read retains its 72 B/op budget.
These metrics are bytes allocated per operation, not retained heap size, RSS or GC pause duration.

The stored limits and source run links are in
[wire-alloc-ci-baseline.json](squirix.e2e.benchmarks/wire-alloc-ci-baseline.json) and
[alloc-ci-baseline.json](squirix.server.benchmarks/alloc-ci-baseline.json).
The checker rejects regressions, missing cases and cases with no baseline.
A zero-byte budget permits no allocation.

## Same-runner release comparison

[performance-ci-baseline.json](performance-ci-baseline.json) pins preview.10 to the full immutable commit
fa3ddce418aa658cc1b71802b4cd6a337d3c0003.
The [runner](../tools/ci/run-performance-guard.py) creates a temporary checkout of that revision.
It rebuilds both versions with the current SDK and identical selected benchmark sources, then runs BenchmarkDotNet
in native child processes: one launch, ten warmup iterations and ten measurement iterations at 500 ms.
Upper outliers are retained. Server and client suites use opposite version order to reduce systematic order bias.

The 39-case suite covers:

- Physical and replicated reads, including the allocated entry control.
- ASCII and Unicode writes at 16, 4096 and 65536 UTF-16 units.
- Get/Set with no metrics listener, a counter, a histogram and both instruments.
- Empty and nonempty idempotency response serialization.
- Put/remove journal frame encoding.
- Small/large replica record encoding and decoding at four payload sizes.
- Public, forwarded and nested invocation scopes.
- Structured JSON entry encoding/decoding at two object sizes.
- Client call policy alone and combined with bootstrap failover.

All 39 cases compare allocations with the simultaneously measured release, with a 5% tolerance.
The 37 cases with meaningful timing compare medians, allowing at most a 20% slowdown.
Empty/nonempty response-buffer timings approach measurement resolution; those two cases guard allocations only.
A relative standard error above 5% fails as a noisy measurement requiring a rerun.
The guard also fails on missing or duplicate cases, different host/runtime/SDK identities, invalid data,
in-process results and fewer than ten measured samples. Smoke/Dry data cannot satisfy it.

This is a representative hot-path regression suite, not complete coverage of every optimization.
It does not establish whole-server throughput, RPC p99 latency, disk durability latency or multi-node capacity.

## Nightly timing and fast PR checks

The [Nightly Performance workflow](../.github/workflows/performance-nightly.yml) runs at 03:00 Asia/Krasnoyarsk
(20:00 UTC), with an optional manual dispatch. It has no push or pull-request trigger.
The measured full comparison takes about 26 minutes; it does not run on PRs and does not block their CI success.
A nightly regression or invalid/noisy measurement fails that separate workflow and preserves its diagnostics.

The regular PR/push CI retains the fast allocation guard and runs the comparator/runner unit tests, which take a few seconds.
Those fast checks gate CI success. Benchmark Smoke remains a separate execution check with no timing thresholds.
The nightly workflow uploads raw JSON, logs, a summary table and the exact manifest/commit identities.

Run a local comparison from a complete checkout with the pinned release commit available:

    python tools/ci/run-performance-guard.py benchmarks/performance-ci-baseline.json /path/to/fresh-artifacts

Use a fresh artifact directory. Logs are preserved on failure; only the owned temporary release checkout is removed.
Do not run other builds or benchmarks on the measurement host at the same time.

To refresh wire allocation limits, use check-wire-alloc.py with --write and at least three successful CI artifact directories,
then run the checker separately against every input run and retain the source run links.
Refresh the timing reference only as an explicit reviewed change to the pinned release commit.
Review newly selected cases and the baseline diff; a failing guard does not justify automatically raising its limits.
