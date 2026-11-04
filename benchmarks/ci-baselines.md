# CI performance baselines

Managed allocations have fixed limits measured on Ubuntu and checked on PRs.
A separate nightly workflow compares execution time with the pinned accepted develop sources rebuilt on the same runner.

## Managed allocation limits

The allocation job checks eight public gRPC Get/Set cases and five server read cases.
The eight wire limits were refreshed from three independent successful Ubuntu CI runs from accepted develop sources:
[41298680753](https://github.com/squirix/squirix/actions/runs/41298680753),
[41274847911](https://github.com/squirix/squirix/actions/runs/41274847911) and
[41273596456](https://github.com/squirix/squirix/actions/runs/41273596456).
Each stored value is their rounded arithmetic mean; the existing 5% tolerance is unchanged.
The previous limits below are previous CI budgets, not a newly measured preview.9 comparison.

| Wire case | Previous B/op | Refreshed B/op |
| --- | ---: | ---: |
| Scalar Get, ephemeral | 10469 | 10530 |
| Scalar Get, persistent | 10361 | 10446 |
| Scalar Set, ephemeral | 11685 | 11780 |
| Scalar Set, persistent | 14342 | 14491 |
| Structured Get, ephemeral | 11895 | 11962 |
| Structured Get, persistent | 11882 | 11953 |
| Structured Set, ephemeral | 12609 | 12682 |
| Structured Set, persistent | 15255 | 15404 |

Four server value-read paths retain their strict zero-allocation budget.
The local entry read retains its 72 B/op budget.
These metrics are bytes allocated per operation, not retained heap size, RSS or GC pause duration.

The stored limits and source run links are in
[wire-alloc-ci-baseline.json](squirix.e2e.benchmarks/wire-alloc-ci-baseline.json) and
[alloc-ci-baseline.json](squirix.server.benchmarks/alloc-ci-baseline.json).
The checker rejects regressions, missing cases and cases with no baseline.
A zero-byte budget permits no allocation.

## Same-runner source comparison

[performance-ci-baseline.json](performance-ci-baseline.json) pins the accepted develop revision to the full immutable commit
e71f8975f5190cde997b9d4a2cd067ae8dc57144.
The [runner](../tools/ci/run-performance-guard.py) creates a temporary checkout of that revision. The reference does not float with the branch.
It rebuilds both versions with the current SDK and identical selected benchmark sources, then runs BenchmarkDotNet
in native child processes: one launch, ten warmup iterations and ten measurement iterations at 500 ms.
Upper outliers are retained. Server and client suites use opposite version order to reduce systematic order bias.

The 53-case suite covers:

- Physical and replicated reads, including the allocated entry control.
- ASCII and Unicode writes at 16, 4096 and 65536 UTF-16 units.
- Get/Set with no metrics listener, a counter, a histogram and both instruments.
- Empty and nonempty idempotency response serialization.
- Put/remove journal frame encoding.
- Small/large replica record encoding and decoding at four payload sizes.
- Public, forwarded and nested invocation scopes.
- Structured JSON entry encoding/decoding at two object sizes.
- Client call policy alone and combined with bootstrap failover.
- Cache decorator chains for synchronous reads, GetOrAdd hits and asynchronously completing Set/TryAdd writes.
- Admission gate acquire/release and single-worker keyed-lock reuse across distinct keys.
- Pooled replica state-transfer parsing, including transport-buffer copying and clearing.
- Loading 100k-entry snapshots containing strings or structured JSON, and scanning 100k journal frames at recovery.
- Key validation at 16, 128 and 1024 characters and repeated cache API lookup by name.

All 53 cases compare allocations with the simultaneously measured reference, with a 5% tolerance.
The 45 cases with meaningful timing compare medians, allowing at most a 20% slowdown.
Eight cases guard allocations only: empty/nonempty response buffers, two asynchronously completing writes,
keyed-lock churn, two snapshot loads and journal recovery scanning.
Their timing depends on measurement resolution, thread scheduling or file-system behavior.
A relative standard error above 5% fails a timing-enabled case as a noisy measurement requiring a rerun.
The guard also fails on missing or duplicate cases, different host/runtime/SDK identities, invalid data,
in-process results and fewer than ten measured samples. Smoke/Dry data cannot satisfy it.

This is a representative hot-path regression suite, not complete coverage of every optimization.
It does not establish whole-server throughput, RPC p99 latency, disk durability latency or multi-node capacity.

## Nightly timing and fast PR checks

The [Nightly Performance workflow](../.github/workflows/performance-nightly.yml) runs at 03:00 Asia/Krasnoyarsk
(20:00 UTC), with an optional manual dispatch. It has no push or pull-request trigger.
The original 39-case comparison took about 26 minutes on Ubuntu.
The additional 14 cases run in a separate server suite; each benchmark command retains its 30-minute timeout.
Allow approximately 40 minutes for the expanded comparison; the workflow has a 60-minute limit.
This estimate will be checked against the first scheduled run.
It does not run on PRs and does not block their CI success.
A nightly regression or invalid/noisy measurement fails that separate workflow and preserves its diagnostics.

The regular PR/push CI retains the fast allocation guard and runs the comparator/runner unit tests, which take a few seconds.
Those fast checks gate CI success. Benchmark Smoke remains a separate execution check with no timing thresholds.
The nightly workflow uploads raw JSON, logs, a summary table and the exact manifest/commit identities.

Run a local comparison from a complete checkout with the pinned reference commit available:

    python tools/ci/run-performance-guard.py benchmarks/performance-ci-baseline.json /path/to/fresh-artifacts

Use a fresh artifact directory. Logs are preserved on failure; only the owned temporary reference checkout is removed.
The runner removes inherited smoke, quick-mode and artifact-path overrides before executing either version.
BenchmarkDotNet generated builds have a 600-second build budget and inherit disabled analyzers from the runner.
Regular CI analyzed-build jobs remain enabled; benchmark measurements, tolerances and sample requirements are unchanged.
Do not run other builds or benchmarks on the measurement host at the same time.

To refresh wire allocation limits, use check-wire-alloc.py with --write and at least three successful CI artifact directories,
then run the checker separately against every input run and retain the source run links.
When an internal API change makes shared benchmark sources incompatible with the pinned reference, refresh the pinned commit to a compatible revision instead of maintaining legacy benchmark adapters. Record optimization results separately before refreshing, because the guard compares only against the new reference.
Refresh the timing reference only as an explicit reviewed change to the pinned source commit.
Review newly selected cases and the baseline diff; a failing guard does not justify automatically raising its limits.
