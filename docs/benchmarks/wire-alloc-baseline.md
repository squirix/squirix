# Wire allocation baseline (ICache matrix)

Single-node E2E allocation baselines for every public `ICache<T>` operation on the gRPC wire path.
Use this document to compare `develop` against `refactor/address-wire-alloc` (or any wire-encoding change).

Durability modes:

- **Ephemeral** — in-memory server (no journal/snapshot).
- **Persistence** — `UsePersistence()` with a temp data directory (journal + snapshot stack).

`RemoveAsync` and `RemoveExpirationAsync` under **Persistence** currently fail with a server handler error on `develop`;
persistence table rows for those APIs remain `_pending_` until the durable remove path is fixed.

## Run metadata

| Field      | Value                                                                                                                          |
|------------|--------------------------------------------------------------------------------------------------------------------------------|
| Git SHA    | `ba0738d0`                                                                                                                     |
| Branch     | `feat/wire-alloc-benchmark-baseline`                                                                                           |
| Date (UTC) | 2026-06-30 08:23:42                                                                                                            |
| Command    | `dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*CacheWire*AllocBenchmarks*' --exporters json` |

## Scalar Ephemeral (`string`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-scalar-ephemeral-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |          749 |             13660.00 | 0.0000 |
| GetValueAsync         |          478 |             11113.00 | 0.0000 |
| GetEntryAsync         |          492 |             13028.00 | 0.0000 |
| TryAddAsync           |          789 |             13736.00 | 0.0000 |
| AddAsync              |          759 |             13913.00 | 0.0000 |
| UpdateAsync           |          832 |             13651.00 | 0.0000 |
| RemoveAsync           |          800 |             11846.00 | 0.0000 |
| GetOrAddAsync (hit)   |          804 |             13568.00 | 0.0000 |
| GetOrAddAsync (miss)  |          820 |             14450.00 | 0.0000 |
| GetExpirationAsync    |          467 |             11274.00 | 0.0000 |
| RemoveExpirationAsync |          781 |             11968.00 | 0.0000 |
| TouchAsync (relative) |          760 |             12379.00 | 0.0000 |
| TouchAsync (absolute) |          717 |             12353.00 | 0.0000 |

<!-- wire-alloc-scalar-ephemeral-end -->

## Scalar Persistence (`string`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-scalar-persistence-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |         1173 |             19010.00 | 0.0010 |
| GetValueAsync         |          448 |             11174.00 | 0.0000 |
| GetEntryAsync         |          507 |             13046.00 | 0.0000 |
| TryAddAsync           |         1125 |             19167.00 | 0.0010 |
| AddAsync              |         1124 |             19324.00 | 0.0010 |
| UpdateAsync           |         1139 |             19031.00 | 0.0010 |
| RemoveAsync           |    _pending_ |                      |        |
| GetOrAddAsync (hit)   |          773 |             13576.00 | 0.0000 |
| GetOrAddAsync (miss)  |         1263 |             19840.00 | 0.0010 |
| GetExpirationAsync    |          461 |             11197.00 | 0.0000 |
| RemoveExpirationAsync |    _pending_ |                      |        |
| TouchAsync (relative) |         1016 |             17316.00 | 0.0000 |
| TouchAsync (absolute) |         1130 |             17306.00 | 0.0000 |

<!-- wire-alloc-scalar-persistence-end -->

## Structured Ephemeral (`BenchmarkUserProfile`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-structured-ephemeral-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |          991 |             26702.00 | 0.0010 |
| GetValueAsync         |          672 |             23424.00 | 0.0010 |
| GetEntryAsync         |          607 |             23650.00 | 0.0010 |
| TryAddAsync           |          989 |             26781.00 | 0.0010 |
| AddAsync              |          925 |             26884.00 | 0.0010 |
| UpdateAsync           |         1005 |             26903.00 | 0.0010 |
| RemoveAsync           |          843 |             19409.00 | 0.0000 |
| GetOrAddAsync (hit)   |          967 |             26135.00 | 0.0010 |
| GetOrAddAsync (miss)  |         1151 |             40024.00 | 0.0020 |
| GetExpirationAsync    |          482 |             11107.00 | 0.0000 |
| RemoveExpirationAsync |          787 |             12005.00 | 0.0000 |
| TouchAsync (relative) |          792 |             12384.00 | 0.0000 |
| TouchAsync (absolute) |          740 |             12376.00 | 0.0000 |

<!-- wire-alloc-structured-ephemeral-end -->

## Structured Persistence (`BenchmarkUserProfile`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-structured-persistence-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |         1368 |             33661.00 | 0.0010 |
| GetValueAsync         |          636 |             23421.00 | 0.0010 |
| GetEntryAsync         |          600 |             23704.00 | 0.0010 |
| TryAddAsync           |         1265 |             33609.00 | 0.0010 |
| AddAsync              |         1465 |             33881.00 | 0.0010 |
| UpdateAsync           |         1285 |             33923.00 | 0.0000 |
| RemoveAsync           |    _pending_ |                      |        |
| GetOrAddAsync (hit)   |         1028 |             26154.00 | 0.0010 |
| GetOrAddAsync (miss)  |         1529 |             47045.00 | 0.0020 |
| GetExpirationAsync    |          454 |             11153.00 | 0.0000 |
| RemoveExpirationAsync |    _pending_ |                      |        |
| TouchAsync (relative) |         1132 |             17368.00 | 0.0000 |
| TouchAsync (absolute) |         1196 |             17324.00 | 0.0000 |

<!-- wire-alloc-structured-persistence-end -->

## Update tables

```powershell
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- `
  --filter '*CacheWire*AllocBenchmarks*' `
  --warmupCount 1 `
  --iterationCount 3 `
  --exporters json

./tools/benchmarks/update-wire-alloc-table.ps1 `
  -ArtifactsDir BenchmarkDotNet.Artifacts/results `
  -GitSha (git rev-parse --short HEAD) `
  -Branch (git branch --show-current)
```

Use `--iterationCount 3` (or higher) when exporting JSON. Lower counts can yield empty rows and, with
`StopOnFirstError`, abort the remaining benchmarks.

The script replaces content between the four `<!-- wire-alloc-*-start/end -->` marker pairs in this file.
