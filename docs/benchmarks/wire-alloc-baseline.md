# Wire allocation baseline (ICache matrix)

Single-node E2E allocation baselines for every public `ICache<T>` operation on the gRPC wire path.
Use this document to compare `develop` against `refactor/address-wire-alloc` (or any wire-encoding change).

## Run metadata

| Field      | Value                                                                                                                          |
|------------|--------------------------------------------------------------------------------------------------------------------------------|
| Git SHA    | _pending_                                                                                                                      |
| Branch     | _pending_                                                                                                                      |
| Date (UTC) | _pending_                                                                                                                      |
| Command    | `dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*CacheWire*AllocBenchmarks*' --exporters json` |

## Scalar (`string`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-scalar-start -->
| ICache API | Mean (ns/op) | Allocated (bytes/op) | Gen0 |
|------------|-------------:|---------------------:|-----:|
| SetAsync | 815 | 13684.00 | 0.0000 |
| GetValueAsync | 427 | 11023.00 | 0.0000 |
| GetEntryAsync | 511 | 13013.00 | 0.0000 |
| TryAddAsync | 743 | 13766.00 | 0.0000 |
| AddAsync | 782 | 13823.00 | 0.0000 |
| UpdateAsync | 774 | 13715.00 | 0.0000 |
| RemoveAsync | 714 | 11967.00 | 0.0000 |
| GetOrAddAsync (hit) | 813 | 13563.00 | 0.0000 |
| GetOrAddAsync (miss) | 817 | 15901.00 | 0.0000 |
| GetExpirationAsync | 429 | 11239.00 | 0.0000 |
| RemoveExpirationAsync | 724 | 11982.00 | 0.0000 |
| TouchAsync (relative) | 761 | 12321.00 | 0.0000 |
| TouchAsync (absolute) | 798 | 12347.00 | 0.0000 |
<!-- wire-alloc-scalar-end -->

## Structured (`BenchmarkUserProfile`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-structured-start -->
| ICache API | Mean (ns/op) | Allocated (bytes/op) | Gen0 |
|------------|-------------:|---------------------:|-----:|
| SetAsync | 1047 | 28056.00 | 0.0010 |
| GetValueAsync | 598 | 23413.00 | 0.0010 |
| GetEntryAsync | 718 | 23591.00 | 0.0010 |
| TryAddAsync | 1067 | 26783.00 | 0.0010 |
| AddAsync | 1075 | 26915.00 | 0.0010 |
| UpdateAsync | 1025 | 26909.00 | 0.0010 |
| RemoveAsync | 853 | 18181.00 | 0.0000 |
| GetOrAddAsync (hit) | 970 | 26135.00 | 0.0010 |
| GetOrAddAsync (miss) | 1246 | 39972.00 | 0.0020 |
| GetExpirationAsync | 478 | 11215.00 | 0.0000 |
| RemoveExpirationAsync | 752 | 11997.00 | 0.0000 |
| TouchAsync (relative) | 827 | 12368.00 | 0.0000 |
| TouchAsync (absolute) | 735 | 12385.00 | 0.0000 |
<!-- wire-alloc-structured-end -->

## Update tables

```powershell
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- `
  --filter '*CacheWire*AllocBenchmarks*' `
  --exporters json

./tools/benchmarks/update-wire-alloc-table.ps1 `
  -ArtifactsDir BenchmarkDotNet.Artifacts/results `
  -GitSha (git rev-parse --short HEAD) `
  -Branch (git branch --show-current)
```

Use `--iterationCount 3` (or higher) when exporting JSON. Lower counts can yield empty rows and, with
`StopOnFirstError`, abort the remaining benchmarks.

The script replaces content between `<!-- wire-alloc-scalar-start/end -->` and
`<!-- wire-alloc-structured-start/end -->` markers in this file.
