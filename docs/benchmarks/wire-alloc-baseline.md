# Wire allocation baseline (ICache matrix)

Single-node E2E allocation baselines for every public `ICache<T>` operation on the gRPC wire path.
Use this document to compare `develop` against `refactor/address-wire-alloc` (or any wire-encoding change).

Durability modes:

- **Ephemeral** — in-memory server (no journal/snapshot).
- **Persistence** — `UsePersistence()` with a temp data directory (journal + snapshot stack).

## Run metadata

| Field      | Value                                                                                                                                  |
|------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Git SHA    | `714b99a1`                                                                                                                             |
| Branch     | `feat/wire-alloc-benchmark-baseline`                                                                                                   |
| Date (UTC) | 2026-06-30 09:56:13                                                                                                                    |
| Command    | `dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*Wire*AllocBenchmarks*' --exporters json`              |

## Scalar Ephemeral (`string`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-scalar-ephemeral-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |          833 |             13535.00 | 0.0000 |
| GetValueAsync         |          458 |             11153.00 | 0.0000 |
| GetEntryAsync         |          484 |             12973.00 | 0.0000 |
| TryAddAsync           |          784 |             13716.00 | 0.0000 |
| AddAsync              |          782 |             15343.00 | 0.0000 |
| UpdateAsync           |          716 |             13604.00 | 0.0000 |
| RemoveAsync           |          690 |             11896.00 | 0.0000 |
| GetOrAddAsync (hit)   |          752 |             13558.00 | 0.0000 |
| GetOrAddAsync (miss)  |          838 |             14415.00 | 0.0000 |
| GetExpirationAsync    |          473 |             11173.00 | 0.0000 |
| RemoveExpirationAsync |          765 |             11884.00 | 0.0000 |
| TouchAsync (relative) |          767 |             12336.00 | 0.0000 |
| TouchAsync (absolute) |          769 |             12330.00 | 0.0000 |

<!-- wire-alloc-scalar-ephemeral-end -->

## Scalar Persistence (`string`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-scalar-persistence-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |         1173 |             19014.00 | 0.0010 |
| GetValueAsync         |          461 |             11174.00 | 0.0000 |
| GetEntryAsync         |          505 |             12972.00 | 0.0000 |
| TryAddAsync           |         1153 |             19098.00 | 0.0010 |
| AddAsync              |         1130 |             19233.00 | 0.0010 |
| UpdateAsync           |         1063 |             19129.00 | 0.0010 |
| RemoveAsync           |         1101 |             17109.00 | 0.0000 |
| GetOrAddAsync (hit)   |          754 |             13538.00 | 0.0000 |
| GetOrAddAsync (miss)  |         1103 |             19834.00 | 0.0010 |
| GetExpirationAsync    |          468 |             11182.00 | 0.0000 |
| RemoveExpirationAsync |         1063 |             18063.00 | 0.0000 |
| TouchAsync (relative) |         1130 |             17241.00 | 0.0000 |
| TouchAsync (absolute) |         1040 |             17308.00 | 0.0000 |

<!-- wire-alloc-scalar-persistence-end -->

## Structured Ephemeral (`BenchmarkUserProfile`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-structured-ephemeral-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |         1001 |             26648.00 | 0.0010 |
| GetValueAsync         |          622 |             23422.00 | 0.0010 |
| GetEntryAsync         |          628 |             23668.00 | 0.0010 |
| TryAddAsync           |          996 |             26666.00 | 0.0010 |
| AddAsync              |         1017 |             26882.00 | 0.0010 |
| UpdateAsync           |         1028 |             26900.00 | 0.0010 |
| RemoveAsync           |          866 |             18154.00 | 0.0000 |
| GetOrAddAsync (hit)   |          999 |             26050.00 | 0.0010 |
| GetOrAddAsync (miss)  |         1182 |             40026.00 | 0.0020 |
| GetExpirationAsync    |          482 |             11099.00 | 0.0000 |
| RemoveExpirationAsync |          787 |             11952.00 | 0.0000 |
| TouchAsync (relative) |          737 |             12363.00 | 0.0000 |
| TouchAsync (absolute) |          730 |             12352.00 | 0.0000 |

<!-- wire-alloc-structured-ephemeral-end -->

## Structured Persistence (`BenchmarkUserProfile`)

Per-operation values (`Batch = 512` per benchmark invocation).

<!-- wire-alloc-structured-persistence-start -->

| ICache API            | Mean (ns/op) | Allocated (bytes/op) |   Gen0 |
|-----------------------|-------------:|---------------------:|-------:|
| SetAsync              |         1351 |             33517.00 | 0.0010 |
| GetValueAsync         |          641 |             23405.00 | 0.0010 |
| GetEntryAsync         |          589 |             23688.00 | 0.0010 |
| TryAddAsync           |         1226 |             33617.00 | 0.0010 |
| AddAsync              |         1305 |             35519.00 | 0.0000 |
| UpdateAsync           |         1300 |             34554.00 | 0.0000 |
| RemoveAsync           |         1202 |             23380.00 | 0.0000 |
| GetOrAddAsync (hit)   |          996 |             26114.00 | 0.0010 |
| GetOrAddAsync (miss)  |         1453 |             47055.00 | 0.0020 |
| GetExpirationAsync    |          479 |             11163.00 | 0.0000 |
| RemoveExpirationAsync |         1142 |             16799.00 | 0.0000 |
| TouchAsync (relative) |         1076 |             17355.00 | 0.0000 |
| TouchAsync (absolute) |         1105 |             17313.00 | 0.0000 |

<!-- wire-alloc-structured-persistence-end -->

## Update tables

```powershell
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- `
  --filter '*Wire*AllocBenchmarks*' `
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
