# Manifest publish performance results — 2026-06-21

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.

Manifest files use `.bmqx` numbered files and a fixed-size SQMC `man-current` pointer.

## Phase 4 post hot-path (`PublishRollBlocking`, fair comparison)

High retention count (100k) so cleanup is off the hot path; warmup publish primes in-memory index.

### Micro publish (`SQUIRIX_BENCH_QUICK=1`, 500 ops/invoke)

| Mean/invoke | ~ms/publish | Alloc/invoke |
| ----------- | ----------- | ------------ |
| **1.758 s** | **3.52 ms** | ~385 MB      |

Default profile (2 000 ops/invoke, 1 warmup + 2 measured): expect ~5× longer wall time than quick row above.

### Segment roll e2e (`ManifestSegmentRollBenchmarks`, quick: 2 rolls/invoke)

Real `JournalCoordinator` overflow roll + manifest publish + fsync. `JournalMaxSegmentCount=1024` for benchmark host
only.

| Mean/invoke (2 rolls) | ~ms/roll     |
| --------------------- | ------------ |
| **23.72 ms**          | **~11.9 ms** |

---

## Phase 5 hot-path (`SQUIRIX_BENCH_QUICK=1`, 500 ops/invoke)

Sync roll publish, cached UTF-8 snapshot path, roll-only encode, directory ensure once, `string.Create` manifest paths.

### Micro publish (`ManifestPublishBenchmarks.PublishManifest`)

| Mean/invoke | ~ms/publish | Alloc/invoke |
| ----------- | ----------- | ------------ |
| **0.821 s** | **1.64 ms** | ~378 MB      |

**~2.1×** faster than Phase 4 micro publish (~3.52 ms → ~1.64 ms).

### Segment roll e2e (`ManifestSegmentRollBenchmarks`, 2 rolls/invoke)

| Mean/invoke (2 rolls) | ~ms/roll     |
| --------------------- | ------------ |
| **26.35 ms**          | **~13.2 ms** |

Manifest publish is a small slice of total roll time (noise within ~0.2 ms/roll vs Phase 4 on this host).

---

## Roll publish breakdown (`ManifestPublishBreakdownBenchmarks`, quick: 500 ops/invoke)

Isolates segment-roll manifest costs on the production roll path (no snapshot payload). Baseline = full
`PublishRollBlocking`.

| Method                         | Mean/invoke | ~µs/op     | Ratio vs full |
| ------------------------------ | ----------- | ---------- | ------------- |
| PublishRollBlocking (baseline) | **804 ms**  | **~1 608** | 1.00          |
| RollEncodeOnly                 | **7.7 µs**  | **~0.015** | ~0            |
| RollDataFileOnly               | **374 ms**  | **~748**   | 0.47          |
| RollPointerOnly                | **369 ms**  | **~738**   | 0.46          |
| RollEncodeAndDataFile          | **376 ms**  | **~751**   | 0.47          |

**Finding:** encode is negligible (~0%); **~47% numbered `.bmqx` fsync + ~46% `man-current` fsync**; the remainder
(~7%) is lock, path build, cache hits, and retention scheduling. Further wins require reducing fsync count or cost
(e.g. journal-thread-only persistent pointer handle), not codec work.

---

## Phase 6 durability (`P6.1–P6.5`, `SQUIRIX_BENCH_QUICK=1`, 500 ops/invoke)

Persistent `man-current` handle (Linux), `WriteThrough` numbered `.bmqx` on Windows, `Interlocked` index allocation
without `_gate` on roll path, pre-sized data files, Linux io_uring batch roll durability (write + write + fsync + fsync
in one `io_uring_enter` when available).

### Phase 6 micro publish (`ManifestPublishBenchmarks.PublishManifest`)

| Mean/invoke | ~ms/publish | Alloc/invoke |
| ----------- | ----------- | ------------ |
| **0.793 s** | **1.59 ms** | ~378 MB      |

**~3%** faster than Phase 5 micro publish (~1.64 ms → ~1.59 ms/op).

### Phase 6 segment roll e2e (`ManifestSegmentRollBenchmarks`, 2 rolls/invoke)

| Mean/invoke (2 rolls) | ~ms/roll     |
| --------------------- | ------------ |
| **32.64 ms**          | **~16.3 ms** |

High variance run-to-run (prior Phase 5 sample was ~13.2 ms/roll on the same host).

### Roll breakdown (`ManifestPublishBreakdownBenchmarks`)

| Method                         | Mean/invoke | ~µs/op     | Ratio vs full |
| ------------------------------ | ----------- | ---------- | ------------- |
| PublishRollBlocking (baseline) | **797 ms**  | **~1 594** | 1.00          |
| RollEncodeOnly                 | **7.7 µs**  | **~0.015** | ~0            |
| RollDataFileOnly               | **358 ms**  | **~715**   | 0.45          |
| RollPointerOnly                | **374 ms**  | **~748**   | 0.47          |
| RollEncodeAndDataFile          | **351 ms**  | **~701**   | 0.44          |

**Finding:** `WriteThrough` on `.bmqx` (Win) trimmed data-file cost ~5%; pointer still ~47% (Win closes persistent
handle after each write; Linux keeps handle + optional io_uring batch).

---

## Re-run

Quick smoke (`SQUIRIX_BENCH_QUICK=1`, ~1–2 min):

```powershell
$env:SQUIRIX_BENCH_QUICK = "1"
dotnet run -c Release --project benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj -- --filter "*Manifest*"
dotnet run -c Release --project benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj -- --filter "*ManifestPublishBreakdown*"
```

Default local profile (~5–10 min):

```powershell
dotnet run -c Release --project benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj -- --filter "*Manifest*"
```
