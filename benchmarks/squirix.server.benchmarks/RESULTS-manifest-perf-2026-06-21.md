# Manifest publish performance results — 2026-06-21

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.

## Phase 0–3 baseline (pre hot-path, 10 000 ops/invoke)

Method: `ManifestPublishBenchmarks.PublishManifest` — mixed `WriteBlocking` (Json) vs `PublishBlocking` (Binary).

| Backend | Mean/invoke  | ~ns/op     | Alloc/op     | Gen0/1k ops  |
| ------- | ------------ | ---------- | ------------ | ------------ |
| Json    | **3.121 ms** | **312 ns** | **28.24 KB** | 1.60 / 0.10  |
| Binary  | **3.206 ms** | **321 ns** | **15.02 KB** | 0.90 / —     |

**Finding:** alloc −47%, latency ≈ parity. Production default stays **Json**.

---

## Phase 4 post hot-path (`PublishRollBlocking`, fair comparison)

Both backends use `PublishRollBlocking` (production roll path). High retention count (100k) so cleanup is off the hot
path; warmup publish primes in-memory index.

### Micro publish (`SQUIRIX_BENCH_QUICK=1`, 500 ops/invoke)

| Backend | Mean/invoke | ~ms/publish | Alloc/invoke |
| ------- | ----------- | ----------- | ------------ |
| Json    | **2.719 s** | **5.44 ms** | ~880 MB      |
| Binary  | **1.758 s** | **3.52 ms** | ~385 MB      |

**Median ratio (Json/Binary): ~1.55×** latency — Binary faster on isolated publish loop after hot-path work.

Default profile (2 000 ops/invoke, 1 warmup + 2 measured): expect ~5× longer wall time than quick row above.

### Segment roll e2e (`ManifestSegmentRollBenchmarks`, quick: 2 rolls/invoke)

Real `JournalCoordinator` overflow roll + manifest publish + fsync. `JournalMaxSegmentCount=1024` for benchmark host
only.

| Backend | Mean/invoke (2 rolls) | ~ms/roll       |
| ------- | --------------------- | -------------- |
| Json    | **25.66 ms**          | **~12.8 ms**   |
| Binary  | **23.72 ms**          | **~11.9 ms**   |

**Median ratio (Json/Binary): ~1.08×** — manifest backend is a small fraction of total roll time; **does not meet the
1.5× cutover gate** on this benchmark.

---

## Phase 5 binary hot-path (`SQUIRIX_BENCH_QUICK=1`, 500 ops/invoke)

Sync roll publish, cached UTF-8 snapshot path, roll-only encode, directory ensure once, `string.Create` manifest paths.
Json backend unchanged.

### Micro publish (`ManifestPublishBenchmarks.PublishManifest`)

| Backend | Mean/invoke | ~ms/publish | Alloc/invoke |
| ------- | ----------- | ----------- | ------------ |
| Json    | **2.741 s** | **5.48 ms** | ~880 MB      |
| Binary  | **0.821 s** | **1.64 ms** | ~378 MB      |

**Median ratio (Json/Binary): ~3.3×** latency vs Phase 4 Binary (~3.52 ms → ~1.64 ms, **~2.1×** improvement on
isolated publish loop).

### Segment roll e2e (`ManifestSegmentRollBenchmarks`, 2 rolls/invoke)

| Backend | Mean/invoke (2 rolls) | ~ms/roll       |
| ------- | --------------------- | -------------- |
| Json    | **26.31 ms**          | **~13.2 ms**   |
| Binary  | **26.35 ms**          | **~13.2 ms**   |

**Median ratio (Json/Binary): ~1.00×** — manifest publish is still a small slice of total roll time; **cutover gate
still not met** on this benchmark (noise within ~0.2 ms/roll).

---

## Binary roll publish breakdown (`ManifestPublishBreakdownBenchmarks`, quick: 500 ops/invoke)

Isolates binary segment-roll manifest costs on the production roll path (no snapshot payload). Baseline = full
`PublishRollBlocking`.

| Method                         | Mean/invoke | ~µs/op     | Ratio vs full |
| ------------------------------ | ----------- | ---------- | ------------- |
| PublishRollBlocking (baseline) | **804 ms**  | **~1 608** | 1.00          |
| RollEncodeOnly                 | **7.7 µs**  | **~0.015** | ~0            |
| RollDataFileOnly               | **374 ms**  | **~748**   | 0.47          |
| RollPointerOnly                | **369 ms**  | **~738**   | 0.46          |
| RollEncodeAndDataFile          | **376 ms**  | **~751**   | 0.47          |

**Finding:** encode is negligible (~0%); **~47% numbered `.bmqx` fsync + ~46% `man-current` fsync**; the remainder
(~7%) is lock, path build, cache hits, and retention scheduling. Further binary wins require reducing fsync count or
cost (e.g. journal-thread-only persistent pointer handle), not codec work.

---

## Phase 6 binary durability (`P6.1–P6.5`, `SQUIRIX_BENCH_QUICK=1`, 500 ops/invoke)

Persistent `man-current` handle (Linux), `WriteThrough` numbered `.bmqx` on Windows, `Interlocked` index allocation
without `_gate` on roll path, pre-sized data files, Linux io_uring batch roll durability (write + write + fsync + fsync
in one `io_uring_enter` when available).

### Phase 6 micro publish (`ManifestPublishBenchmarks.PublishManifest`)

| Backend | Mean/invoke | ~ms/publish | Alloc/invoke |
| ------- | ----------- | ----------- | ------------ |
| Json    | **2.756 s** | **5.51 ms** | ~880 MB      |
| Binary  | **0.793 s** | **1.59 ms** | ~378 MB      |

**Median ratio (Json/Binary): ~3.5×** (Binary publish ~1.64 → ~1.59 ms/op vs Phase 5).

### Phase 6 segment roll e2e (`ManifestSegmentRollBenchmarks`, 2 rolls/invoke)

| Backend | Mean/invoke (2 rolls) | ~ms/roll       |
| ------- | --------------------- | -------------- |
| Json    | **41.82 ms**          | **~20.9 ms**   |
| Binary  | **32.64 ms**          | **~16.3 ms**   |

**Median ratio (Json/Binary): ~1.28×** on this sample (high variance run-to-run; prior Phase 5 sample was ~1.00×).
Cutover gate **still not met** at 1.5×.

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

## Gate (informal)

| Criterion | Status |
| --------- | ------ |
| Segment-roll e2e Binary ≥ **1.5×** faster than Json (median) | **Not met** (~1.28× Phase 6; ~1.00× Phase 5) |
| Parity tests Json \| Binary | **Green** (30 tests) |
| Retention safety (Binary async) | **Done** (single-flight worker + burst test) |
| Migration doc + `MigrateJsonToBinaryAsync` | **Done** |
| **Cutover default Binary** | **Blocked** — stay on **Json** |

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
