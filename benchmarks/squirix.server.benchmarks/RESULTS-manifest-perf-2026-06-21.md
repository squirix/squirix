# Manifest publish performance results — 2026-06-21

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.

## Phase 0–3 baseline (pre hot-path, 10 000 ops/invoke)

Method: `ManifestPublishBenchmarks.PublishManifest` — mixed `WriteBlocking` (Json) vs `PublishBlocking` (Binary).

| Backend | Mean/invoke | ~ns/op | Alloc/op | Gen0/1k ops |
|---------|-------------|--------|----------|---------------|
| Json    | **3.121 ms** | **312 ns** | **28.24 KB** | 1.60 / 0.10 |
| Binary  | **3.206 ms** | **321 ns** | **15.02 KB** | 0.90 / — |

**Finding:** alloc −47%, latency ≈ parity. Production default stays **Json**.

---

## Phase 4 post hot-path (`PublishRollBlocking`, fair comparison)

Both backends use `PublishRollBlocking` (production roll path). High retention count (100k) so cleanup is off the hot path; warmup publish primes in-memory index.

### Micro publish (`SQUIRIX_BENCH_QUICK=1`, 500 ops/invoke)

| Backend | Mean/invoke | ~ms/publish | Alloc/invoke |
|---------|-------------|-------------|--------------|
| Json    | **2.719 s** | **5.44 ms** | ~880 MB |
| Binary  | **1.758 s** | **3.52 ms** | ~385 MB |

**Median ratio (Json/Binary): ~1.55×** latency — Binary faster on isolated publish loop after hot-path work.

Default profile (2 000 ops/invoke, 1 warmup + 2 measured): expect ~5× longer wall time than quick row above.

### Segment roll e2e (`ManifestSegmentRollBenchmarks`, quick: 2 rolls/invoke)

Real `JournalCoordinator` overflow roll + manifest publish + fsync. `JournalMaxSegmentCount=1024` for benchmark host only.

| Backend | Mean/invoke (2 rolls) | ~ms/roll |
|---------|----------------------|----------|
| Json    | **25.66 ms** | **~12.8 ms** |
| Binary  | **23.72 ms** | **~11.9 ms** |

**Median ratio (Json/Binary): ~1.08×** — manifest backend is a small fraction of total roll time; **does not meet the 1.5× cutover gate** on this benchmark.

---

## Gate (informal)

| Criterion | Status |
|-----------|--------|
| Segment-roll e2e Binary ≥ **1.5×** faster than Json (median) | **Not met** (~1.08×) |
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
```

Default local profile (~5–10 min):

```powershell
dotnet run -c Release --project benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj -- --filter "*Manifest*"
```
