# Snapshot benchmark baseline — 2026-06-21 (post P8 re-run)

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.

Synthetic workload: strings, longs, doubles entry values. JSON backend uses `.ssqx`; binary uses `.bsqx`.

```bash
# Quick (1k entries, 2 ops/invoke write/breakdown)
$env:SQUIRIX_BENCH_QUICK = "1"
dotnet run -c Release --project benchmarks/squirix.server.benchmarks -- --filter '*Snapshot*'

# Full gate profile (10k entries, 4 ops/invoke write)
dotnet run -c Release --project benchmarks/squirix.server.benchmarks -- --filter '*SnapshotRead*'
dotnet run -c Release --project benchmarks/squirix.server.benchmarks -- --filter '*SnapshotWrite*'
dotnet run -c Release --project benchmarks/squirix.server.benchmarks -- --filter '*SnapshotWriteBreakdown*'
```

---

## Quick profile (1k entries, `SQUIRIX_BENCH_QUICK=1`)

### Write (`SnapshotWriteBenchmarks.WriteSnapshotAsync`, 2 writes/invoke)

| Backend | Mean/invoke | ~ms/write | Alloc/invoke |
|---------|-------------|-----------|--------------|
| JSON | 6.93 ms | ~3.5 ms | 1909 KB |
| Binary (P6) | 3.08 ms | ~1.5 ms | 300 KB |

Binary **~2.3×** faster, **~6.4×** less alloc.

### Read (`SnapshotReadBenchmarks.LoadStrictAsync`)

| Backend | Mean/load | Alloc/load | Notes |
|---------|-----------|------------|-------|
| JSON | 1.59 ms | 878 KB | baseline |
| Binary **P5b** | **420 µs** | **335 KB** | sync `IEnumerator`, reused scratch |

Binary **~3.8×** faster, **~2.6×** less alloc vs JSON.

### Write breakdown (`SnapshotWriteBreakdownBenchmarks`, 2 ops/invoke)

Baseline = `PublishSnapshot` (binary temp write + rename; **no** manifest I/O).

| Method | Mean/invoke | ~ms/op | Ratio | Alloc/op |
|--------|-------------|--------|-------|----------|
| PublishSnapshot | 6.93 ms | ~3.47 | 1.00 | 300 KB |
| WriteTempFileOnly | 1.83 ms | ~0.91 | 0.26 | 298 KB |
| EncodeOnly | 202 µs | ~0.10 | 0.03 | 26 KB |
| **ManifestWriteOnly** | **5.02 ms** | **~2.51** | **0.72** | 1831 KB |

**Findings (1k):**
- Encode **~3%** of snapshot file publish; temp-file I/O **~26%**; rename/publish overhead **~74%** of `PublishSnapshot`.
- **`ManifestWriteOnly`** is **~72%** of snapshot-file publish time — dominates end-to-end coordinator cost when added to snapshot write.

---

## Full profile (10k entries, gate for P9)

### Write (`SnapshotWriteBenchmarks`, 4 writes/invoke)

| Backend | Mean/invoke | ~ms/write | Alloc/invoke |
|---------|-------------|-----------|--------------|
| JSON | 189.2 ms | ~47.3 ms | 35.6 MB |
| Binary | 34.6 ms | ~8.7 ms | 3.52 MB |

Binary **~5.5×** faster, **~10×** less alloc. **Gate: write ≥3× — pass.**

### Read (`SnapshotReadBenchmarks`, single load)

| Backend | Mean/load | Alloc/load |
|---------|-----------|------------|
| JSON | 15.22 ms | 8.29 MB |
| Binary (P5b) | 3.39 ms | 3.71 MB |

Binary **~4.5×** faster; alloc **~2.2×** less than JSON (dominated by materialized entries). **Gate: read ≥3× — pass.**

### Write breakdown (`SnapshotWriteBreakdownBenchmarks`, 4 ops/invoke, 10k)

| Method | Mean/invoke | ~ms/op | Ratio | Alloc/op |
|--------|-------------|--------|-------|----------|
| PublishSnapshot | 13.85 ms | ~3.46 | 1.00 | 1801 KB |
| WriteTempFileOnly | 8.34 ms | ~2.08 | 0.60 | ~1798 KB |
| EncodeOnly | 2.28 ms | ~0.57 | 0.16 | 260 KB |
| ManifestWriteOnly | 5.08 ms | ~1.27 | 0.37 | 1769 KB |

At 10k, manifest slice is **~37%** of binary snapshot-file publish (vs **~72%** at 1k — manifest cost scales sub-linearly with entry count).

---

## Parity

`SnapshotBackendParityTests` — JSON and binary backends produce equivalent recovered state.

## Phase summary

| Phase | Read 1k | Write 1k | Notes |
|-------|---------|----------|-------|
| P5 stream | 446 KB | — | +alloc regression |
| P5b sync | **335 KB** | — | fixed read path |
| P6 | — | ~300 KB | durability + coordinator + metrics |
| P7 | — | breakdown + 10k gate | `ManifestWriteOnly` isolates manifest slice |
| P8 | — | — | retention `.bsqx`; no perf change expected |

## Gate verdict (2026-06-21 re-run)

| Metric | Target | Result |
|--------|--------|--------|
| Write @ 10k | ≥3× vs JSON | **5.5×** pass |
| Read @ 10k | ≥3× vs JSON | **4.5×** pass |

Ready for **P9** (default Binary, remove JSON backend).
