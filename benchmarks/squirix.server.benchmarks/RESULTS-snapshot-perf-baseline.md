# Snapshot benchmark baseline — 2026-06-22

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.

Synthetic workload: 1000 cache entries (strings, longs, doubles), quick profile (`SQUIRIX_BENCH_QUICK=1` → 2 snapshot ops per benchmark invoke). JSON backend uses `.ssqx`; binary uses `.bsqx`.

Run:

```bash
SQUIRIX_BENCH_QUICK=1 dotnet run -c Release --project benchmarks/squirix.server.benchmarks -- --filter '*Snapshot*'
```

## Write (`SnapshotWriteBenchmarks.WriteSnapshotAsync`)

| Backend | BackendValue | Mean/invoke (2 writes) | ~ms/write | Alloc/invoke |
|---------|--------------|------------------------|-----------|-------------|
| JSON    | 0            | 9.90 ms                | ~4.95 ms  | 1909 KB     |
| Binary  | 1            | 3.42 ms                | ~1.71 ms  | 530 KB      |

Binary write is **~2.9×** faster and allocates **~3.6×** less than JSON on this host.

## Read (`SnapshotReadBenchmarks.LoadStrictAsync`)

| Backend | BackendValue | Mean/load | Alloc/load |
|---------|--------------|-----------|------------|
| JSON    | 0            | 1.21 ms   | 878 KB     |
| Binary  | 1            | 343 µs    | 400 KB     |

Binary read is **~3.5×** faster and allocates **~2.2×** less than JSON.

## Binary write breakdown (`SnapshotWriteBreakdownBenchmarks`, 2 ops/invoke)

Isolates encode (no I/O), temp-file write + flush (no rename), and full publish (production path). Baseline = `PublishSnapshot`.

| Method            | Mean/invoke | ~ms/op | Ratio vs publish | Alloc/op |
|-------------------|-------------|--------|------------------|----------|
| PublishSnapshot   | 8.13 ms     | ~4.07  | 1.00             | 529 KB   |
| WriteTempFileOnly | 1.58 ms     | ~0.79  | 0.20             | 297 KB   |
| EncodeOnly        | 200 µs      | ~0.10  | 0.02             | 26 KB    |

**Finding:** encode + incremental CRC is **~2%** of publish time; **~20%** is temp-file I/O; the remaining **~78%** is publish overhead (rename, retention, directory sync, and per-invoke allocations outside encode/write). Further wins likely need reducing publish-path allocations or fsync/rename cost, not codec work.

## Parity

`SnapshotBackendParityTests` asserts JSON and binary backends produce equivalent recovered state for the same logical entries.
