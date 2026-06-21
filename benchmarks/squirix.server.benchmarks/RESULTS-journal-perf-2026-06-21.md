# Journal performance results — 2026-06-21

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.

> **Note:** JsonFramed **write** backend removed in `8d2664c5`. Only `JournalBackend.Pipelined` remains.
> Tables marked *historical* compare against the old JsonFramed writer before removal.

## Strict @ 4096 B (`GroupCommitMaxWait=0`) — historical A/B

| Backend (write) | Mean/op      | Alloc/op    | Gate                         |
|-----------------|--------------|-------------|------------------------------|
| JsonFramed      | **101.9 μs** | **14.8 KB** | baseline (removed)           |
| Pipelined       | **850 ns**   | **22 B**    | **pass** (latency and alloc) |

Method: `JournalAppendBenchmarks.AppendPutAsync` (separate append + await).

---

## Group commit @ 8 writers — baseline (pre-tune, full 16k ops/invoke) — historical

`JournalGroupCommitBenchmarks`, GC MaxWait=1 ms, batch=32.

| Backend (write) | Payload | ~μs/op | Alloc/op | vs JsonFramed |
|-----------------|---------|--------|----------|---------------|
| JsonFramed      | 256 B   | 79     | 2.9 KB   | 1.0×          |
| JsonFramed      | 4096 B  | 69     | 15.9 KB  | 1.0×          |
| Pipelined       | 256 B   | 121    | 841 B    | **0.65×**     |
| Pipelined       | 4096 B  | 100    | 838 B    | **0.69×**     |

Gate **Pipelined ≥ 2× throughput: FAIL** (raw coordinator append+await path).

---

## Group commit — quick re-run post-tune (`SQUIRIX_BENCH_QUICK=1`) — historical

Scale: **200 ops/writer × 4 writers = 800 ops/invoke**, warmup=0, iteration=1.
After gc-tune (`11e3bffd`) + allocation passes.

### Raw coordinator (`JournalGroupCommitBenchmarks.ConcurrentAppendPutAsync`)

| Backend (write) | Payload | ms/invoke | ~μs/op (800 ops) | vs JsonFramed |
|-----------------|---------|-----------|------------------|---------------|
| JsonFramed      | 256 B   | 2420      | 3025             | 1.0×          |
| JsonFramed      | 4096 B  | 1311      | 1639             | 1.0×          |
| Pipelined       | 256 B   | 2915      | 3644             | **0.83×**     |
| Pipelined       | 4096 B  | 1729      | 2161             | **0.76×**     |

Raw path still slower; 4096 B gap narrowed vs baseline.

### Production path (`DurableMutationGroupCommitBenchmarks.ExecutePutMutationAsync`)

| Backend (write) | Payload | ms/invoke | ~μs/op (800 ops) | vs JsonFramed |
|-----------------|---------|-----------|------------------|---------------|
| JsonFramed      | 256 B   | 3102      | 3878             | 1.0×          |
| JsonFramed      | 4096 B  | 3113      | 3891             | 1.0×          |
| Pipelined       | 256 B   | 1583      | **1978**         | **1.96×**     |
| Pipelined       | 4096 B  | 2651      | 3314             | **1.17×**     |

**Finding:** under `DurableMutationExecutor` group-commit path, Pipelined **wins @ 256 B** (quick sample). Raw coordinator bench understates production throughput.

---

## Breakdown @ 256 B (`SQUIRIX_BENCH_QUICK=1`, 1k ops/invoke) — current Pipelined

Post–JournalRecord pool (`35bc0840`):

| Method (Pipelined)              | Mean     | Alloc |
|---------------------------------|----------|-------|
| AppendPutWithDurabilityAsync    | 611 ns   | **3 B** |
| EncodeOnly                      | 4 ns     | 1 B   |
| EnqueueOnlyAsync                | 727 ns   | 7 B   |
| FsyncOnlyAsync                  | 765 ns   | 4 B   |

Pre-pool baseline: AppendPutWithDurabilityAsync **793 ns / 496 B** (strict gate).

Encode ≈ 0.7% of baseline; cross-thread + durability dominate.

---

## Next steps

- Full (non-quick) group-commit benchmarks on CI/Linux when needed (Pipelined only).
- Raw coordinator path: further profiling (append+await double call vs executor single barrier).
- `io_uring` — real Linux implementation (currently delegates to `RandomAccessJournalSegmentWriter`).
