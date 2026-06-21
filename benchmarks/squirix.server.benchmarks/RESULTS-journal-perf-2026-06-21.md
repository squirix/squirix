# Journal performance results — 2026-06-21

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.  
Logs: `benchmark-*.log` at repo root (`squirix-wal-pipelined/`).

## Strict @ 4096 B (`GroupCommitMaxWait=0`)

| Backend    | Mean/op      | Alloc/op    | Gate                                             |
|------------|--------------|-------------|--------------------------------------------------|
| JsonFramed | **101.9 μs** | **14.8 KB** | baseline                                         |
| Pipelined  | **850 ns**   | **22 B**    | **pass** (latency and alloc)                     |

Method: `JournalAppendBenchmarks.AppendPutAsync` (separate append + await).

---

## Group commit @ 8 writers — baseline (pre-tune, full 16k ops/invoke)

`JournalGroupCommitBenchmarks`, GC MaxWait=1 ms, batch=32.

| Backend    | Payload | ~μs/op | Alloc/op | vs JsonFramed |
|------------|---------|--------|----------|---------------|
| JsonFramed | 256 B   | 79     | 2.9 KB   | 1.0×          |
| JsonFramed | 4096 B  | 69     | 15.9 KB  | 1.0×          |
| Pipelined  | 256 B   | 121    | 841 B    | **0.65×**     |
| Pipelined  | 4096 B  | 100    | 838 B    | **0.69×**     |

Gate **Pipelined ≥ 2× throughput: FAIL** (raw coordinator append+await path).

---

## Group commit — quick re-run post-tune (`SQUIRIX_BENCH_QUICK=1`)

Scale: **200 ops/writer × 4 writers = 800 ops/invoke**, warmup=0, iteration=1.  
After gc-tune (`11e3bffd`) + allocation passes. Log: `benchmark-gc-quick-post-tune.log`.

### Raw coordinator (`JournalGroupCommitBenchmarks.ConcurrentAppendPutAsync`)

| Backend    | Payload | ms/invoke | ~μs/op (800 ops) | vs JsonFramed |
|------------|---------|-----------|------------------|---------------|
| JsonFramed | 256 B   | 2420      | 3025             | 1.0×          |
| JsonFramed | 4096 B  | 1311      | 1639             | 1.0×          |
| Pipelined  | 256 B   | 2915      | 3644             | **0.83×**     |
| Pipelined  | 4096 B  | 1729      | 2161             | **0.76×**     |

Raw path still slower; 4096 B gap narrowed vs baseline.

### Production path (`DurableMutationGroupCommitBenchmarks.ExecutePutMutationAsync`)

| Backend    | Payload | ms/invoke | ~μs/op (800 ops) | vs JsonFramed |
|------------|---------|-----------|------------------|---------------|
| JsonFramed | 256 B   | 3102      | 3878             | 1.0×          |
| JsonFramed | 4096 B  | 3113      | 3891             | 1.0×          |
| Pipelined  | 256 B   | 1583      | **1978**         | **1.96×**     |
| Pipelined  | 4096 B  | 2651      | 3314             | **1.17×**     |

**Finding:** under `DurableMutationExecutor` group-commit path, Pipelined **wins @ 256 B** (quick sample). Raw coordinator bench understates production throughput.

---

## Breakdown @ 256 B (`SQUIRIX_BENCH_QUICK=1`, 1k ops/invoke)

| Method (Pipelined)              | Mean   | Alloc |
|---------------------------------|--------|-------|
| AppendPutWithDurabilityAsync    | 793 ns | 4 B   |
| EncodeOnly                      | 5 ns   | 1 B   |
| EnqueueOnlyAsync                | 738 ns | 8 B   |
| FsyncOnlyAsync                  | 753 ns | 5 B   |

Encode ≈ 0.6% of baseline; cross-thread + durability dominate.

---

## Next steps

- Full (non-quick) `JournalGroupCommitBenchmarks` + `DurableMutationGroupCommitBenchmarks` on CI/Linux when needed.
- Raw coordinator path: further profiling (append+await double call vs executor single barrier).
- `JournalRecord` pool — strict alloc ~496 B/op.
- Production tuning doc (`MaxWaitMs`, `MaxBatch`).
