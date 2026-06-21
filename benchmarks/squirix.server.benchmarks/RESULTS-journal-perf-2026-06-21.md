# Journal performance results — 2026-06-21

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.9, Release.  
Logs: `benchmark-*.log` at repo root (`squirix-wal-pipelined/`).

## Strict @ 4096 B (`GroupCommitMaxWait=0`)

| Backend | Mean/op | Alloc/op | Gate (latency ≤ JsonFramed ±10%, alloc ≪ JsonFramed) |
|---------|---------|----------|------------------------------------------------------|
| JsonFramed | **101.9 μs** | **14.8 KB** | baseline |
| Pipelined | **850 ns** | **22 B** | **pass** (latency and alloc) |

Method: `JournalAppendBenchmarks.AppendPutAsync` (separate append + await).  
Pipelined run used BDN adaptive 600k ops/iteration; JsonFramed 100k ops/iteration.

## Group commit @ 8 writers (`GC MaxWait=1 ms`, batch=32)

16k ops/invoke (8×2000). Per-op latency = Mean / 16000.

| Backend | Payload | Mean/invoke | ~μs/op | Alloc/op | vs JsonFramed throughput |
|---------|---------|-------------|--------|----------|--------------------------|
| JsonFramed | 256 B | 1.27 ms | 79 | 2.9 KB | baseline |
| JsonFramed | 4096 B | 1.11 ms | 69 | 15.9 KB | baseline |
| Pipelined | 256 B | 1.93 ms | 121 | 841 B | **0.65× (slower)** |
| Pipelined | 4096 B | 1.60 ms | 100 | 838 B | **0.69× (slower)** |

**Gate Pipelined ≥ 2× throughput: FAIL** on this machine under concurrent GC workload.

Note: Pipelined 256 B failed before fix (`ObjectDisposedException` in `JournalDurabilityGroupCommit.ScheduleDelayFlushAsync` when immediate batch flush races delay timer). Fixed + test `GroupCommitImmediateBatchFlushRacesDelayTimer`.

## Breakdown @ 256 B (`SQUIRIX_BENCH_QUICK=1`, 1k ops/invoke)

### Pipelined

| Method | Mean | Alloc |
|--------|------|-------|
| AppendPutWithDurabilityAsync (baseline) | 793 ns | 4 B |
| EncodeOnly | 5.0 ns | 1 B |
| EnqueueOnlyAsync | 738 ns | 8 B |
| FsyncOnlyAsync | 753 ns | 5 B |

Encode ≈ 0.6% of baseline; enqueue + fsync dominate (~93% each vs baseline — overlap in combined path).

### JsonFramed

| Method | Mean | Alloc |
|--------|------|-------|
| AppendPutWithDurabilityAsync | 886 ns | 22 B |
| EncodeOnly | 5.0 ns | 9 B |
| EnqueueOnlyAsync | 909 ns | 19 B |
| FsyncOnlyAsync | — | (run failed / NA) |

## Next steps

- Investigate GC path: why Pipelined loses to JsonFramed under 8-writer GC despite piggyback.
- `JournalRecord` pool — target remaining ~500 B/op strict alloc.
- Re-run GC bench on Linux / with AV exclusions for stable CI numbers.
