# Client phase-budget slowdown investigation

Date: 2026-10-30. Measured base: `c151a3654d28396e1504d876741c8f8c2e57d933` (`develop`, including PR #1587).
The publication branch was subsequently rebased onto `930c5d15e`; intervening changes do not modify client production code.
The original phase-budget implementation is retained. The experimental alternatives below were not selected:
none established a causal fix for the earlier in-process latency excursions. No new latency workaround is shipped.

## Controlled in-process comparison

One freshly built benchmark/server/dependency directory was copied to an ignored harness. Only `Squirix.dll` and
its PDB were replaced between processes; each variant was rebuilt without incremental compilation, and the source
was restored after experimental builds. Normal tiered compilation and PGO were retained. No builds or tests overlapped measurement.
All runs use 8 warmups, 10 measurement iterations and a requested 750 ms iteration time. Allocation includes the in-process server.

| Variant / run | Ephemeral mean (us) | Persistence mean (us) | Ephemeral allocation (KiB) |
| --- | ---: | ---: | ---: |
| control-1 | 64.80 | 78.35 | 10.62 |
| prototype-1 | 61.72 | 62.20 | 10.32 |
| root-timer-1 | 71.25 | 74.99 | 10.61 |
| simple-gate-1 | 79.43 | 76.86 | 10.32 |
| pinned-control-1 | 82.63 | 54.72 | 10.57 |
| pinned-prototype-1 | 57.61 | 60.69 | 10.26 |
| prototype-2 | 54.24 | 70.49 | 10.27 |
| control-2 | 44.72 | 55.31 | 10.57 |
| control-3 | 48.04 | 58.47 | 10.56 |
| prototype-3 | 140.15 | 65.09 | 10.38 |
| control-4 | 77.32 | 46.26 | 10.61 |
| typed-catches-1 | 55.26 | 56.12 | 10.30 |
| typed-catches-2 | 47.81 | 48.79 | 10.27 |
| prototype-4 | 48.02 | 63.18 | 10.27 |
| typed-catches-3 | 45.74 | 44.70 | 10.27 |

`root-timer` restores the operation-wide timer; `simple-gate` restores the original semaphore wait (a laboratory-only
variant that does not implement the new queued-budget protection); `typed-catches` replaces the exception filter
and switch with separate typed catches while disposing the attempt timer before backoff. None is the retained source.
The pinned runs inherit an affinity mask for logical CPUs 0, 1, 10, 11, 12, 13, 22, 23, reported by Windows as the higher efficiency class.
This did not stabilize timing, and no affinity or power settings are changed in the library or system.

Both the unchanged control and candidate vary substantially. The candidate has a slow 140 us run, but later the
same DLL runs at 48 us between typed-catch runs at 48 and 46 us. This does not establish that the filter causes
the slowdown. It also does not prove the old slow run harmless or establish a worst-case latency bound.

## Sampled thread profiles

`dotnet-trace collect --profile dotnet-sampled-thread-time --format Speedscope` was run for the control and candidate.
The trace files include startup, seeding, benchmark GC and cleanup. Thread-stack residency is dominated by waits
and includes `PollGC`; these are not measurements of active CPU consumption or GC pause durations per request.
The profiles did not isolate a candidate-specific hot path. Profiled timings are excluded from the table above.

## One unchanged server in a separate process

A diagnostic probe uses the same testkit server factory and public SDK. The ephemeral server process keeps the
baseline DLL throughout each balanced sequence; only the separately launched client DLL changes. Each client
seeds 512 scalar keys, warms up for 8 seconds, then reads 81,920 values over loopback, validating every value.
There are no forced GCs during measurement. This is a custom probe, not a BenchmarkDotNet result.

Allocated bytes and CPU are for the entire client process, including its background tasks and the identical probe
overhead. Server allocation and CPU are excluded. Sequential mean is elapsed wall time divided by count;
concurrent mean and p99 are measured request durations. These are closed-loop reads, not an open-loop SLA test.

| Concurrency | Variant / order | Mean request (us) | p99 (us) | Client B/op | Client CPU us/op | Throughput (op/s) |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline-1 | 92.19 | 209.5 | 7754.0 | 136.38 | 10847 |
| 1 | prototype-2 | 85.32 | 142.9 | 7450.2 | 131.80 | 11721 |
| 1 | typed-catches-3 | 83.13 | 128.6 | 7446.5 | 124.74 | 12030 |
| 1 | typed-catches-4 | 84.38 | 130.9 | 7448.3 | 115.20 | 11851 |
| 1 | prototype-5 | 83.11 | 136.8 | 7447.3 | 132.94 | 12033 |
| 1 | baseline-6 | 106.59 | 649.0 | 7762.4 | 104.14 | 9382 |
| 32 | baseline-1 | 165.97 | 1431.3 | 7644.5 | 32.23 | 191840 |
| 32 | prototype-2 | 156.92 | 1392.5 | 7339.6 | 33.38 | 203010 |
| 32 | prototype-3 | 161.01 | 1455.1 | 7337.9 | 36.62 | 197449 |
| 32 | baseline-4 | 161.58 | 1467.4 | 7641.4 | 38.53 | 197101 |
| 128 | baseline-1 | 497.62 | 2534.1 | 8188.8 | 32.04 | 256136 |
| 128 | prototype-2 | 502.39 | 2491.4 | 8192.3 | 32.04 | 253674 |
| 128 | prototype-3 | 496.38 | 2524.0 | 8187.8 | 35.29 | 256753 |
| 128 | baseline-4 | 503.95 | 2432.8 | 8187.6 | 32.23 | 252955 |

At concurrency 1 and 32, the retained candidate saves approximately 304 B per client read (about 4%). At 128,
where the per-peer limit is 64 and calls queue, allocations and throughput are effectively unchanged: queued calls
retain their operation-wide timer by design. The candidate does not show the earlier sustained slowdown in these
separate-process runs. CPU varies in both directions; no CPU saving or guaranteed speedup is established.

The earlier latency excursions are confined to the evidence from the colocated benchmark so far. Process separation
changes GC, scheduling and compilation interactions together; the experiment cannot name one of them as the cause.
It is therefore incorrect to report a proven timer/JIT bug or a latency fix. A repeatable isolated regression would
still require investigation. The result supports the allocation optimization in these measured read scenarios only.

## Validation and remaining scope

- Measured base: 183 client unit tests and 1 client integration test passed; zero failed or skipped.
- Publication rebase: 182 unit tests and 1 integration test passed; the new base removed one architecture test.
- Release builds with analyzers: unit tests, integration tests, internal benchmarks and end-to-end benchmarks.
- Source normalized SHA-256: `048c2b4a0637c4b96bc2bbb169afb515792a69b9449bc5e469aa76cd3ae66712`.
- Experimental alternatives are excluded; the measured phase-budget implementation is retained.
- No crashes, partitions, writes, replicated cluster CPU profile or distributed-machine latency were tested here.
- Finding 37 remains partial: the gRPC/server and replication timer sources are unchanged.

## Reproduction artifacts

All investigation artifacts are under the ignored `artifacts/perf37-investigation/` directory:
`manifest.json`, variant DLLs and source snapshots, `run-*.log`, per-run BenchmarkDotNet reports,
`trace-control.nettrace`, `trace-prototype.nettrace`, their Speedscope exports, and `process*-results.json`.
The diagnostic source and orchestration are archived in `reproduction/`. Their checkout paths are machine-specific.

For colocated reads, use the command in [the original report](client-phase-budget-notes.md#reproduction-and-artifacts).
For the process probe, build `reproduction/perf37-probe/Probe.csproj` in Release, then run
`python reproduction/perf37-process.py`, followed sequentially by the same command with `32` and `128`.
The script holds one baseline server per sequence, launches clients in balanced order and stops the server afterwards.
