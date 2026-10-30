# Server phase-budget timer optimization

Date: 2026-10-30. Measured base: `ad7ee8dad58807452c234b25d1c08d43ebaa7e78` (`develop`, including client PR #1602).
This is the owner-forwarding portion of performance finding 37, isolated in `perf/server-timeout-scopes`.
The publication branch was rebased onto `83c73b10f`; the intervening changes affect tests, not server production code.

## Change and invariants

`ServerCallPolicy` captures the incoming monotonic budget once. When the budget and policy use the same
`TimeProvider` instance, one attempt timer covers the minimum of the per-attempt limit and remaining operation
budget. Retry backoff is capped by that remaining budget; the attempt timer is disposed before backoff begins.
Independent clocks retain the operation-wide budget source. A finite budget still bounds an infinite attempt limit.

The per-peer permit remains fail-fast. Caller cancellation retains its precedence. An exhausted budget cannot
start another attempt, including when its timer callback precedes the next monotonic timestamp observation.
Ambiguous-commit, ring-mismatch and ring-fenced responses retain their original terminal exceptions even at
the budget boundary. The former nested attempt wrapper is folded into the timer-owning method so its source
can be disposed before failure mapping waits for backoff.

The commit coordinator, batch-preparation budget, follower timeout reuse, gRPC deadlines and durability protocol
are unchanged. In particular, the commit budget remains independent of a caller that cancels after append.
`InternalOwnerClientInterceptor` currently creates no timer; its name in the old audit was a call-stack context.

## Fresh allocation profiles

Three actual host processes use RF=3, persistence and internode mTLS, with the SDK client in a fourth process.
After a five-second warmup, 48 worker tasks send successful scalar sets with unique keys for 25 seconds.
Each server is sampled for 15 seconds with `gc-verbose,dotnet-sampled-thread-time` via `dotnet-trace`.

The following are allocation-tick samples whose type is `TimerQueueTimer`, grouped by their call stacks across
the three servers. They are not exact timer creation counts. The two sampled windows process different amounts
of work, so the columns must not be interpreted as a normalized allocation reduction for the whole cluster.

| Timer source | Control samples | Candidate samples |
| --- | ---: | ---: |
| policy operation budget | 77 | 0 |
| policy attempt | 74 | 48 |
| gRPC inbound | 165 | 135 |
| gRPC outbound | 69 | 48 |
| batch preparation | 14 | 4 |
| commit coordinator | 23 | 10 |
| other | 16 | 18 |

The separate policy-budget timer disappears on this shared-clock workload. Deterministic tests independently
verify one policy timer rather than two. gRPC deadline timers and the separate commit budgets remain visible.
The historical audit estimate of six timers per operation and 3-5% CPU is not a new measurement from this run.
Sampled thread-stack residency does not establish active CPU percentages or a timer-lock contention percentage.

## Policy microbenchmark

`ServerCallPolicyBudgetBenchmarks` executes an action that suspends with `Task.Yield`, with and without an ambient
budget. The same benchmark assembly and dependencies are used for both variants; only `Squirix.Server.dll` and
its PDB are replaced. BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K. Tiered JIT and PGO
overrides are absent. One in-process job uses three warmups, five measurements and a requested 500 ms iteration time.

| Operation budget | Control allocated | Candidate allocated | Control mean | Candidate mean |
| --- | ---: | ---: | ---: | ---: |
| False | 1.23 KB | 1.14 KB | 1.404 μs | 1.451 μs |
| True | 1.66 KB | 1.27 KB | 1.718 μs | 1.355 μs |

BDN labels these allocation units KB; they are binary KiB. The budget path allocates about 24% less, and the
path without a budget allocates less as well. These are policy costs, not a 24% reduction for an entire RPC.
Asynchronous scheduling can affect wrapper suspension and allocation. The timing improvement in this small
benchmark is not proof of a cluster speedup.

## Unprofiled three-process RF=3 writes

The control host/dependency directory is frozen before editing. The candidate uses the same host and dependencies
with only the server DLL/PDB replaced. Every run starts three fresh host processes and fresh data directories.
Each server has `DOTNET_PROCESSOR_COUNT=4`; all processes share the same physical Windows machine and storage.
No builds or tests overlap measurement. The client uses the same SDK DLL and a loopback development-certificate
handler throughout; internode mTLS still validates the generated test CA and node identities.

Each run warms up for five seconds, then performs successful scalar sets with unique keys for 15 seconds.
The workload is closed loop: 48 or 256 worker tasks share one SDK client and its existing per-peer concurrency
limit. They are not 256 independent SDK connections. The run grows the cache and retained operation records;
this is not a steady-state capacity or saturation study. Warmup, startup and teardown are excluded from latency.
Server CPU is aggregate process CPU time for the three servers divided by completed operations, excluding the client.

| Run order | Workers | Variant | Writes | Throughput (op/s) | p50 (ms) | p99 (ms) | Server CPU (us/op) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 48 | baseline-48-1 | 107320 | 7145 | 4.68 | 30.03 | 662.6 |
| 2 | 48 | candidate-48-1 | 109412 | 7292 | 4.61 | 27.09 | 572.9 |
| 3 | 48 | baseline-48-2 | 131464 | 8762 | 4.61 | 24.07 | 608.4 |
| 4 | 48 | candidate-48-2 | 122261 | 8148 | 4.54 | 26.09 | 579.8 |
| 5 | 256 | baseline-256-1 | 135190 | 9001 | 20.43 | 98.21 | 532.1 |
| 6 | 256 | candidate-256-1 | 117974 | 7853 | 21.23 | 101.09 | 521.2 |
| 7 | 256 | candidate-256-2 | 143449 | 9552 | 20.00 | 95.31 | 548.6 |
| 8 | 256 | baseline-256-2 | 135187 | 8960 | 20.83 | 98.15 | 566.1 |

Throughput moves in both directions. At 256 workers, candidate p99 spans 95-101 ms, around the controls at 98 ms.
CPU/op is lower in the two 48-worker candidate samples, but the 256-worker ranges overlap. With two runs per
variant and load, this does not establish a general throughput, CPU or p99 improvement. The supported benefit
is one fewer policy timer on same-clock forwards and lower measured policy allocation. There is no demonstrated
persistent slowdown in these runs; neither a worst-case latency bound nor general non-regression is established.

## Validation and limits

- New deterministic phase-budget tests: 18 passed.
- Measured-base server unit suite: 4454 passed, zero failed; one macOS-only test skipped on Windows (4455 total).
- Full server integration suite: 322 passed, zero failed or skipped.
- Publication rebase: 4456 server unit tests passed, one macOS-only test skipped; 322 integration tests passed.
- Release builds with analyzers: server, server unit tests, server integration tests, server benchmarks and host.
- On the measured base, the server DLL in both tested suites matched the measured candidate SHA-256.
- Successful scalar writes were benchmarked without injected failures. The runs do not establish crash/partition
  behavior, a read-performance result, a multi-machine SLA, or a full cluster allocation reduction.
- Finding 37 remains partial; other timer sources have not been removed.

## Reproduction and artifacts

Build the server benchmark project in Release and run its assembly with:

```powershell
dotnet benchmarks/squirix.server.benchmarks/bin/Release/net10.0/Squirix.Server.Benchmarks.dll --filter '*ServerCallPolicyBudgetBenchmarks*' --inProcess --warmupCount 3 --iterationCount 5 --iterationTime 500
```

For binary comparisons, freeze one benchmark directory and swap only the server DLL/PDB before each new process.
Use only the in-process job: an additional out-of-process job can rebuild against the current checkout instead
of the swapped DLL. The initial mixed-job exploration is archived as `exploratory-mixed-jobs-*` and excluded here.

Local artifacts are under ignored `artifacts/server37/`: frozen host directories, `manifest.json`, source snapshots,
per-run results/logs, three baseline and three candidate traces, allocation-stack exports and policy BDN reports.
Diagnostic process-runner/analyzer source is archived in `reproduction/`; checkout paths are machine-specific.
The process runner reserves loopback ports, generates disposable test certificates and stops only its own child
processes. Use fresh run labels/data directories when repeating a run.

Measured server DLL SHA-256:

- Control: `571e012a8e4cf7c73d9d5d6799f6895d4e87e96bcb387eb82c67eb81b6052b39`.
- Candidate: `bbbd288f293fe3cdf2f79ad85ff6b24f2985dfee1ae5d8ef23a31fb35739428b`.
- Policy source: `ef8b5da675b2ae84fa398770a7e272a9eb42aa25d077546c2daafe256d527468`.
