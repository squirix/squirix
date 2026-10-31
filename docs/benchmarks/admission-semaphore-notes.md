# Semaphore fast path investigation

Date: 2026-10-30. Measured control: `17af9e171dce4448307c3d56e7c7e00b5dda99df` (`develop`, including PR #1607).
Candidate: `perf/admission-semaphore`. This investigates performance finding 50.
Publication was rebased onto `d2921900b` after measurement; the semaphore inherited the upstream attribute
namespace relocation. Performance tables and hashes below identify the pre-rebase measurement snapshot,
not new measurements of the publication base. Validation of the rebased branch is listed separately.

## Change and invariants

`AsyncSemaphore` previously took its queue lock for every acquire and return. The candidate uses one atomic
64-bit state containing the available count, queue flag and disposal flag. Successful operations with no
queued waiters use compare-and-exchange without entering the queue lock. Both `AdmissionGate` and
`ReplicaMutationGate` use this primitive; their limits and public behavior are preserved.

The FIFO queue remains protected by `Lock`. Before enqueueing the first waiter, the slow path sets the queue
flag in the same atomic state and rechecks the available count. This closes the race with a simultaneous
return: either the waiter consumes the returned permit, or the returning thread performs a locked handoff.
A fast acquire cannot consume a permit while the queue flag is set. Removing the last waiter reopens the
fast path; all count updates remain atomic even in the locked path. This matters when fast returns resume
before the thread holding the queue lock has left it.

Disposal publishes its flag in the same state before draining the queue. A later CAS cannot overwrite the
flag. Queued waiters still complete through exactly one handoff, cancellation or disposal, and held permits
may be returned after disposal. Cancellation unregisters without waiting for an executing callback.
The full positive `int` capacity remains supported. `AdmissionGate._inFlight` remains: it counts outstanding
leases and checks over-release, rather than duplicating the available count including reserved handoffs.

## Profile before changing the lock

Three actual host processes run RF=3 with persistence and internode mTLS; a fourth process runs the SDK.
After five seconds of warmup, successful scalar writes with unique keys run for 25 seconds. Each server is
traced for 15 seconds with `dotnet-sampled-thread-time` and
`Microsoft-Windows-DotNETRuntime:0x4000:5`. The parser pairs contention starts and stops by thread, retains the
start stack, and sums the stop event's nanosecond duration. Totals below cover all three servers.

| Workload | Workers | Paired lock waits | Total wait (ms) | Waits attributed to admission / replica capacity / AsyncSemaphore |
| --- | ---: | ---: | ---: | ---: |
| Unique scalar sets | 48 | 97 | 14.7633 | 0 |
| Unique scalar sets | 256 | 98 | 22.1986 | 0 |
| Seeded scalar gets | 256 | 1029 | 50.5651 | 7 (0.1131 ms combined) |

These profiles do not identify this semaphore as a blocking bottleneck. They also do not measure the cost of
uncontended locking or spinning: the runtime emits contention events when entering its wait stage after the
spin path ([.NET 10 Lock implementation](https://raw.githubusercontent.com/dotnet/runtime/v10.0.0/src/libraries/System.Private.CoreLib/src/System/Threading/Lock.cs)).
This motivated measuring the primitive separately rather than treating zero events as zero cost.
Profiled throughput is not used to compare candidate and control.

## Controlled microbenchmarks

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K. Tiered JIT/PGO overrides are absent.
Both variants use the same benchmark assembly and dependencies; only `Squirix.Server.dll` and its PDB differ.
One in-process job uses three warmups, five measurements and a requested 500 ms iteration time.
`AdmissionGateBenchmarks.AcquireAndDisposeAsync` decreases from 106.0 to 92.79 ns per pair (about 12.5%).

The new `AdmissionSemaphoreContentionBenchmarks` starts synchronized dedicated workers sharing one primitive.
Capacity is sufficient for every worker; this measures contention on shared state without permit exhaustion.
Each invocation performs 262144 total acquire/return pairs, split between workers. Reported wall time is
normalized by the total number of pairs, including worker creation and coordination. It is not per-worker
latency or an RPC measurement.

| Method | Workers | Control (ns/pair) | Candidate (ns/pair) |
| --- | ---: | ---: | ---: |
| AdmissionAsync | 1 | 109.16 | 96.03 |
| AcquireReleaseAsync | 1 | 26.52 | 15.11 |
| WaitAsync | 1 | 42.79 | 14.98 |
| AdmissionAsync | 8 | 307.14 | 139.79 |
| AcquireReleaseAsync | 8 | 168.49 | 68.13 |
| WaitAsync | 8 | 94.50 | 76.48 |
| AdmissionAsync | 24 | 373.73 | 154.34 |
| AcquireReleaseAsync | 24 | 99.39 | 76.38 |
| WaitAsync | 24 | 135.96 | 80.73 |

The 24-worker admission case is about 2.42 times faster in this synthetic workload. The control is noisy in
some rows: WaitAsync/1 has a 47.5 ns error and AcquireReleaseAsync/8 has a 128 ns error. These short runs
support lower primitive overhead; they do not establish exact ratios for every row or a cluster speedup.
The existing sequential admission benchmark reports zero allocated bytes for both variants. Worker setup
allocations are amortized in the new benchmark and must not be interpreted as absence of setup allocation.

## Unprofiled RF=3 reads

The control host/dependency directory is frozen before production editing. The candidate replaces only the
server DLL/PDB. Each run starts three fresh hosts and seeds 512 scalar keys, then warms up for five seconds
and measures successful reads for 15 seconds. No builds, tests or trace conversion overlap these runs.
Each server and the client use `DOTNET_PROCESSOR_COUNT=4`; all processes share one physical machine.
The client uses the same SDK and a loopback development-certificate handler throughout; internode mTLS
validates the generated CA and node identities. Read values are checked by the diagnostic workload.

48 or 256 worker tasks share one SDK client and its existing per-peer limit of 64. These are closed-loop
requests, not independent SDK connections. The order at each load is control, candidate, candidate, control.
Server CPU is aggregate process CPU time for all three hosts divided by completed reads, excluding the client.

| Order | Workers | Variant | Reads | Throughput (op/s) | p50 (ms) | p99 (ms) | Server CPU (us/op) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 48 | baseline | 1267541 | 84501 | 0.4160 | 3.0933 | 92.3 |
| 2 | 48 | candidate | 1457731 | 97178 | 0.3864 | 2.6884 | 89.4 |
| 3 | 48 | candidate | 1463431 | 97560 | 0.3964 | 2.5584 | 87.6 |
| 4 | 48 | baseline | 1473918 | 98258 | 0.3819 | 2.6672 | 92.1 |
| 1 | 256 | baseline | 1847224 | 123138 | 1.6257 | 5.1684 | 75.8 |
| 2 | 256 | candidate | 1824560 | 121608 | 1.6352 | 5.2805 | 80.9 |
| 3 | 256 | candidate | 1687106 | 112462 | 1.7799 | 5.8270 | 79.2 |
| 4 | 256 | baseline | 1542885 | 102833 | 1.9848 | 6.2927 | 79.6 |

At 48 workers, controls span 84.5-98.3 thousand reads/s and candidates 97.2-97.6 thousand. At 256 workers,
controls span 102.8-123.1 thousand and candidates 112.5-121.6 thousand. The first 48-worker control and last
256-worker control are slower than the other samples, so an aggregate average would overstate a gain.
The throughput and p99 ranges overlap; CPU/op does not improve consistently across both loads. Two samples
per variant/load on one shared host do not establish a general RPC speedup or non-regression guarantee.
The supported result is lower measured semaphore/admission overhead, with existing queue semantics retained.
Further end-to-end work should follow a fresh bottleneck profile rather than extrapolate the 2.42x micro result.

## Validation and reproduction

- Release server unit-test build: zero warnings/errors with repository analyzers enabled.
- Measured-base server unit suite: 4468 passed, zero failed, one macOS-only test skipped on Windows (4469 total).
- Publication-base server unit suite: 4469 passed, zero failed, one macOS-only test skipped on Windows (4470 total).
- Server integration suite with this production implementation: 322 passed, zero failed/skipped.
- Twelve new semaphore cases cover cancellation at the head/middle/tail, conservation and capacity under
  concurrent returns and cancellation, disposal racing return, full-range capacity, and 256 synchronized
  fresh-semaphore wait/return races. They supplement the nine existing semaphore cases.
- The tests exercise local concurrency behavior; they are not a proof over every possible interleaving or
  a distributed crash/partition/recovery campaign. Durability protocol code is outside this change.

Local artifacts are ignored under `artifacts/admission50/`: `baseline-profile-48`, `baseline-profile-256`,
`baseline-read-profile-256`, `baseline-micro`, `cas-release-micro`, and the eight `read-*` directories.
The `cas-release-micro` label refers to the retained single-state CAS implementation.
Each cluster directory retains `result.json`, workload logs, settings and host logs; profiled directories
also retain traces and parsed contention stacks. BDN directories retain full JSON and Markdown reports.
`reproduction/` retains the local runner, diagnostic probe and measurement/parser helpers; paths in these
diagnostic scripts refer to this local worktree and must be adjusted on another machine. Generated test
certificates and persisted test data stay in ignored artifacts.

Measured snapshot SHA-256 (before rebase; its candidate DLL matched the production output and unit-test dependency):

- Control server DLL: `49220198cf8c127db4749e74cb36ac7722bdef6b3e1b6923ca99e88f195aba74`.
- Candidate server DLL: `b8008fd629f5a82e69010d75122a84dbf2f2ee9f8ebd4ae18c493f816c9d1216`.
- Candidate semaphore source: `1635f99f233c682e755b05757ccf9718afade02adb5050815bf7fd087ef87911`.

Example unit command after building Release:

```powershell
dotnet tests/squirix.server/squirix.server.unit-tests/bin/Release/net10.0/Squirix.Server.UnitTests.dll
```

Example BDN arguments for a fixed harness process, swapping only the server DLL/PDB between variants:

```text
--filter '*AdmissionSemaphoreContentionBenchmarks*' '*AdmissionGateBenchmarks*' --inProcess --warmupCount 3 --iterationCount 5 --iterationTime 500 --exporters json
```
