# Journal completion and wake overhead

Date: 2026-10-30. Measured control: `e8488ed442d74dc0cb57e8353f76e3b0792a54bd` (`develop`).
Local candidate: `perf/journal-thread-locks`.

## Changes and invariants

`PendingAppendRegistry.UntrackBatch` removes a completed write batch under one registry lock and returns
only the entries whose cleanup the journal thread owns. A concurrent failure drain still owns the entries
it removes first, including their buffer quarantine, queued-append counter and failed ack. Removal continues
to use reference identity, even when two `JournalWorkItem` records compare equal. Duplicate or missing input
items cannot claim a resource twice. The reusable output list reserves capacity before changing ownership.

The segment writer keeps the abandoned-item check while staging and both batch checks around the segment
write. A drain observed after writing still causes truncation and a flush of that truncation. Written-byte
accounting advances before batch removal lowers pending bytes, preserving conservative capacity admission.
Buffer clearing/return, queued-slot decrement and ack completion remain outside the registry lock, and only
owned entries are completed. The completion list is cleared after use so it retains no frame references;
its backing array can retain capacity for the largest completed batch.

For an ordinary staged batch of N frames, the consumer-side registry lock count changes from 2N+2 to N+3:
N staging checks and two batch checks remain; N individual completion removals become one batch removal.
Producer tracking remains one registry lock per frame. Single-frame batches do not save a lock acquisition.
The individual completion API remains for unbatched writes and failure cleanup.

`BoundedJournalRing` now signals an enqueue only while its single consumer has registered a possible wait.
The consumer registers using an interlocked operation, rechecks queue state after that full fence, and
clears registration when it resumes. Producers publish the item before a fenced read of registration.
If publication precedes registration, the recheck observes work; if it follows registration, the producer
signals the event. Registration stays set until the consumer resumes, so a producer delayed before setting
the event cannot prevent a later producer from waking that consumer.

The fence assumptions follow the [.NET runtime memory model](https://github.com/dotnet/runtime/blob/v10.0.0/docs/design/specs/Memory-model.md),
which specifies full-fence ordering for Interlocked operations; a volatile flag alone would not provide that ordering.

`NotifyWorkAvailable` remains unconditional for control notifications: group-commit deadlines, segment-roll
completion and shutdown can require a re-evaluation without enqueueing an item. A control event sent before
wait registration stays latched. Finite and infinite waits retain their existing deadline-wait implementation.
Slot reservation/publication, failure polling, journal framing, durability flushes and recovery formats remain
unchanged. This change does not relax disk persistence requirements.

## Fresh contention profile

Three actual server host processes run RF=3, persistence and internode mTLS, with the SDK in a fourth process.
After five seconds of warmup, 256 closed-loop worker tasks perform successful scalar sets with unique keys
for 25 seconds. Every server is traced for 15 seconds with `dotnet-sampled-thread-time` and
`Microsoft-Windows-DotNETRuntime:0x4000:5`. The parser pairs contention starts/stops by thread and groups
start stacks using the stop duration. Counts below cover all three servers.

| Stack attribution | Paired waits | Total duration (ms) |
| --- | ---: | ---: |
| PendingAppendRegistry | 8 | 0.1082 |
| BoundedJournalRing | 2 | 0.0806 |
| Other | 85 | 2.0541 |

These events do not establish either registry or ring blocking as a major current bottleneck. They do not
measure uncontended lock cost, spinning or the CPU cost of calling the work event's Set method. The profiled
9503 writes/s is diagnostic context; it is not compared with an unprofiled candidate as evidence of speedup.
Historical audit CPU estimates are not current measurements from this run.

## Controlled measurements

BenchmarkDotNet 0.15.8, .NET 10.0.14, Windows 11, Intel Core Ultra 9 285K. Tiered JIT/PGO overrides are absent.
Each benchmark uses one in-process job, three warmups, five measurements and a requested 500 ms iteration time.
No builds, tests or trace conversion overlap measurement.

`JournalRingBusyBenchmarks` performs 65536 enqueue/dequeue pairs per invocation in bursts of one or 32.
The same thread fills and drains each burst; the consumer never parks and capacity never blocks. This
isolates redundant enqueue notification overhead. Both variants use the same frozen benchmark assembly
and dependencies, replacing only the server DLL/PDB.

| Burst | Control (ns/pair) | Candidate (ns/pair) | Allocated per pair |
| ---: | ---: | ---: | ---: |
| 1 | 151.39 | 41.75 | 0 B at BDN resolution |
| 32 | 151.71 | 42.63 | 0 B at BDN resolution |

This is a busy-ring microbenchmark, not RPC throughput, concurrent MPSC saturation or idle wake latency.
The ownership and wait-registration tests independently exercise parked consumers and concurrent producers.

`JournalRegistryBatchBenchmarks` compares the unchanged individual completion API with `UntrackBatch` in
the same candidate DLL. Both paths perform the same tracking, staging checks, two batch checks and output
validation, without segment I/O, pooled-buffer release or ack completion. Each invocation processes 65536
frames in batches of one, 32 or 256. Integer batch parameters divide this total exactly. Both paths reuse
fixed items and preallocated completion storage; `PendingAppendEntry` allocation during Track remains.
This is a comparison of completion strategies, not an end-to-end writer benchmark or a frozen-DLL comparison.

| Batch size | Individual API (ns/frame) | Batch API (ns/frame) | Allocated per frame, both APIs |
| ---: | ---: | ---: | ---: |
| 1 | 75.78 | 75.46 | 48 B |
| 32 | 54.74 | 43.31 | 48 B |
| 256 | 53.84 | 41.86 | 48 B |

Busy-ring cost decreases by about 72% in this isolated workload. The measured registry lifecycle is about
21-22% cheaper for batches of 32-256 frames; the single-frame case is essentially unchanged. Track's 48 B
allocation per frame remains. Neither result implies the same percentage improvement for an entire RPC.
The registry measurement does not include competing producer threads or output-buffer clearing, while the
ring measurement deliberately excludes parking, so these microbenchmarks do not characterize every journal load.

## Unprofiled RF=3 writes

The control host and dependencies are frozen before production editing. The candidate replaces only the
server DLL/PDB; the client SDK is identical. Each run starts three fresh hosts and fresh data directories,
warms up for five seconds, then measures successful scalar sets with unique keys for 15 seconds. Each
load uses the order control, candidate, candidate, control. Startup, warmup and teardown are excluded.

48 or 256 worker tasks share one SDK client and its existing per-peer limit of 64; they are not independent
SDK connections. Each server and the client use `DOTNET_PROCESSOR_COUNT=4`. All processes share one physical
machine and storage. The diagnostic client bypasses loopback development-certificate validation; internode
mTLS validates the generated CA and node identities. The workload grows the cache and retained operation
records, so this is not a steady-state capacity or saturation study. Server CPU sums all three hosts,
excludes the client, and is normalized by successful writes.

| Order | Workers | Variant | Writes | Throughput (op/s) | p50 (ms) | p99 (ms) | Server CPU (us/op) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 48 | baseline | 139159 | 9266 | 4.3298 | 24.0434 | 563.1 |
| 2 | 48 | candidate | 121566 | 8102 | 4.4022 | 25.9939 | 535.1 |
| 3 | 48 | candidate | 135988 | 9063 | 4.4029 | 23.9900 | 567.0 |
| 4 | 48 | baseline | 119725 | 7979 | 4.4773 | 25.8876 | 545.0 |
| 1 | 256 | baseline | 127476 | 8447 | 20.3360 | 97.9347 | 499.8 |
| 2 | 256 | candidate | 146158 | 9731 | 19.6355 | 92.9762 | 501.7 |
| 3 | 256 | candidate | 122999 | 8151 | 20.0660 | 94.9949 | 504.8 |
| 4 | 256 | baseline | 140554 | 9316 | 20.8770 | 93.8390 | 419.4 |

At 48 workers, control throughput spans 7979-9266 writes/s and candidate throughput 8102-9063. At 256,
controls span 8447-9316 and candidates 8151-9731. Throughput ranges overlap. At 256 workers, candidate
p99 spans 92.98-94.99 ms and controls 93.84-97.93 ms; these ranges overlap, as do the 48-worker ranges.
Two samples per variant/load do not establish a general tail-latency improvement. CPU/op overlaps at 48; both 256-worker candidate samples are above the two controls.
No consistent server CPU reduction or general RPC speedup is established. The run-to-run variation is not
explained by this experiment, and these results are not a general non-regression guarantee.

## Validation and limits

- Release server unit, integration and benchmark builds passed with repository analyzers enabled.
- The initial focused journal run passed 214 cases before the additional multi-producer case was added.
- Final full server unit suite: 4492 passed, zero failed; one macOS-only test skipped on Windows (4493 total).
- Final server integration suite: 322 passed, zero failed/skipped.
- Fourteen new cases cover batch ownership against a failure drain in both orders and concurrent races,
  reference identity, duplicate/missing entries, multi-frame write truncation and repeated completion,
  pre-wait control notifications, already parked consumers, 256 empty-ring publication/wait races for
  each deadline mode, and eight concurrent producers preserving 1024 items and all queue slots.
- Local manual review checked the two publication/registration orderings, competing producers, control
  notifications without queue items, removal ownership, byte-accounting order and buffer quarantine.

These checks cover the stated local concurrency and workload scenarios. They are not an exhaustive proof
of every interleaving or a new crash, partition and recovery campaign. Existing failure/recovery tests ran
as part of the server suites. No general throughput, CPU or tail-latency guarantee follows from a primitive
microbenchmark or two cluster samples per variant/load.

## Reproduction

Ignored local artifacts under `artifacts/journal15/` retain the frozen hosts, ring harness, registry harness,
BDN JSON/Markdown reports, `baseline-profile-256` traces and parsed stacks, and eight `write-*` result/log
directories. `reproduction/` retains the diagnostic probe, runner and measurement/parser helpers. Those
scripts refer to this local worktree; adjust paths and output tags before running elsewhere or repeating
runs. Generated test certificates and persisted workload data remain ignored.

SHA-256 (candidate DLL matches the production output and both tested server-suite dependencies):

- Control server DLL: `a32628b6333e91af76d5f2f0a0e0f989dc6239a9bf14535236599b082f4405ef`.
- Candidate server DLL: `5835ae87c4cb97e8080cc107f48fe03f697a4321e3935479489610ada8372309`.
- Frozen ring benchmark DLL: `93f8cef78258c85c92b6de71fb611efaf56b6d92da2347bae25ed5134ef63751`.
- Registry benchmark DLL: `18798120260259e8261874b41e6a92f5842ef71aa13e5b79155bcf3a598971eb`.
- Candidate BoundedJournalRing.cs: `8ca32a6ece2f556e0cdba7765392d404e599e953bc2a6919b547818caf64a533`.
- Candidate PendingAppendRegistry.cs: `b1af70e1be3be073f1dc4f1bee975b56b15fe40446c2cbc56bab93e7487eb2f7`.
- Candidate JournalEventLoopSegmentWriter.cs: `9436ae19b1615204b43cfc5de3af986a6e6469c334bca1db489f8d106b3ccee6`.

Build the Release unit-test project, then run the focused journal namespace:

```powershell
dotnet tests/squirix.server/squirix.server.unit-tests/bin/Release/net10.0/Squirix.Server.UnitTests.dll --treenode-filter "/*/Squirix.Server.UnitTests.Persistence.Journaling/*/*"
```

BDN arguments for the frozen ring harness:

```text
--filter '*JournalRingBusyBenchmarks*' --inProcess --warmupCount 3 --iterationCount 5 --iterationTime 500 --exporters json
```

Use `*JournalRegistryBatchBenchmarks*` with the candidate registry harness to compare both completion APIs.
