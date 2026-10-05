# Journal group commit

Durable mutations append a journal record and wait for a durability flush before the caller gets its response. Without
group commit, each mutation pays for its own durability flush (typically one fsync round-trip per commit).

## Defaults

The library uses **conservative defaults** suitable for unknown workloads (single writer, latency-first, no tuning required):

| Setting                       | Default    | Meaning                                |
| ----------------------------- | ---------- | -------------------------------------- |
| `Journal.GroupCommitMaxWait`  | `00:00:00` | Group commit **disabled**              |
| `Journal.GroupCommitMaxBatch` | `32`       | Waiting writes that trigger a flush    |

Group commit is **opt-in**: set `Journal.GroupCommitMaxWait` to a value greater than zero (1 to 100 whole milliseconds).
`Journal.GroupCommitMaxBatch` (1 to 4096) only applies when group commit is enabled. It is a flush trigger, not a hard cap:
one flush may cover more writes than `GroupCommitMaxBatch`.

Both are public hosting settings: `SquirixServerOptions.Journal` or the `Squirix:Cluster:Journal` settings object, for
example `"GroupCommitMaxWait": "00:00:00.002"`. A positive wait requires persistence. See
[configuration](configuration.md#journal).

## Policy

| Setting                       | Effect                                                                        |
| ----------------------------- | ----------------------------------------------------------------------------- |
| `Journal.GroupCommitMaxWait`  | When `> 0`, concurrent durable mutations can share one `FlushAsync` / fsync.  |
| `Journal.GroupCommitMaxBatch` | Waiting writes at which the batch flushes early (may cover more).             |

A batch ends when **either** limit is reached first:

- **`MaxBatch`** — enough waiters joined → flush without waiting for `MaxWait`; the flush covers every waiter present,
  so it may cover more than `MaxBatch`.
- **`MaxWait`** — timer expires before `MaxBatch` waiters joined → flush the partial batch.

See [configuration](configuration.md#journal) for the settings format and bounds.

## Durability guarantee

Group commit never lets a caller or a reader see a value before its journal bytes are fsynced:

- **Apply after fsync.** A mutation appends, waits for the shared flush, and applies to memory only afterwards, with or
  without an operation id. A reader never sees a value that a crash right now would lose, and a failed fsync never leaves
  the value in memory.
- **Response after a second fsync (with an operation id).** Every mutation of the v0.1 client carries an operation id, and
  its outcome is recorded durably before the response is sent
  (`RpcMutationIdempotencyCoordinator.RecordOutcomeDurablyAsync`).

Replicated applies are the exception: their durable source is the group log, so they do not wait for the node journal
before applying.

## Scope and failure behaviour

- **RF>1.** Group commit affects the node journal only. A replicated write is acknowledged by the group log fsync;
  `ReplicaGroupCommitter.FlushAppliedAsync` waits on the node journal, so a replicated commit stretches by up to
  `GroupCommitMaxWait`.
- **Cancellation.** After the append the durability wait cannot be cancelled by the caller. A client deadline shorter than
  `GroupCommitMaxWait` ends the call with `DeadlineExceeded` while the write still commits; a retry with the same
  operation id replays the recorded outcome.
- **Shutdown and flush failure.** A graceful stop completes pending grouped writes without an operation id successfully:
  the final flush of the shutdown marker covers their frames. A write with an operation id parked in the durability wait is
  made durable and applied, but its outcome can no longer be appended, so it ends with `COMMIT_OUTCOME_UNKNOWN` (gRPC
  `Unavailable`); retry with the same operation id after restart. Any write also ends with `COMMIT_OUTCOME_UNKNOWN` when
  the stop times out, the final flush fails, or a failure was latched before the marker; the write may or may not be
  durable.

## When to enable group commit

Enable group commit only when **all** of the following apply:

- You have **many concurrent durable mutations** (on different keys).
- Throughput matters more than minimizing commit tail latency.
- You can benchmark on **your** storage and OS and accept the latency trade-off.

Leave the default (`MaxWait = 0`) when:

- Most traffic is single-writer or low concurrency.
- You need the lowest predictable commit latency.
- You have not measured fsync cost and concurrent writer count on target hardware.

Group commit batches waiters that reach `AwaitDurabilityCommitAsync` at roughly the same time. Mutations on the **same
cache key** are serialized per key in arrival order and none is refused. The key stays held across the fsync, so a hot key
completes about one write per fsync (per `MaxWait` plus fsync); distinct keys still share batches.

## Tuning guide (operator / integrator)

There is no single pair of values that maximizes performance for every deployment. Treat tuning as a **workload-specific
measurement exercise**, not a library default.

### `Journal.GroupCommitMaxWait`

Maximum time to wait for additional waiters before flushing a batch that has not reached `GroupCommitMaxBatch`.

| Direction | Throughput                                  | Tail commit latency                  |
| --------- | ------------------------------------------- | ------------------------------------ |
| Higher    | Usually up (larger batches, fewer fsyncs)   | Usually up (waiters may wait longer) |
| Lower     | Usually down                                | Usually down                         |
| `0`       | One fsync per mutation (group commit off)   | Lowest for isolated writers          |

### `Journal.GroupCommitMaxBatch`

Number of waiting writes at which the batch is flushed without waiting for `GroupCommitMaxWait`. One flush may cover more.

| Direction | Effect |
| --------- | ------ |
| Higher | Fewer fsyncs under heavy concurrency; last waiter in a large batch waits for the whole batch |
| Lower | Smaller batches, more frequent fsyncs, lower batch-induced latency |
| Irrelevant when `MaxWait = 0` | Group commit is disabled |

Set `MaxBatch` to at least your expected **peak concurrent durable mutations on distinct keys**, but avoid unnecessarily
large values if p99 commit latency is sensitive.

### Recommended starting values

Keep group commit **off** (`MaxWait = 0`) unless a measurement on your storage shows fsync is the bottleneck. When it is,
start with:

| Setting                       | Starting value | Notes                                                              |
| ----------------------------- | -------------- | ------------------------------------------------------------------ |
| `Journal.GroupCommitMaxWait`  | `2 ms`         | Latency a lone write can pay per durability wait; sweep `1–10 ms`  |
| `Journal.GroupCommitMaxBatch` | `32` (default) | Raise toward the peak count of concurrent writers on distinct keys |

Group commit can only win when many writers on **distinct keys** reach the journal together (roughly `MaxBatch` or more
in flight) and one fsync is expensive (network or cloud block storage, SATA SSD without a write cache, spinning disk).
On fast local storage a flush is cheap, so waiting for a batch only adds latency.

Effects to account for:

- **Two durability waits per hosted write.** A hosted write waits once for its mutation frame and once for its
  idempotency outcome frame, so a lone write can pay up to `2 × MaxWait`.
- **Hot key.** Writers on one key are serialized across the fsync, so a hot key completes about one write per batch wait
  and gains nothing; batching needs distinct keys.
- **Windows timer granularity.** On Windows 10 version 1803 and later (Windows Server 2019 and later) the journal thread
  waits for the batch deadline on a high-resolution timer, so a partial batch is flushed about `1 ms` after `MaxWait`.
  Older Windows falls back to a millisecond wait handle that Windows rounds up to the system timer tick (about
  `15.6 ms`), so `MaxWait = 1 ms` and `5 ms` behave alike there; the journal logs one Information line when this
  fallback is in use. Linux and macOS use monotonic millisecond waits. Group commit never changes the process-wide timer
  resolution. A batch that reaches `MaxBatch` flushes at once and does not wait for the timer, so keep concurrency at or
  above `MaxBatch`.

Suggested sweep:

1. Fix `MaxBatch = 32`, vary `MaxWait` (for example `0`, `1`, `2`, `5`, `10 ms`) and measure throughput and p99 commit
   latency.
2. At the best `MaxWait`, vary `MaxBatch` (for example `16`, `32`, `64`, `128`) until throughput stops improving or p99
   exceeds your budget.

See [journal-single-owner-wal.md](journal-single-owner-wal.md) for the journal thread wait loop.

### Measured on one machine

`DurableMutationGroupCommitBenchmarks`: hosted write path (mutation frame plus idempotency outcome frame), distinct keys,
256 B values, 8 writers with 200 writes each, `MaxBatch = 32`, benchmark quick mode, Windows 11, local NVMe SSD with a
write cache (cheap fsync). Throughput in writes per second:

| Writers | Off (`MaxWait = 0`) | `MaxWait = 1 ms` | `MaxWait = 5 ms` |
| ------- | ------------------- | ---------------- | ---------------- |
| 8       | ~9,600              | ~1,900           | ~640             |

On this machine group commit still does not beat the default: with 8 writers the batch never fills, so every write
waits out `MaxWait` per durability wait, and fsync is cheap enough that sharing it would not pay. The high-resolution
batch timer keeps that wait close to `MaxWait`, where the system timer tick previously stretched it to about `15.6 ms`
(about 290 and 260 writes per second at `1 ms` and `5 ms`). Expect a gain only on storage where one fsync costs
milliseconds; measure there.

### Development benchmarks vs production tuning

Internal benchmarks may use a **minimal non-zero** `MaxWait` (for example `1 ms`) to exercise the group-commit code path
under concurrent writers. That value is a **regression gate for backend comparison**, not a recommendation for production.

Production integrators should choose `MaxWait` and `MaxBatch` from their own measurements and SLA.

## Latency vs throughput (summary)

| Mode                                        | Throughput under concurrent writers | Tail latency                                     |
| ------------------------------------------- | ----------------------------------- | ------------------------------------------------ |
| Disabled (`Journal.GroupCommitMaxWait = 0`) | One fsync per mutation              | Lowest for a single writer                       |
| Enabled                                     | Amortizes fsync across waiters      | Adds up to `Journal.GroupCommitMaxWait` per call |

Benchmark journal persistence on representative hardware before enabling group commit in production.
