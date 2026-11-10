# Journal group commit

Durable mutations append a journal record and wait for a durability flush before the caller gets its response. Without
group commit, each mutation pays for its own durability flush (typically one fsync round-trip per commit). A hosted
write with an operation id waits for one flush, not two: its outcome frame is appended right behind its mutation frame
and the same flush covers both.

## Defaults

| Setting | Default | Valid values and effect |
| --- | --- | --- |
| `Journal.GroupCommitMaxWait` | `00:00:00` | Zero disables group commit. A wait of 1 to 100 whole milliseconds enables it and sets the batch deadline. |
| `Journal.GroupCommitMaxBatch` | `32` | 1 to 4096. When group commit is enabled, this many waiting writes trigger an early flush. |

The journal flushes when either threshold is reached. An early flush covers all writes already waiting and can cover
more than `GroupCommitMaxBatch`.

Both are public hosting settings under `SquirixServerOptions.Journal` or `Squirix:Cluster:Journal`. For example,
`"GroupCommitMaxWait": "00:00:00.002"` sets a 2 ms wait. A positive wait requires persistence. See
[configuration](configuration.md#journal).

## Durability guarantee

Group commit never lets a caller or a reader see a value before its journal bytes are fsynced:

- **Apply after fsync.** A mutation appends, waits for the shared flush, and applies to memory only afterwards, with or
  without an operation id. A reader never sees a value that a crash right now would lose, and a failed fsync never leaves
  the value in memory.
- **Response after the outcome is durable (with an operation id).** Every mutation of the v0.1 client carries an
  operation id, and its outcome is recorded durably before the response is sent. For the v0.1 RPCs the outcome is known
  before the apply, so its frame is appended right behind the mutation frame and one flush makes both durable; a
  mutation whose outcome frame cannot be appended with it falls back to recording the outcome after the apply, with a
  second flush. The journal order guarantees that a durable outcome frame always has its mutation frame durable before it.

## Scope and failure behaviour

- **RF>1.** Group commit affects the node cache journal only; replicated writes use the replica group log for durability
  and skip the per-write cache journal durability wait, including when applying committed entries again.
  Background log maintenance calls
  `ReplicaGroupCommitter.FlushAppliedAsync` before persisting the applied index and compacting the group log. That
  maintenance waits for the cache journal flush; it is not an additional `GroupCommitMaxWait` on every replicated commit.
- **Cancellation.** After the append the durability wait cannot be cancelled by the caller. A client deadline shorter than
  `GroupCommitMaxWait` ends the call with `DeadlineExceeded` while the write still commits; a retry with the same
  operation id replays the recorded outcome.
- **Shutdown and flush failure.** A graceful stop completes pending grouped writes successfully: the final flush of the
  shutdown marker covers their frames, including the outcome frame of a write with an operation id that is parked in the
  durability wait, so that write is applied and answered. A write whose outcome frame was not yet appended when the stop
  began is made durable and applied, but its outcome can no longer be appended, so it ends with `COMMIT_OUTCOME_UNKNOWN`
  (gRPC `Unavailable`); retry with the same operation id after restart. Any write also ends with
  `COMMIT_OUTCOME_UNKNOWN` when the stop times out, the final flush fails, or a failure was latched before the marker; the
  write may or may not be durable.

A new public SDK call generates a new operation id; it does not repeat the previous call with its original id.
The same-id retry described above requires a caller that retains and resends that id, within the replay window.
After `COMMIT_OUTCOME_UNKNOWN`, reconcile the application state before issuing another mutation through the SDK.
See [unknown commit outcomes](diagnostics.md#metrics-ownership).

## When to enable group commit

Consider enabling it when many durable writers on distinct keys can share an expensive fsync and throughput matters
more than tail latency. Keep the default for isolated writers and measure on the storage and OS used in production.

## Tuning guide (operator / integrator)

Start with a 2 ms wait and the default batch threshold. Longer waits can improve throughput at the cost of tail
latency. Raising the batch threshold can reduce flush frequency under load, but can also make writers wait longer.

Effects to account for:

- **One durability wait per hosted write.** A hosted write waits once, for a flush that covers its mutation frame and its
  idempotency outcome frame, so a lone write pays up to `1 × MaxWait`. A write that falls back to recording its outcome
  after the apply waits twice.
- **Hot key.** Writers queue for the same key in arrival order and hold it across the durability wait. A hot key
  completes about one write per fsync. Writers on distinct keys can share a flush when they reach
  `AwaitDurabilityCommitAsync` together.
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

`DurableMutationGroupCommitBenchmarks`: hosted write path (mutation frame plus idempotency outcome frame, measured with
two durability waits per write, so the figures overstate the latency cost of one wait), distinct keys,
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
