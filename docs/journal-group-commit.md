# Journal group commit

Durable mutations append a journal record and wait for a durability flush before the caller gets its response. Without
group commit, each mutation pays for its own durability flush (typically one fsync round-trip per commit).

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

- **Apply after fsync.** A mutation appends, waits for the shared flush, and applies to memory only afterwards. A reader never sees a value that a crash right now would lose, and a failed fsync never leaves
  the value in memory.
## Scope and failure behaviour

- **RF>1.** Group commit affects the node cache journal only; replicated writes use the replica group log for durability
  and skip the per-write cache journal durability wait, including when applying committed entries again.
  Background log maintenance calls
  `ReplicaGroupCommitter.FlushAppliedAsync` before persisting the applied index and compacting the group log. That
  maintenance waits for the cache journal flush; it is not an additional `GroupCommitMaxWait` on every replicated commit.
- **Cancellation.** After the append, caller cancellation cannot stop the durability wait or memory apply. A client
  may stop waiting while the write completes.
- **Shutdown and flush failure.** A graceful stop flushes pending records and lets their writes apply. If the final
  flush fails or shutdown interrupts it, the caller can receive an unknown commit outcome.

## When to enable group commit

Consider enabling it when many durable writers on distinct keys can share an expensive fsync and throughput matters
more than tail latency. Keep the default for isolated writers and measure on the storage and OS used in production.

## Tuning guide (operator / integrator)

Start with a 2 ms wait and the default batch threshold. Longer waits can improve throughput at the cost of tail
latency. Raising the batch threshold can reduce flush frequency under load, but can also make writers wait longer.

Effects to account for:

- **One durability wait per hosted write.** A hosted write waits once, for a flush that covers its mutation frame and other
  pending writes.
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

### Development benchmarks vs production tuning

Internal benchmarks may use a **minimal non-zero** `MaxWait` (for example `1 ms`) to exercise the group-commit code path
under concurrent writers. That value is a **regression gate for backend comparison**, not a recommendation for production.

Production integrators should choose `MaxWait` and `MaxBatch` from their own measurements and SLA.
