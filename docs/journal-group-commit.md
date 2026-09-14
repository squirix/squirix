# Journal group commit

Durable mutations append a journal record, wait for durability, then apply in-memory state. Without group commit,
each mutation pays for its own durability flush (typically one fsync round-trip per commit).

## Defaults

The library uses **conservative defaults** suitable for unknown workloads (single writer, latency-first, no tuning required):

| Setting                      | Default   | Meaning                                      |
| ---------------------------- | --------- | -------------------------------------------- |
| `JournalGroupCommitMaxWait`  | `0`       | Group commit **disabled**                    |
| `JournalGroupCommitMaxBatch` | `32`      | Batch cap when group commit is enabled       |

Group commit is **opt-in**: set `JournalGroupCommitMaxWait` to a value greater than zero. `JournalGroupCommitMaxBatch`
only applies when group commit is enabled.

These defaults are set on internal `PersistenceOptions`. v0.1 public hosting (`squirix-server` / `AddSquirixServerAsync`)
does not merge a Persistence JSON section from `Squirix.settings.json`, so group commit stays disabled unless a custom
host injects `PersistenceOptions` (for example tests).

## Policy

| Setting                      | Effect                                                                               |
| ---------------------------- | ------------------------------------------------------------------------------------ |
| `JournalGroupCommitMaxWait`  | When `> 0`, concurrent durable mutations can share one `FlushAsync` / fsync.         |
| `JournalGroupCommitMaxBatch` | Upper bound on mutations batched into a single durability flush.                     |

A batch ends when **either** limit is reached first:

- **`MaxBatch`** — enough waiters joined → flush immediately.
- **`MaxWait`** — timer expires before the batch is full → flush the partial batch.

When constructing `PersistenceOptions` explicitly (tests / custom composition), the wait uses property
`JournalGroupCommitMaxWait` (JSON name `groupCommitMaxWait`, milliseconds). See [configuration](configuration.md).

## Durability guarantee

Group commit does **not** relax fsync-before-memory-apply:

1. Precondition + journal append run under the mutation gate (serialized).
2. The caller waits on `AwaitDurabilityCommitAsync`, which joins a flush batch.
3. Memory apply runs under the mutation gate only after the shared flush completes.

A mutation never observes in-memory apply until its appended bytes are covered by a completed durability flush (same
guarantee as per-mutation `FlushAsync`).

## When to enable group commit

Enable group commit only when **all** of the following apply:

- You have **many concurrent durable mutations on different keys** (not a single hot key).
- Throughput matters more than minimizing commit tail latency.
- You can benchmark on **your** storage and OS and accept the latency trade-off.

Leave the default (`MaxWait = 0`) when:

- Most traffic is single-writer or low concurrency.
- You need the lowest predictable commit latency.
- You have not measured fsync cost and concurrent writer count on target hardware.

Group commit batches waiters that reach `AwaitDurabilityCommitAsync` at roughly the same time. Mutations on the **same
cache key** are still serialized (one in-flight durable mutation per key), so a hot key does not benefit from batching.

## Tuning guide (operator / integrator)

There is no single pair of values that maximizes performance for every deployment. Treat tuning as a **workload-specific
measurement exercise**, not a library default.

### `JournalGroupCommitMaxWait`

Maximum time to wait for additional waiters before flushing a batch that is not yet full.

| Direction | Throughput                                  | Tail commit latency                  |
| --------- | ------------------------------------------- | ------------------------------------ |
| Higher    | Usually up (larger batches, fewer fsyncs)   | Usually up (waiters may wait longer) |
| Lower     | Usually down                                | Usually down                         |
| `0`       | One fsync per mutation (group commit off)   | Lowest for isolated writers          |

### `JournalGroupCommitMaxBatch`

Hard cap on how many durability waiters share one fsync.

| Direction | Effect |
| --------- | ------ |
| Higher | Fewer fsyncs under heavy concurrency; last waiter in a large batch waits for the whole batch |
| Lower | Smaller batches, more frequent fsyncs, lower batch-induced latency |
| Irrelevant when `MaxWait = 0` | Group commit is disabled |

Set `MaxBatch` to at least your expected **peak concurrent durable mutations on distinct keys**, but avoid unnecessarily
large values if p99 commit latency is sensitive.

### Starting points (not defaults)

Use these only as **first experiments** after enabling group commit, then sweep on representative hardware:

| Profile          | `MaxWait` (starting point) | `MaxBatch` (starting point)       |
| ---------------- | -------------------------- | --------------------------------- |
| Latency-first    | `0` (keep disabled)        | n/a                               |
| Balanced         | `2–5 ms`                   | `32` (default cap)                |
| Throughput-first | `5–10 ms`                  | `64` (if concurrency supports it) |

Suggested sweep:

1. Fix `MaxBatch = 32`, vary `MaxWait` (for example `0`, `1`, `2`, `5`, `10 ms`) and measure throughput and p99 commit
   latency.
2. At the best `MaxWait`, vary `MaxBatch` (for example `16`, `32`, `64`, `128`) until throughput stops improving or p99
   exceeds your budget.

On Windows, short `Task.Delay` values may resolve coarser than the configured duration; include `2–5 ms` in sweeps, not
only `1 ms`.

### Development benchmarks vs production tuning

Internal benchmarks may use a **minimal non-zero** `MaxWait` (for example `1 ms`) to exercise the group-commit code path
under concurrent writers. That value is a **regression gate for backend comparison**, not a recommendation for production.

Production integrators should choose `MaxWait` and `MaxBatch` from their own measurements and SLA.

### Measured defaults (Windows, 2026-06-21)

Internal quick benchmarks (`SQUIRIX_BENCH_QUICK=1`, 800 ops/invoke, 8→4 writers) after Pipelined GC tuning.
JsonFramed write backend was removed in `8d2664c5`; numbers below are from pre-removal A/B runs kept for context.

| Path                                     | Payload | Pipelined vs legacy JsonFramed write |
| ---------------------------------------- | ------- | ------------------------------------ |
| **DurableMutationExecutor** (production) | 256 B   | **~2× throughput**                   |
| DurableMutationExecutor                  | 4096 B  | ~1.17× throughput                    |

**Recommendations for production concurrent durable writes:**

- As a production starting point after enabling group commit, try **`JournalGroupCommitMaxWait = 1–5 ms`** with
  **`JournalGroupCommitMaxBatch = 32`** (default batch cap). The library default remains `MaxWait = 0` (disabled).
- Prefer the **DurableMutationExecutor** group-commit path (conflict key + barrier) over calling `AppendPutAsync` and
  `AwaitDurabilityCommitAsync` separately on hot paths.

## Latency vs throughput (summary)

| Mode                                       | Throughput under concurrent writers                               | Tail latency                                                                    |
| ------------------------------------------ | ----------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Disabled (`JournalGroupCommitMaxWait = 0`) | One fsync per mutation                                            | Lowest for a single writer                                                      |
| Enabled                                    | Amortizes fsync across up to `JournalGroupCommitMaxBatch` writers | Adds up to `JournalGroupCommitMaxWait` wait before flush when batch is not full |

Benchmark journal persistence on representative hardware before enabling group commit in production.
