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

Group commit never lets a caller see success before its journal bytes are fsynced, but when memory is applied depends on
whether the mutation carries an operation id:

- **Response after fsync (always).** The caller's response waits for a durability flush that covers its append. Every
  mutation of the v0.1 client carries an operation id, and its outcome is recorded durably before the response is sent
  (`RpcMutationIdempotencyCoordinator.RecordOutcomeDurablyAsync`).
- **Apply after fsync (only without an operation id).** A mutation without an operation id appends, waits for the shared
  flush, and applies to memory under the mutation gate only afterwards.
- **Apply before fsync (with an operation id).** Durability is deferred: memory is applied right after the append, so
  other readers can observe the value before it is durable. Only the caller's response waits for the fsync.

A response is therefore never sent before its bytes are covered by a completed durability flush (same guarantee as
per-mutation `FlushAsync`), while a concurrent reader can see a value that a crash right now would lose.

## Scope and failure behaviour

- **RF>1.** Group commit affects the node journal only. A replicated write is acknowledged by the group log fsync;
  `ReplicaGroupCommitter.FlushAppliedAsync` waits on the node journal, so a replicated commit stretches by up to
  `GroupCommitMaxWait`.
- **Cancellation.** After the append the durability wait cannot be cancelled by the caller. A client deadline shorter than
  `GroupCommitMaxWait` ends the call with `DeadlineExceeded` while the write still commits; a retry with the same
  operation id replays the recorded outcome.
- **Shutdown and flush failure.** When the node shuts down or a flush fails, pending grouped writes complete with
  `COMMIT_OUTCOME_UNKNOWN` (gRPC `Unavailable`): the write may or may not be durable, so retry with the same operation id.

## When to enable group commit

Enable group commit only when **all** of the following apply:

- You have **many concurrent durable mutations** (on different keys or on a hot key).
- Throughput matters more than minimizing commit tail latency.
- You can benchmark on **your** storage and OS and accept the latency trade-off.

Leave the default (`MaxWait = 0`) when:

- Most traffic is single-writer or low concurrency.
- You need the lowest predictable commit latency.
- You have not measured fsync cost and concurrent writer count on target hardware.

Group commit batches waiters that reach `AwaitDurabilityCommitAsync` at roughly the same time. Mutations on the **same
cache key** are serialized per key in arrival order and none is refused. A same-key mutation releases the key before its
fsync, and the outcome waits of consecutive same-key mutations share batches, so a hot key also benefits.

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

### Starting points (not defaults)

Recommended values are pending measurement on the public hosting path; the numbers below are provisional.
Use these only as **first experiments** after enabling group commit, then sweep on representative hardware:

| Profile          | `MaxWait` (starting point) | `MaxBatch` (starting point)       |
| ---------------- | -------------------------- | --------------------------------- |
| Latency-first    | `0` (keep disabled)        | n/a                               |
| Balanced         | `2–5 ms`                   | `32` (default)                    |
| Throughput-first | `5–10 ms`                  | `64` (if concurrency supports it) |

Suggested sweep:

1. Fix `MaxBatch = 32`, vary `MaxWait` (for example `0`, `1`, `2`, `5`, `10 ms`) and measure throughput and p99 commit
   latency.
2. At the best `MaxWait`, vary `MaxBatch` (for example `16`, `32`, `64`, `128`) until throughput stops improving or p99
   exceeds your budget.

The journal thread uses a timed wait, so on Windows a short `MaxWait` may resolve coarser than the configured value
(OS timer granularity); include `2–5 ms` in sweeps, not only `1 ms`. See [journal-single-owner-wal.md](journal-single-owner-wal.md).

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

**Recommendations for production concurrent durable writes (pending measurement on the public hosting path):**

- As a production starting point after enabling group commit, try **`Journal.GroupCommitMaxWait = 1–5 ms`** with
  **`Journal.GroupCommitMaxBatch = 32`** (default). The library default remains `MaxWait = 0` (disabled).
- Prefer the **DurableMutationExecutor** group-commit path (conflict key + barrier) over calling `AppendPutAsync` and
  `AwaitDurabilityCommitAsync` separately on hot paths.

## Latency vs throughput (summary)

| Mode                                        | Throughput under concurrent writers | Tail latency                                     |
| ------------------------------------------- | ----------------------------------- | ------------------------------------------------ |
| Disabled (`Journal.GroupCommitMaxWait = 0`) | One fsync per mutation              | Lowest for a single writer                       |
| Enabled                                     | Amortizes fsync across waiters      | Adds up to `Journal.GroupCommitMaxWait` per call |

Benchmark journal persistence on representative hardware before enabling group commit in production.
