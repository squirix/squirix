# Journal group commit

Durable mutations append a journal record, wait for durability, then apply in-memory state. With strict fsync
(`PersistenceOptions.StrictFsync`), each independent flush pays a full disk round-trip.

## Policy

| Setting                       | Default (node host) | Effect                                                                               |
| ----------------------------- | ------------------- | ------------------------------------------------------------------------------------ |
| `JournalGroupCommitMaxWaitMs` | `0` (disabled)      | When `> 0`, concurrent durable mutations can share one `FlushAsync` / fsync.         |
| `JournalGroupCommitMaxBatch`  | `32`                | Upper bound on mutations batched into a single durability flush.                     |
| `StrictFsync`                 | `true`              | Unchanged; group commit still calls `FlushCoreAsync` with strict fsync when enabled. |

## Durability guarantee

Group commit does **not** relax fsync-before-memory-apply:

1. Precondition + journal append run under the mutation gate (serialized).
2. The caller waits on `AwaitDurabilityCommitAsync`, which joins a flush batch.
3. Memory apply runs under the mutation gate only after the shared flush completes.

A mutation never observes in-memory apply until its appended bytes are covered by a completed durability flush (same
guarantee as per-mutation `FlushAsync`).

## Latency vs throughput

| Mode                                         | Throughput under concurrent writers                               | Tail latency                                                                      |
| -------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Disabled (`JournalGroupCommitMaxWaitMs = 0`) | One fsync per mutation                                            | Lowest for a single writer                                                        |
| Enabled                                      | Amortizes fsync across up to `JournalGroupCommitMaxBatch` writers | Adds up to `JournalGroupCommitMaxWaitMs` wait before flush when batch is not full |

Tune `JournalGroupCommitMaxWaitMs` for throughput (higher wait → larger batches) and lower it toward `0` for minimal
commit latency.

Benchmark journal persistence on representative hardware before enabling group commit in production.
