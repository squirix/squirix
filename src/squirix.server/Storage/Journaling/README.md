# Journaling & Durability Subsystem

Server-side append-only journal, single-writer event loop, and the durability/ack ownership that lets external callers await persistence before an in-memory cache apply.

This document is an architecture review of the `issue-400` rewrite (per-wait
`TaskCompletionSource` durability acks, replacing the pooled `PooledAck`). It records why the subsystem is still fragile after that rewrite.

## Data flow

```
external thread
  -> JournalCoordinatorAppendPipeline.Encode(frame)
  -> BoundedJournalRing.EnqueueAsync(item)            (MPMC ring, slot-limited)
       -> JournalEventLoop (single journal thread) drains the ring FIFO
            -> JournalEventLoopSegmentWriter stages into WriteBatch / writes segment
            -> fsync (journal thread)
            -> item.Completion.TrySetResult / TrySetException   (resolves external await)
```

There are **three independent completion paths**, each owning a
`TaskCompletionSource` (ack) with its own lifetime rules:

1. **Per-append durability (group-commit disabled).** `AppendWithDurability`
   item -> write + `FsyncOnJournalThread()` -> `Completion.TrySetResult`. (`JournalEventLoopSegmentWriter.ProcessAppendWithDurability`,
   `JournalCoordinator.AppendRecordWithDurabilityCoreAsync`.)
2. **Group commit.** The append is staged into the write batch and its
   `Completion` resolves as *staged* (after the bytes are written, before the fsync); durability is awaited separately via `JournalDurabilityGroupCommit
   .AwaitCommitAsync` (its own `TaskCompletionSource`).
3. **Forced flush / maintenance.** `DurabilityCheckpoint` and
   `MaintenanceBegin|End` items carry their own completion, resolved after an fsync. (`JournalDurabilityCoordinator.CompleteCheckpointOnJournalThread`,
   `EnqueueMaintenanceAsync`.)

## Why the subsystem is still fragile (root causes)

**1. Duplicated ack lifetime ownership (two copies of the same mechanism).**
`DurabilityAckRegistry` (`DurabilityAckRegistry.cs:10`, exposed via
`JournalCoordinator.DurabilityAcks`, `JournalCoordinator.cs:76`) and the internal `_waits` list inside `JournalDurabilityGroupCommit`
(`JournalDurabilityGroupCommit.cs:22-24`) **both** hand-roll
`List<TaskCompletionSource> + spare-swap + _failure latch + lock`. The same lifetime pattern is written twice with diverging semantics. The two failure latches can desync (group
commit failed, registry did not, or vice versa), and there is no single point that observes the full set of pending acks.

**2. `TrySetResult` / `TrySetException` results are discarded (`_ = ...`).**
`JournalEventLoopSegmentWriter.cs:122` (`_ = item.Completion?.TrySetResult()`),
`:217`, `JournalDurabilityCoordinator.cs:74` / `:128`,
`JournalDurabilityGroupCommit.cs:100` / `:134` / `:142` — the return value is always ignored. If a completion is completed twice (success on one path + exception on another — e.g.
a group-commit batch completes successfully while
`FailPendingDurabilityAcks` / `CancelPendingCore` sets an exception concurrently, or the reverse), the second call is silently dropped. Which outcome wins depends on ordering, not
correctness. Neither tests nor runtime can detect the wrong winner. This is the primary fragility: silent incorrect completion.

**3. Asymmetric `JournalWorkItem.Completion` contract across modes.**
In group-commit mode `Append` carries `appendCompletion`, resolved in
`CompleteWriteBatch` -> `CompleteStagedAppend`
(`JournalEventLoopSegmentWriter.cs:156-185`) **immediately after the batch is written, BEFORE** `DrainDueBatchesOnJournalThread` -> fsync (`:183-184`,
`JournalDurabilityGroupCommit.cs:147-164`). So `Completion` means *"staged into batch"*, not *"durable"*; durability is awaited separately via
`GroupCommit.AwaitCommitAsync` (`JournalCoordinator.cs:336-339`). In group-commit-disabled mode `AppendWithDurability.Completion` means *"fsynced"*. One call
(`AppendPutAndAwaitDurabilityAsync`) has **two different completion contracts** depending on a flag, sharing the same `Completion` field. Current correctness rests on
`EnqueueAppendAsync` awaiting both sequentially (`JournalCoordinator.cs:405` + the caller's `AwaitCommitAsync`). Easy to break on the next refactor.

**4. No global shutdown gate before enqueue.**
`JournalCoordinator.DisposeAsync` (`JournalCoordinator.cs:174-205`) does not set a flag that rejects new appends *before* they enter the ring. The registry's
`_failure` is set only **after** `AwaitJournalThreadDuringDisposeAsync` (thread join). Between dispose start and `_failure` being set, a new append passes `Add`
(not yet failed) and is enqueued; the thread is already dead -> its completion never resolves -> **caller hangs**. `Add` throws `InvalidOperationException` only if `_failure` is
already set, so the race window exists. There is no atomic gate closed at the start of `DisposeAsync` and checked in `Enqueue*` before
`Ring.EnqueueAsync`.

**5. Ring lifecycle is decoupled from the coordinator; best-effort notify hides it.**
`BoundedJournalRing.NotifyWorkAvailable` (`BoundedJournalRing.cs:56-73`) catches
`ObjectDisposedException` and sets `_disposed = 1` — silently swallowing the signal. The comment claims a missed wake is harmless; that holds only while the loop is alive. The fact
that a post-dispose notify is silently dropped hides that the ring and the coordinator have independent dispose orderings and share
`_workSignal` with no single owner. Any future path that needs a notify after a partial dispose yields a silent hang.

**6. Durability correctness rests on implicit FIFO + comments, not a mechanism.**
`JournalDurabilityCoordinator.CompleteCheckpointOnJournalThread`
(`JournalDurabilityCoordinator.cs:63-77`) completes **only its own** ack after the fsync, leaving *"foreign waits pending"* — an invariant spread across ring -> write batch ->
group-commit batch and captured only in comments. There is no structure enforcing that an ack cannot complete before it covers earlier frames. Any reordering silently breaks the
guarantee.

**7. The central mechanism has no unit tests.**
Per CodeGraph, `DurabilityAckRegistry` has **no covering tests**. Group-commit, checkpoint-ownership, and ring tests exist, but the central ack-ownership registry does not. The
fragility persists in part because the most important state machine is only exercised by integration / e2e runs, which rarely catch completion races.

## What the `issue-400` rewrite did and did not fix

The rewrite replaced the pooled `PooledAck` with per-wait
`TaskCompletionSource` instances. It did **not** remove the root causes: ack ownership is still fragmented (registry + group-commit + per-item), completions are still
fire-and-forget discards, there is still no shutdown gate, and correctness still depends on implicit ordering. Hence the subsystem remains fragile.

## Remediation priorities

- **Centralize ack lifetime.** One owner registry; group commit and per-append reuse it instead of keeping their own `_waits` / failure latch.
- **Stop discarding `TrySetResult` / `TrySetException`.** Check the return value and log/assert on a second completion so a wrong winner is observable (at least in debug /
  telemetry).
- **Add an atomic `_shuttingDown` flag**, set at the start of `DisposeAsync` and checked in `Enqueue*` before `Ring.EnqueueAsync` (reject new appends and resolve them with failure
  before they enter the ring).
- **Unify the `Completion` contract**: always mean *"durable"*, or split into two explicitly typed fields (staged vs durable) so group-commit and non-group-commit modes cannot be
  confused.
- **Cover `DurabilityAckRegistry` with unit tests**: double-complete, fail-after-register race, remove-after-complete.

## Key files

| File                                        | Role                                                               |
|---------------------------------------------|--------------------------------------------------------------------|
| `JournalCoordinator.cs`                     | Append pipeline, dispose ordering, `DurabilityAcks` registry       |
| `JournalCoordinatorAppendPipeline` (nested) | Encode + enqueue + await completion                                |
| `BoundedJournalRing.cs`                     | MPMC slot-limited handoff between producers and the journal thread |
| `JournalEventLoop.cs`                       | Single-consumer drain loop, group-commit deadline                  |
| `JournalEventLoopSegmentWriter.cs`          | Frame write, batch flush, fsync, per-item completion               |
| `JournalDurabilityCoordinator.cs`           | Checkpoint / maintenance / pipeline-failure handling               |
| `JournalDurabilityGroupCommit.cs`           | Batched fsync sharing, group-commit acks                           |
| `DurabilityAckRegistry.cs`                  | Pending durability waits + failure latch                           |
| `JournalWorkItem.cs`                        | Ring item carrying the `Completion` source                         |
