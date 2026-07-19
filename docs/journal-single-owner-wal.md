# Journal single-owner WAL pipeline

## Status

Accepted — implemented in `JournalCoordinator` (`JournalBackend.Pipelined`).

## Context

High-throughput write-ahead logs converge on **single ownership**: one thread owns the file handle, byte
offset, segment rotation, fsync, and durability batching. Producers serialize records and enqueue work;
they never touch the segment file.

Squirix persistence uses a pipelined binary journal (`docs/journal-binary-format.md`) coordinated by
`JournalCoordinator` on a dedicated `squirix-journal-io` thread.

## Architecture

```text
Mutator threads
      ↓  serialize frame + enqueue
BoundedJournalRing (MPSC, bounded backpressure)
      ↓
Dedicated journal I/O thread (event loop)
      ↓  batched RandomAccess.Write
Segment file(s)
      ↓  group commit / fsync
Complete durability waiters
```

Manifest roll notifications are handed off to a dedicated manifest publisher thread. The journal thread
starts the publish asynchronously and opens the next segment only after manifest success, without
blocking on manifest disk I/O.

## Invariants (must hold for all journal changes)

1. **Disk ownership** — Only the `squirix-journal-io` thread calls `IJournalSegmentWriter.Write`,
   `Fsync`, `Truncate`, and `OpenSegment` for the active journal segment.

2. **Offset ownership** — Only the journal thread reads or updates `_activeSegmentWrittenBytes` and
   decides when to roll segments.

3. **Rotation ownership** — Segment roll (fsync, open next segment, write header) runs entirely on the
   journal thread.

4. **Fsync ownership** — Durability flushes (`Fsync` / group commit) run only on the journal thread.

5. **Producer hot path** — Mutator threads must not acquire locks on the segment file or call
   `WriteAsync` on a shared `FileStream`. Producers may: allocate sequence (lock-free), serialize,
   enqueue to `BoundedJournalRing`, and await completion.

## Allowed cross-thread mechanisms

| Mechanism                      | Purpose                                                          |
|--------------------------------|------------------------------------------------------------------|
| `BoundedJournalRing`           | MPSC queue with bounded `SemaphoreSlim` backpressure             |
| `Interlocked` / `Volatile`     | Metrics, sequence allocation, queued-append counters             |
| `JournalDurabilityGroupCommit` | Batches durability waiters; deadline evaluated on journal thread |
| `JournalStartupGate`           | Blocks appends until recovery completes                          |
| `_mutationGate`                | Snapshot barrier and exclusive maintenance (not per-append)      |
| Manifest roll queue            | Async handoff of roll metadata to manifest publisher thread      |

## Out of scope for the journal thread

- Cache eviction, entry encoding policy, and snapshot payload construction
- Journal compaction body rewrite (runs under `ExecuteMaintenanceExclusiveAsync` after journal releases
  the active segment)
- Recovery replay (`JournalReadPath`, `BinaryJournalSegmentReader`)

## Consequences

- Reasoning about durability reduces to one thread plus a bounded queue.
- Group commit timer and write batching live in the journal event loop (no `Task.Delay` on the flush
  path).
- Manifest roll is eventually consistent on disk immediately after roll; recovery still scans on-disk
  segment indices when manifest lags. The journal thread waits for manifest success before opening the
  next segment, but does not block on manifest disk I/O.
- Violating any invariant (e.g. shared `FileStream`, second writer thread) requires an explicit design
  change and doc update.

## References

- [journal-binary-format.md](journal-binary-format.md)
- [journal-group-commit.md](journal-group-commit.md)
- [persistence.md](persistence.md)
