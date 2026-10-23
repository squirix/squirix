using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable write coordination for log frames and metadata.</summary>
internal static class FollowerLogDurable
{
    internal static async Task AppendFramesDurableAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        List<FollowerLogEntry> toAppend,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (toAppend.Count == 0)
            return;

        var (durable, offsets, logLength) = ScheduleFrames(owner, toAppend);
        try
        {
            await durable.ConfigureAwait(false);

            // Only after the durable writing succeeds do the in-memory indexes gain the entries,
            // so a crash mid-appending can never leave the index ahead of the file.
            IndexAppendedFrames(journal, owner, toAppend, offsets, logLength);
        }
        catch (Exception ex)
        {
            // A failure after the frames were handed to the storage leaves them possibly on disk, where a restart recovers them as a tail
            // that can commit, and a write after a failed flush is unsafe: a torn leftover behind the next frame can pass for a record.
            // A shutdown that faulted the waiter leaves the write running, so it is no different. The log therefore refuses every
            // further append until the node restarts.
            owner.LatchAppendUnconfirmed(ex);
            throw new GroupLogAppendUnconfirmedException(ex);
        }
    }

    internal static Task PersistMetaAsync(FollowerLogJournal journal, IFollowerLogContext owner, in GroupLogMetadata meta, bool mirrored, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // A non-cancelable scheduling token after the explicit check guarantees the worker always runs.
        return new MetaDurableWork(owner.Acks, journal.Metadata, in meta, mirrored).RunTrackedAsync();
    }

    internal static (Task FirstCopy, Task Mirror) PersistMetaDeferredMirror(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        in GroupLogMetadata meta,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return new DeferredMirrorMetaDurableWork(owner.Acks, journal.Metadata, in meta).RunDeferredTracked();
    }

    internal static async Task<(int Length, List<long> Offsets)> ReplaceLogAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        List<FollowerLogEntry> tail,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var standbySlot = 1 - journal.LogSlot;
        var generation = journal.LogGeneration + 1;
        var work = new ReplaceDurableWork(owner.Acks, owner.Durability, journal.Paths.GetLogSlotPath(standbySlot), generation, tail, owner.Faults);
        try
        {
            await work.RunTrackedAsync().ConfigureAwait(false);
        }
        finally
        {
            // Once the header is flushed the new file is the durable log, even when a later step fails.
            if (work.Sealed)
                journal.UseLog(standbySlot, generation);
        }

        return (work.Length, work.Offsets);
    }

    internal static async Task<int> TruncateFromAsync(FollowerLogJournal journal, IFollowerLogContext owner, ulong logIndex, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!journal.EntryOffsets.TryGetValue(logIndex, out var location))
            throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot truncate from a missing index '{logIndex}'.");

        var released = 0;
        var durable = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var work = new TruncateDurableWork(owner.Acks, owner.Durability, location.Offset, owner.Faults, durable);
            try
            {
                await work.RunTrackedAsync().ConfigureAwait(false);
            }
            finally
            {
                // OnFlushed may throw after the flush completed. The work item records that durable boundary so
                // reservations are released whenever the truncated bytes are no longer recoverable.
                if (durable.Task.IsCompletedSuccessfully)
                    released = owner.Idempotency.ReleaseFromIndex(logIndex);
            }
        }
        catch (Exception ex)
        {
            // A truncate that failed before its durable point may or may not have cut the file, while the memory below is reconciled to
            // the cut. The log cannot tell which, so it refuses further appends until a restart re-reads the file.
            if (!durable.Task.IsCompletedSuccessfully)
                owner.LatchTruncateUnconfirmed(ex);

            throw;
        }
        finally
        {
            // Reconcile the in-memory indexes even when the durable truncate throws (SetLength may already
            // have been applied), so the ready log never validates against a suffix that may no longer be
            // durable. The failure still propagates; the append whose frames were never scheduled is a definite failure.
            journal.RemoveEntriesAbove(logIndex - 1UL);
            owner.LastLogIndex = logIndex - 1;
            owner.LogLength = location.Offset;
        }

        return released;
    }

    /// <summary>Encodes a batch of entries and schedules its durable write without awaiting anything.</summary>
    /// <param name="owner">The owning follower log.</param>
    /// <param name="toAppend">The entries to append, in log order.</param>
    /// <returns>The ack of the scheduled work, the log index and file offset of each frame, and the log length after the batch.</returns>
    /// <exception cref="ReplicaShutdownRefusedException">The owning log was disposed; nothing was scheduled.</exception>
    private static ScheduledFrames ScheduleFrames(IFollowerLogContext owner, List<FollowerLogEntry> toAppend)
    {
        var startOffset = owner.LogLength;
        var (buffer, totalLength, offsets) = EncodeFrames(toAppend, startOffset);

        // The durability worker flushes the exact-size buffer and reports any I/O fault through the ack. The
        // buffer is owned by the work item and returned only inside its Execute; scheduling with a non-cancelable
        // token after the explicit check guarantees the callback always runs and the buffer is returned once the work is scheduled.
        var work = new AppendDurableWork(owner.Acks, owner.Durability, buffer, startOffset, totalLength, owner.Faults);

        owner.Faults.OnFramesScheduling();

        // The classification point is the scheduling: RunTrackedAsync is synchronous up to it, so a throw here means nothing was handed
        // to the storage and the failure is definite. It must stay synchronous up to the scheduling for this to hold. The rented
        // buffer of that path is not returned to the pool; it is collected like any array. That path is reachable only through a narrow
        // race with the dispose, which closes the ack registry.
        try
        {
            return new ScheduledFrames(work.RunTrackedAsync(), offsets, startOffset + totalLength);
        }
        catch (ObjectDisposedException ex)
        {
            throw new ReplicaShutdownRefusedException(nameof(FollowerLog), ex);
        }
    }

    /// <summary>Encodes a batch of entries into one rented buffer and records the on-disk offset of each frame.</summary>
    /// <param name="toAppend">The entries to encode, in log order.</param>
    /// <param name="startOffset">The log file offset the first frame is written at.</param>
    /// <returns>The rented buffer, the encoded length of the batch, and the log index and offset of each frame.</returns>
    private static (byte[] Buffer, int Length, List<KeyValuePair<ulong, long>> Offsets) EncodeFrames(List<FollowerLogEntry> toAppend, long startOffset)
    {
        // The whole batch is encoded into a single contiguous buffer, so the OS
        // performs one sequential writing instead of many small ones.
        var totalLength = 0;
        for (var i = 0; i < toAppend.Count; i++)
            totalLength += GroupLogCodec.ComputeFrameEncodedLength(toAppend[i].Payload.Length);

        var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
        var position = 0;

        // Each frame is encoded at its final on-disk offset, and that offset is remembered,
        // so the in-memory index can serve reads without re-walking the file.
        var offsets = new List<KeyValuePair<ulong, long>>(toAppend.Count);
        for (var i = 0; i < toAppend.Count; i++)
        {
            var entry = toAppend[i];
            var encodedLength = GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length);
            var frameOffset = startOffset + position;
            GroupLogCodec.EncodeFrame(buffer.AsSpan(position, encodedLength), in entry);
            offsets.Add(new KeyValuePair<ulong, long>(entry.LogIndex, frameOffset));
            position += encodedLength;
        }

        return (buffer, totalLength, offsets);
    }

    private static void IndexAppendedFrames(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        List<FollowerLogEntry> appended,
        List<KeyValuePair<ulong, long>> offsets,
        long logLength)
    {
        for (var i = 0; i < offsets.Count; i++)
            journal.AddEntry(appended[i], offsets[i].Value, appended[i].Term);

        owner.LastLogIndex = offsets[^1].Key;
        owner.LogLength = logLength;
    }

    /// <summary>An append batch handed to the storage.</summary>
    /// <param name="Durable">The ack of the scheduled append work.</param>
    /// <param name="Offsets">The log index and file offset of each appended frame.</param>
    /// <param name="LogLength">The log length after the appended frames.</param>
    [Immutable]
    private readonly record struct ScheduledFrames(Task Durable, List<KeyValuePair<ulong, long>> Offsets, long LogLength);

    /// <summary>Durable work whose caller awaits a tracked ack instead of the pool task.</summary>
    /// <remarks>
    /// The pool thread owns the outcome of the operation: it completes the ack with the result or the failure of
    /// <see cref="Run" /> and then untracks it. The only other writer is the shutdown drain of the owning log, which
    /// faults the ack with <see cref="ObjectDisposedException" />; the first writer wins, so a late outcome is a no-op.
    /// </remarks>
    [Immutable]
    private abstract class TrackedDurableWorkBase : IWorkPoolItem
    {
        private readonly TaskCompletionSource _ack = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected TrackedDurableWorkBase(FollowerLogAckRegistry acks)
        {
            Acks = acks;
        }

        /// <summary>Gets the registry that tracks the acks of the owning log until their outcome is written.</summary>
        protected FollowerLogAckRegistry Acks { get; }

        void IWorkPoolItem.Execute()
        {
            // Outcome first, then untrack: an ack is never untracked before its outcome is written, and a drain
            // that takes an already completed ack leaves it as is and does not count it.
            try
            {
                _ack.RunIsolated(this, static work => work.Run());
            }
            finally
            {
                Acks.Complete(_ack);
            }
        }

        /// <summary>Tracks the ack and schedules the work item on a pool thread.</summary>
        /// <remarks>Synchronous up to the scheduling: a throw means nothing was scheduled, which callers rely on to tell a definite failure from an unconfirmed one.</remarks>
        /// <returns>The ack task; the discarded pool task never faults because <see cref="IWorkPoolItem.Execute" /> reports through the ack.</returns>
        /// <exception cref="ObjectDisposedException">The owning log was disposed; nothing was scheduled.</exception>
        [SuppressMessage("Usage", "VSTHRD003", Justification = "The ack is created by this work item and completed only by its own Execute or by the shutdown drain.")]
        internal Task RunTrackedAsync()
        {
            Acks.Track(_ack);
            try
            {
                _ = WorkPool.RunAsync(this, TaskCreationOptions.None, CancellationToken.None);
            }
            catch
            {
                Acks.Complete(_ack);
                throw;
            }

            return _ack.Task;
        }

        /// <summary>Performs the durable operation on the pool thread.</summary>
        protected abstract void Run();
    }

    /// <summary>Background work that durably writes appended frames and flushes them.</summary>
    [Immutable]
    private sealed class AppendDurableWork : TrackedDurableWorkBase
    {
        private readonly byte[] _buffer;
        private readonly GroupLogDurability _durability;
        private readonly IFollowerLogFaultHooks _faults;
        private readonly int _length;
        private readonly long _startOffset;

        internal AppendDurableWork(FollowerLogAckRegistry acks, GroupLogDurability durability, byte[] buffer, long startOffset, int length, IFollowerLogFaultHooks faults)
            : base(acks)
        {
            _durability = durability;
            _buffer = buffer;
            _startOffset = startOffset;
            _length = length;
            _faults = faults;
        }

        protected override void Run()
        {
            try
            {
                _durability.Write(_buffer.AsMemory(0, _length), _startOffset);
            }
            finally
            {
                ArrayPool<byte>.Shared.ReturnCleared(_buffer);
            }

            _faults.OnFrameWritten();
            _durability.Flush();
            _faults.OnFlushed();
        }
    }

    /// <summary>Background work that durably publishes metadata through the metadata store.</summary>
    [Immutable]
    private sealed class MetaDurableWork : TrackedDurableWorkBase
    {
        private readonly GroupLogMetadata _meta;
        private readonly bool _mirrored;
        private readonly GroupMetadataStore _store;

        internal MetaDurableWork(FollowerLogAckRegistry acks, GroupMetadataStore store, in GroupLogMetadata meta, bool mirrored)
            : base(acks)
        {
            _store = store;
            _meta = meta;
            _mirrored = mirrored;
        }

        protected override void Run()
        {
            if (_mirrored)
                _store.PublishMirrored(in _meta);
            else
                _store.Publish(in _meta);
        }
    }

    /// <summary>Background work that durably publishes metadata into one slot, signals it, and then publishes the same record into the other slot.</summary>
    /// <remarks>
    /// Both copies are written by this one work item, so the metadata store is never written concurrently. The ack of the base class is the
    /// mirror copy; the first copy has its own tracked ack, which the shutdown drain faults like any other. A failed first copy completes both
    /// with the failure and writes no mirror copy.
    /// </remarks>
    [Immutable]
    private sealed class DeferredMirrorMetaDurableWork : TrackedDurableWorkBase
    {
        private readonly TaskCompletionSource _firstCopy = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly GroupLogMetadata _meta;
        private readonly GroupMetadataStore _store;

        internal DeferredMirrorMetaDurableWork(FollowerLogAckRegistry acks, GroupMetadataStore store, in GroupLogMetadata meta)
            : base(acks)
        {
            _store = store;
            _meta = meta;
        }

        /// <summary>Tracks both acks and schedules the work item on a pool thread.</summary>
        /// <returns>The ack of the first copy and the ack of the mirror copy.</returns>
        /// <exception cref="ObjectDisposedException">The owning log was disposed; nothing was scheduled.</exception>
        [SuppressMessage("Usage", "VSTHRD003", Justification = "The first-copy ack is created by this work item and completed only by its own Run or by the shutdown drain.")]
        internal (Task FirstCopy, Task Mirror) RunDeferredTracked()
        {
            Acks.Track(_firstCopy);
            try
            {
                return (_firstCopy.Task, RunTrackedAsync());
            }
            catch
            {
                Acks.Complete(_firstCopy);
                throw;
            }
        }

        protected override void Run()
        {
            try
            {
                _store.Publish(in _meta);
            }
            catch (Exception ex)
            {
                _ = _firstCopy.TrySetException(ex);
                Acks.Complete(_firstCopy);
                throw;
            }

            _ = _firstCopy.TrySetResult();
            Acks.Complete(_firstCopy);
            _store.Publish(in _meta);
        }
    }

    /// <summary>Writes and flushes each retained frame into the inactive generation file, seals it, and makes it the live log.</summary>
    /// <remarks>
    /// Frames are encoded into short-lived pooled buffers one at a time, so the retained tail is never materialized into a
    /// single contiguous array (which could overflow <see cref="int" /> or exhaust contiguous memory for a large tail). The superseded
    /// file stays intact until the sealed header of the new one is flushed, so a crash at any point leaves a complete log.
    /// </remarks>
    private sealed class ReplaceDurableWork : TrackedDurableWorkBase
    {
        private readonly GroupLogDurability _durability;
        private readonly IFollowerLogFaultHooks _faults;
        private readonly ulong _generation;
        private readonly string _standbyPath;
        private readonly List<FollowerLogEntry> _tail;

        internal ReplaceDurableWork(
            FollowerLogAckRegistry acks,
            GroupLogDurability durability,
            string standbyPath,
            ulong generation,
            List<FollowerLogEntry> tail,
            IFollowerLogFaultHooks faults)
            : base(acks)
        {
            _durability = durability;
            _standbyPath = standbyPath;
            _generation = generation;
            _tail = tail;
            _faults = faults;
        }

        internal int Length { get; private set; }

        internal List<long> Offsets { get; } = [];

        internal bool Sealed { get; private set; }

        protected override void Run()
        {
            long total = GroupLogSlots.HeaderSize;
            for (var i = 0; i < _tail.Count; i++)
                total += GroupLogCodec.ComputeFrameEncodedLength(_tail[i].Payload.Length);
            if (total > int.MaxValue)
                throw new InvalidOperationException($"Compaction tail of {total} bytes exceeds the maximum log size.");

            var handle = _durability.OpenStandbyOnce(_standbyPath);
            try
            {
                WriteTail(handle, total);
                _faults.OnFlushed();
                GroupLogDurability.Seal(handle, _generation);
                Sealed = true;
                _faults.OnFlushed();

                // Adopt owns the handle from here on, whether or not emptying the superseded file succeeds.
                var sealedHandle = handle;
                handle = null;
                _durability.Adopt(sealedHandle);
            }
            finally
            {
                handle?.Dispose();
            }

            Length = int.CreateTruncating(total);
        }

        private void WriteTail(SafeFileHandle handle, long total)
        {
            long position = GroupLogSlots.HeaderSize;
            for (var i = 0; i < _tail.Count; i++)
            {
                var entry = _tail[i];
                var frameLength = GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length);
                var frameBuffer = ArrayPool<byte>.Shared.Rent(frameLength);
                try
                {
                    GroupLogCodec.EncodeFrame(frameBuffer.AsSpan(0, frameLength), in entry);
                    RandomAccess.Write(handle, frameBuffer.AsSpan(0, frameLength), position);
                }
                finally
                {
                    // The frame bytes are in the open handle; release the pooled
                    // buffer immediately, so a large tail never holds every frame buffer at once.
                    ArrayPool<byte>.Shared.ReturnCleared(frameBuffer);
                }

                Offsets.Add(position);
                position += frameLength;
            }

            RandomAccess.SetLength(handle, total);
            RandomAccess.FlushToDisk(handle);
        }
    }

    /// <summary>Background work that durably truncates the log before a conflicting tail is rewritten.</summary>
    [Immutable]
    private sealed class TruncateDurableWork : TrackedDurableWorkBase
    {
        private readonly GroupLogDurability _durability;
        private readonly TaskCompletionSource<bool> _durable;
        private readonly IFollowerLogFaultHooks _faults;
        private readonly long _length;

        internal TruncateDurableWork(FollowerLogAckRegistry acks, GroupLogDurability durability, long length, IFollowerLogFaultHooks faults, TaskCompletionSource<bool> durable)
            : base(acks)
        {
            _durability = durability;
            _length = length;
            _faults = faults;
            _durable = durable;
        }

        protected override void Run()
        {
            _durability.Truncate(_length);
            _durability.Flush();
            _ = _durable.TrySetResult(true);
            _faults.OnFlushed();
        }
    }
}
