using System;
using System.Buffers;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Segment rolls, batching, and frame writes for the journal event loop.</summary>
[Immutable]
internal sealed class JournalEventLoopSegmentWriter
{
    private readonly MaintenanceCompletion _maintenance;
    private readonly IJournalEventLoopState _owner;
    private readonly IJournalEventLoopRollState _roll;
    private readonly JournalRollTargetProvisioner _rollTarget;

    internal JournalEventLoopSegmentWriter(IJournalEventLoopState owner, IJournalEventLoopRollState roll)
    {
        _owner = owner;
        _roll = roll;
        _rollTarget = new JournalRollTargetProvisioner(owner, roll);
        _maintenance = new MaintenanceCompletion(owner, roll);
    }

    internal void FlushWriteBatch(bool notifyGroupCommit = false)
    {
        if (_owner.WriteBatch.IsEmpty)
            return;

        var registry = _owner.Host.PendingAppends;
        if (registry.AnyAbandoned(_owner.WriteBatch.PendingAppends))
        {
            // A failure to drain reclaimed staged frames: every staged ack is already faulted, so the
            // batch must be dropped instead of written (ghost-writes). Drains are total; hence a
            // single abandoned item proves the whole batch was drained by the same TakeAll.
            // A post-write re-check below covers a drain landing mid-write.
            DropAbandonedBatch();
            return;
        }

        var span = _owner.WriteBatch.ActiveSpan;
        var offset = _owner.ActiveSegmentWrittenBytes;
        WriteBatchSpan(span, offset);

        // Re-check after the writing: a drain may have landed mid-write and faulted the staged acks.
        // Truncating the just-written span keeps faulted frames from replaying; the completions below
        // stay idempotent (Untrack-gated release, TrySet* acks), so this only removes bytes.
        if (registry.AnyAbandoned(_owner.WriteBatch.PendingAppends))
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            DropAbandonedBatch();
            return;
        }

        CompleteWriteBatch(span.Length, offset, notifyGroupCommit);
        return;

        void DropAbandonedBatch()
        {
            var failure = registry.Failure ?? new InvalidOperationException("journal I/O thread failed.");
            for (var i = 0; i < _owner.WriteBatch.PendingAppends.Count; i++)
                FailAppendWorkItem(_owner.WriteBatch.PendingAppends[i], failure);

            _owner.WriteBatch.Clear();
        }
    }

    internal bool ProcessJournalWorkItem(JournalWorkItem item)
    {
        // Maintenance completions dispatch through their owner to keep this dispatcher thin.
        if (_maintenance.TryProcess(item))
            return false;

        if (item.Kind == JournalWorkKind.Append)
        {
            ProcessAppend(item);
            return false;
        }

        if (item.Kind == JournalWorkKind.AppendWithDurability)
        {
            ProcessAppendWithDurability(item);
            return false;
        }

        if (item.Kind == JournalWorkKind.DurabilityCheckpoint)
        {
            FlushWriteBatch();
            _owner.Host.CompleteDurabilityCheckpoint(item);
            return false;
        }

        if (item.Kind == JournalWorkKind.Shutdown)
        {
            FlushWriteBatch();
            _owner.FsyncOnJournalThread();
            return true;
        }

        if (item.Kind != JournalWorkKind.MaintenanceBegin)
            throw new InvalidOperationException("Unknown journal work kind.");
        FlushWriteBatch();
        _owner.FsyncOnJournalThread();
        _roll.SetActiveSegmentPath(null);
        CompleteJournalWorkItem(item);
        return false;
    }

    internal bool TryAcceptAppendIntoBatch(JournalWorkItem item, out bool rollDeferred)
    {
        rollDeferred = false;
        if (RejectAbandonedAppend(item))
            return true;

        try
        {
            EnsureSegmentOpen();
            var needsRoll = ShouldRollSegmentForAppend(item.FrameLength);
            var headerDelta = _rollTarget.GetAppendHeaderDelta(needsRoll, out var existingTargetLength);
            var rollTargetPreexists = existingTargetLength != null;
            var requiredBytes = item.FrameLength + headerDelta;
            _owner.Policy.EnsureAppendCapacityOrThrow(GetEffectiveJournalTotalBytes(), requiredBytes);
            if (needsRoll)
            {
                FlushWriteBatch();
                BeginSegmentRollOnJournalThread(rollTargetPreexists);
                rollDeferred = true;
                return false;
            }

            if (_owner.WriteBatch.TryStageAppend(in item))
                return true;

            FlushWriteBatch();
            return _owner.WriteBatch.TryStageAppend(in item);
        }
        catch (JournalCapacityExceededException ex)
        {
            FailAppendWorkItem(item, ex);
            return true;
        }
    }

    internal bool TryCompletePendingSegmentRoll()
    {
        if (!_roll.TryConsumeSegmentRollCompletion())
            return false;

        CompleteSegmentRollOnJournalThread();
        return true;
    }

    private static void CompleteJournalWorkItem(JournalWorkItem item) => _ = item.Ack?.TrySetResult();

    private void BeginSegmentRollOnJournalThread(bool rollTargetPreexists)
    {
        if (_roll.SegmentRollInFlight)
            return;

        _owner.FsyncOnJournalThread();

        // The roll target segment is created durably before the manifest advertises
        // CurrentJournal = target, so a crash can never leave the manifest ahead of the last
        // available segment (issue #439). The active segment stays on the old full segment until
        // the manifest publication succeeds, which keeps deferring appending via ShouldRollSegmentForAppend.
        var targetSegmentIndex = _roll.CurrentSegmentIndex + 1;
        var targetPath = _rollTarget.BuildRollTargetPath();

        // Roll capacity uses in-memory counters maintained by the single journal-thread writer instead
        // of rescanning the directory (two EnumerateFiles passes plus a stat per segment) on the hot
        // roll path. The counters are seeded at startup and resynced after compaction (MaintenanceEnd).
        // A pre-created target from a crashed roll is already counted, so it must not consume another slot.
        if (rollTargetPreexists)
            _owner.Policy.EnsurePrecreatedRollCapacityOrThrow(_roll.JournalSegmentCount, _owner.JournalTotalBytes);
        else
            _owner.Policy.EnsureRollCapacityOrThrow(_roll.JournalSegmentCount, _owner.JournalTotalBytes);

        _rollTarget.PrepareRollTargetSegment(targetSegmentIndex, targetPath);

        _roll.SetPendingRollTargetSegmentIndex(targetSegmentIndex);
        _roll.SetSegmentRollInFlight(true);
        _owner.Host.PublishRoll(_roll.PendingRollTargetSegmentIndex);
    }

    private void CompleteSegmentRollOnJournalThread()
    {
        _roll.SetCurrentSegmentIndex(_roll.PendingRollTargetSegmentIndex);
        var segmentPath = JournalReadPath.BuildSegmentPath(_owner.Options.DataDir, _roll.CurrentSegmentIndex);
        _roll.SetActiveSegmentPath(segmentPath);

        // The target was created durably before the roll manifest was published
        // (BeginSegmentRollOnJournalThread), so only open it here: never truncate it and never
        // rewrite an existing header.
        _owner.SegmentWriter.OpenSegment(segmentPath, true);
        if (_owner.SegmentWriter.Length == 0)
        {
            // Defensive: a headerless target (not expected with atomic pre-create). The empty file was
            // already counted, so only its new header bytes enter the total.
            Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
            JournalFraming.WriteFileHeader(header);
            _owner.SegmentWriter.Write(header, 0);
            _owner.SegmentWriter.Fsync();
            _owner.AddJournalTotalBytes(JournalFraming.FileHeaderSize);
        }

        _owner.SetActiveSegmentWrittenBytes(_owner.SegmentWriter.Length);
        _owner.SetDirty(false);
        _roll.SetSegmentRollInFlight(false);
    }

    private void CompleteWriteBatch(int spanLength, long offset, bool notifyGroupCommit)
    {
        _owner.SetActiveSegmentWrittenBytes(offset + spanLength);
        _owner.AddJournalTotalBytes(spanLength);
        _owner.SetDirty(true);

        for (var i = 0; i < _owner.WriteBatch.PendingAppends.Count; i++)
        {
            // Release is Untrack-gated: when a failure drain won the race, it already released the
            // buffer and decremented the counter. Completing the ack is always safe (TrySet*).
            ReleaseQueuedAppendResources(_owner.WriteBatch.PendingAppends[i]);
            CompleteJournalWorkItem(_owner.WriteBatch.PendingAppends[i]);
        }

        _owner.WriteBatch.Clear();

        if (notifyGroupCommit && _owner.Options.IsJournalGroupCommitEnabled)
            _owner.GroupCommit?.DrainDueBatchesOnJournalThread();
    }

    private void EnsureSegmentOpen()
    {
        // _activeSegmentWrittenBytes is authoritative: the journal thread is the sole writer and
        // advances it after every Write. No per-call stat/lseek of the writer length is needed.
        if (_roll.ActiveSegmentPath != null)
            return;

        var segmentPath = JournalReadPath.BuildSegmentPath(_owner.Options.DataDir, _roll.CurrentSegmentIndex);
        _roll.SetActiveSegmentPath(segmentPath);
        var append = File.Exists(segmentPath);
        _owner.SegmentWriter.OpenSegment(segmentPath, append);
        if (_owner.SegmentWriter.Length == 0)
        {
            Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
            JournalFraming.WriteFileHeader(header);
            _owner.SegmentWriter.Write(header, 0);

            // Mirror CompleteSegmentRollOnJournalThread: header bytes must enter JournalTotalBytes so
            // EnsureAppendCapacityOrThrow / UsedBytes see the same on-disk size as ActiveSegmentWrittenBytes.
            _owner.AddJournalTotalBytes(JournalFraming.FileHeaderSize);
            if (!append)
                _roll.IncrementJournalSegmentCount();
        }

        _owner.SetActiveSegmentWrittenBytes(_owner.SegmentWriter.Length);
    }

    private void FailAppendWorkItem(JournalWorkItem item, Exception error)
    {
        ReleaseQueuedAppendResources(item);
        _ = item.Ack?.TrySetException(error);
    }

    private long GetEffectiveActiveSegmentBytes() => _owner.ActiveSegmentWrittenBytes + _owner.WriteBatch.StagedByteLength;

    private long GetEffectiveJournalTotalBytes() => _owner.JournalTotalBytes + _owner.WriteBatch.StagedByteLength;

    private void ProcessAppend(JournalWorkItem item)
    {
        // Admitted before a failure drain that already faulted it; failing again is idempotent.
        if (RejectAbandonedAppend(item))
            return;

        try
        {
            WriteAppendFrame(item);
        }
        catch (JournalCapacityExceededException ex)
        {
            FailAppendWorkItem(item, ex);
            return;
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            ReleaseQueuedAppendResources(item);
            _ = item.Ack?.TrySetException(ex);
            throw;
        }

        ReleaseQueuedAppendResources(item);

        if (_owner.Options.IsJournalGroupCommitEnabled)
            _owner.GroupCommit?.DrainDueBatchesOnJournalThread();

        CompleteJournalWorkItem(item);
    }

    private void ProcessAppendWithDurability(JournalWorkItem item)
    {
        var ack = ThrowHelper.Required(item.Ack, "AppendWithDurability work item is missing a durability ack.");

        // Admitted before a failure drain that already faulted it; failing again is idempotent.
        if (RejectAbandonedAppend(item))
            return;

        try
        {
            WriteAppendFrame(item);
            _owner.FsyncOnJournalThread();
            _ = ack.TrySetResult();
        }
        catch (JournalCapacityExceededException ex)
        {
            FailAppendWorkItem(item, ex);
            return;
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            ReleaseQueuedAppendResources(item);
            _ = ack.TrySetException(ex);
            throw;
        }

        ReleaseQueuedAppendResources(item);
    }

    private bool RejectAbandonedAppend(JournalWorkItem item)
    {
        var registry = _owner.Host.PendingAppends;
        if (!registry.IsAbandoned(item))
            return false;

        // Staging gate: after a failure drain, nothing new may enter the batch. The drain already faulted the item; failing it again here is idempotent.
        FailAppendWorkItem(item, registry.Failure ?? new InvalidOperationException("journal I/O thread failed."));
        return true;
    }

    private void ReleaseQueuedAppendResources(JournalWorkItem item)
    {
        // Only the Untrack winner releases: a failure drain that won already returned the buffer
        // and decremented the counter while quarantining the buffer.
        if (!_owner.Host.PendingAppends.Untrack(item, out var entry) || entry == null)
            return;

        var frameBytes = entry.FrameBytes;
        try
        {
            _owner.Host.DecrementQueuedAppends();
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
        }
    }

    private bool ShouldRollSegmentForAppend(int incomingFrameBytes) => _owner.Policy.ShouldRollSegment(GetEffectiveActiveSegmentBytes(), incomingFrameBytes);

    private void TruncateActiveSegmentAfterFailedFrame(long frameStart)
    {
        _owner.SegmentWriter.Truncate(frameStart);
        _owner.SetActiveSegmentWrittenBytes(frameStart);
        _owner.SetDirty(frameStart > 0);
    }

    private void WriteAppendFrame(JournalWorkItem item)
    {
        var frameBytes = ThrowHelper.Required(item.FrameBytes, "Append work item is missing frame bytes.");
        EnsureSegmentOpen();
        var needsRoll = ShouldRollSegmentForAppend(item.FrameLength);
        var requiredBytes = needsRoll ? item.FrameLength + JournalFraming.FileHeaderSize : item.FrameLength;
        _owner.Policy.EnsureAppendCapacityOrThrow(GetEffectiveJournalTotalBytes(), requiredBytes);
        if (needsRoll)
            throw new InvalidOperationException("append requires a segment roll; use the journal thread deferral path.");
        var offset = _owner.ActiveSegmentWrittenBytes;
        try
        {
            _owner.SegmentWriter.Write(frameBytes.AsSpan(0, item.FrameLength), offset);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }

        _owner.SetActiveSegmentWrittenBytes(offset + item.FrameLength);
        _owner.AddJournalTotalBytes(item.FrameLength);
        _owner.SetDirty(true);
    }

    private void WriteBatchSpan(ReadOnlySpan<byte> span, long offset)
    {
        try
        {
            _owner.SegmentWriter.Write(span, offset);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
    }

    /// <summary>
    /// Durably provisions the next journal segment before its roll manifest is published, so a crash can
    /// never leave the manifest ahead of the last available segment (issue #439). All members run on the
    /// dedicated <c language="csharp">squirix-journal-io</c> thread.
    /// </summary>
    [Immutable]
    private sealed class JournalRollTargetProvisioner
    {
        private readonly IJournalEventLoopState _owner;
        private readonly IJournalEventLoopRollState _roll;

        internal JournalRollTargetProvisioner(IJournalEventLoopState owner, IJournalEventLoopRollState roll)
        {
            _owner = owner;
            _roll = roll;
        }

        /// <summary>Builds the roll target path for the segment following the current one.</summary>
        /// <returns>Absolute path of the roll target segment file.</returns>
        internal string BuildRollTargetPath() => JournalReadPath.BuildSegmentPath(_owner.Options.DataDir, _roll.CurrentSegmentIndex + 1);

        /// <summary>Computes the header bytes a pending roll is about to add for an appended capacity check.</summary>
        /// <param name="needsRoll">Whether the appending requires a segment roll.</param>
        /// <param name="len">Current length of the roll target segment file, when a roll is pending.</param>
        /// <returns>Full header for a new target, replacement delta for a torn or empty one, zero otherwise.</returns>
        internal int GetAppendHeaderDelta(bool needsRoll, out long? len)
        {
            len = null;
            if (!needsRoll)
                return 0;

            // Reserve exactly the header bytes the roll is about to add, mirroring
            // PrepareRollTargetSegment: full header for a new target, replacement delta for a
            // torn/empty one, none for a usable target.
            len = GetRollTargetExistingLength();
            return len switch
            {
                null => JournalFraming.FileHeaderSize,
                _ => len.Value < JournalFraming.FileHeaderSize ? JournalFraming.FileHeaderSize - Convert.ToInt32(len.Value) : 0,
            };
        }

        /// <summary>Ensures the roll target segment exists durably, creating or replacing it atomically when needed.</summary>
        /// <param name="targetSegmentIndex">One-based index of the roll target segment.</param>
        /// <param name="targetPath">Absolute path of the roll target segment file.</param>
        internal void PrepareRollTargetSegment(int targetSegmentIndex, string targetPath)
        {
            // A valid pre-created target (crash after a previous pre-creation, before its manifest publication)
            // is reused as-is: startup stats already counted it, and replacing it would needlessly churn
            // the disk.
            if (HasUsableRollTargetHeader(targetPath))
                return;

            // Same probe as GetRollTargetExistingLength (targetPath is BuildRollTargetPath()):
            // a missing or unstatable file counts as new, a leftover counts by its delta.
            var preExistingLength = GetRollTargetExistingLength();

            PublishRollTargetHeader(targetSegmentIndex, targetPath);

            if (preExistingLength == null)
            {
                _owner.AddJournalTotalBytes(JournalFraming.FileHeaderSize);
                _roll.IncrementJournalSegmentCount();
            }
            else
            {
                // Replaced a torn or empty leftover: the file itself was already counted, only its byte
                // delta is new.
                _owner.AddJournalTotalBytes(JournalFraming.FileHeaderSize - preExistingLength.Value);
            }
        }

        private static bool HasUsableRollTargetHeader(string targetPath)
        {
            long length;
            try
            {
                if (!File.Exists(targetPath))
                    return false;

                length = new FileInfo(targetPath).Length;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                return false;
            }

            // A longer file already holds content: never replace it here, reuse as-is. Startup recovery
            // validated its header before any roll could run, and nothing else writes the file while appends
            // are deferred, so revalidating here would add I/O without changing the only safe action.
            // (see PrepareRollTargetSegment).
            if (length > JournalFraming.FileHeaderSize)
                return true;

            if (length != JournalFraming.FileHeaderSize)
                return false;

            try
            {
                using var handle = File.OpenHandle(targetPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan);
                JournalFraming.ReadAndValidateSegmentHeader(handle, 0);
                return true;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException)
            {
                return false;
            }
        }

        /// <summary>Gets the current length of the roll target segment file.</summary>
        /// <returns>File length in bytes, or <see langword="null" /> when the target does not exist or cannot be stated.</returns>
        private long? GetRollTargetExistingLength()
        {
            try
            {
                var targetPath = BuildRollTargetPath();
                return !File.Exists(targetPath) ? null : new FileInfo(targetPath).Length;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                return null;
            }
        }

        private void PublishRollTargetHeader(int targetSegmentIndex, string targetPath)
        {
            // Stage the header under a temp name and publish it atomically: a crash must never leave a
            // partially written header under the final segment name, because a 1..FileHeaderSize-1 byte
            // trailing segment fails recovery replay. The temp suffix keeps it invisible to enumeration.
            // The active writer is deliberately untouched here; it stays on the old segment until the
            // manifest publication succeeds, so a failed roll leaves the writer, path, and offsets consistent.
            var tmpPath = JournalReadPath.BuildRollTempPath(_owner.Options.DataDir, targetSegmentIndex);
            WriteRollTargetHeaderFile(tmpPath);
            _ = FileEx.PublishFile(tmpPath, targetPath);
        }

        private void WriteRollTargetHeaderFile(string tmpPath)
        {
            using var writer = JournalSegmentWriterFactory.Create(_owner.Options.JournalPlatformBackend);
            writer.OpenSegment(tmpPath, false);
            Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
            JournalFraming.WriteFileHeader(header);
            writer.Write(header, 0);
            writer.Fsync();
        }
    }

    /// <summary>Maintenance completion on the journal thread: abort resyncing and end pointer install.</summary>
    [Immutable]
    private sealed class MaintenanceCompletion
    {
        private readonly IJournalEventLoopState _owner;
        private readonly IJournalEventLoopRollState _roll;

        internal MaintenanceCompletion(IJournalEventLoopState owner, IJournalEventLoopRollState roll)
        {
            _owner = owner;
            _roll = roll;
        }

        /// <summary>Tries to complete a maintenance abort or end work item.</summary>
        /// <param name="item">Work item to inspect.</param>
        /// <returns><see langword="true" /> when the item was a maintenance completion.</returns>
        internal bool TryProcess(JournalWorkItem item)
        {
            if (item.Kind == JournalWorkKind.MaintenanceAbort)
            {
                ApplyAbort(item);
                return true;
            }

            if (item.Kind != JournalWorkKind.MaintenanceEnd)
                return false;
            CompleteEnd(item);
            return true;
        }

        /// <summary>Applies a maintenance abort: resyncs layout-wide counters from disk without installing reset pointers.</summary>
        /// <param name="item">Abort work item. Its reset fields are always zero and must stay unread.</param>
        private void ApplyAbort(JournalWorkItem item)
        {
            // The abort carries no reset pointers by construction (the factory pins them to zero):
            // against a torn layout, only the layout-wide counters are well-defined, so the per-segment
            // written-bytes counter is left alone (the next EnsureSegmentOpen overwrites it anyway) and
            // the resyncing exists for observability on an already-failed pipeline.
            try
            {
                var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(_owner.Options.DataDir);
                _owner.SetJournalTotalBytes(totalBytes);
                _roll.SetJournalSegmentCount(segmentCount);
                _owner.SetDirty(false);
            }
            catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException)
            {
                _ = item.Ack?.TrySetException(ex);
                return;
            }
            catch (Exception unexpectedEx) when (unexpectedEx is not (ArgumentException or IOException or UnauthorizedAccessException))
            {
                // Total guard: the abort must never escape the journal thread, no matter how exotic
                // the scan failure is. The filter also keeps CA1031 silent: the preceding catch
                // already handles the listed types, so this arm is the documented remainder.
                // Faulting the ack keeps the failure explicit; the producer side
                // treats any ack failure as suppressed and still fails loudly with the original error.
                _ = item.Ack?.TrySetException(unexpectedEx);
                return;
            }

            _ = item.Ack?.TrySetResult();
        }

        /// <summary>Installs maintenance end reset pointers and resyncs capacity counters from the rewritten layout.</summary>
        /// <param name="item">Maintenance end work item carrying the reset pointers.</param>
        /// <exception cref="InvalidOperationException">The on-disk resync scan failed; the pipeline failed loudly.</exception>
        private void CompleteEnd(JournalWorkItem item)
        {
            _roll.SetCurrentSegmentIndex(item.ResetSegmentIndex);
            _owner.Host.SetNextSequence(item.ResetSequence);
            _owner.SetActiveSegmentWrittenBytes(0);
            _owner.SetDirty(false);

            // Compaction rewrote the segment set on disk; resync the in-memory capacity counters
            // (used by the hot roll path) from the new on-disk layout.
            try
            {
                var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(_owner.Options.DataDir);
                _owner.SetJournalTotalBytes(totalBytes);
                _roll.SetJournalSegmentCount(segmentCount);
            }
            catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException)
            {
                // The resynced scan can fail with error types the journal thread loop does not
                // handle: an escape would kill the thread without latching the pipeline, hanging
                // the End waiter and producers. Fault the ack with the root cause, fail the
                // pipeline loudly, then exit through the loop handler by throwing a type it
                // catches. The pointers above stay installed, but a failed pipeline requires a
                // restart, so nothing proceeds on the torn mix.
                _ = item.Ack?.TrySetException(ex);
                _owner.Host.FailPipeline(ex);
                throw new InvalidOperationException("maintenance end resync failed.", ex);
            }

            CompleteJournalWorkItem(item);
        }
    }
}
