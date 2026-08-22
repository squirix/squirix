using System;
using System.Buffers;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Segment rolls, batching, and frame writes for the journal event loop.</summary>
[Immutable]
internal sealed class JournalEventLoopSegmentWriter
{
    private readonly IJournalEventLoopState _owner;
    private readonly IJournalEventLoopRollState _roll;

    internal JournalEventLoopSegmentWriter(IJournalEventLoopState owner, IJournalEventLoopRollState roll)
    {
        _owner = owner;
        _roll = roll;
    }

    internal void FlushWriteBatch(bool notifyGroupCommit = false)
    {
        if (_owner.WriteBatch.IsEmpty)
            return;

        var span = _owner.WriteBatch.ActiveSpan;
        var offset = _owner.ActiveSegmentWrittenBytes;
        WriteBatchSpan(span, offset);
        CompleteWriteBatch(span.Length, offset, notifyGroupCommit);
    }

    internal bool ProcessJournalWorkItem(JournalWorkItem item)
    {
        switch (item.Kind)
        {
            case JournalWorkKind.Append:
                ProcessAppend(item);
                return false;

            case JournalWorkKind.AppendWithDurability:
                ProcessAppendWithDurability(item);
                return false;

            case JournalWorkKind.DurabilityCheckpoint:
                FlushWriteBatch();
                _owner.Host.CompleteDurabilityCheckpoint(item);
                return false;

            case JournalWorkKind.Shutdown:
                FlushWriteBatch();
                _owner.FsyncOnJournalThread();
                return true;

            case JournalWorkKind.MaintenanceBegin:
                FlushWriteBatch();
                _owner.FsyncOnJournalThread();
                _roll.SetActiveSegmentPath(null);
                CompleteJournalWorkItem(item);
                return false;

            case JournalWorkKind.MaintenanceEnd:
                _roll.SetCurrentSegmentIndex(item.ResetSegmentIndex);
                _owner.Host.SetNextSequence(item.ResetSequence);
                _owner.SetActiveSegmentWrittenBytes(0);
                _owner.SetDirty(false);

                // Compaction rewrote the segment set on disk; resync the in-memory capacity counters
                // (used by the hot roll path) from the new on-disk layout.
                var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(_owner.Options.DataDir);
                _owner.SetJournalTotalBytes(totalBytes);
                _roll.SetJournalSegmentCount(segmentCount);
                CompleteJournalWorkItem(item);
                return false;

            default:
                throw new InvalidOperationException("Unknown journal work kind.");
        }
    }

    internal bool TryAcceptAppendIntoBatch(JournalWorkItem item, out bool rollDeferred)
    {
        rollDeferred = false;
        try
        {
            EnsureSegmentOpen();
            var needsRoll = ShouldRollSegmentForAppend(item.FrameLength);
            var requiredBytes = needsRoll ? item.FrameLength + JournalFraming.FileHeaderSize : item.FrameLength;
            _owner.Policy.EnsureAppendCapacityOrThrow(GetEffectiveJournalTotalBytes(), requiredBytes);
            if (needsRoll)
            {
                FlushWriteBatch();
                BeginSegmentRollOnJournalThread();
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

    private void BeginSegmentRollOnJournalThread()
    {
        if (_roll.SegmentRollInFlight)
            return;

        _owner.FsyncOnJournalThread();

        // Roll capacity uses in-memory counters maintained by the single journal-thread writer instead
        // of rescanning the directory (two EnumerateFiles passes plus a stat per segment) on the hot
        // roll path. The counters are seeded at startup and resynced after compaction (MaintenanceEnd).
        _owner.Policy.EnsureRollCapacityOrThrow(_roll.JournalSegmentCount, _owner.JournalTotalBytes);
        _roll.SetPendingRollTargetSegmentIndex(_roll.CurrentSegmentIndex + 1);
        _roll.SetSegmentRollInFlight(true);
        _owner.Host.PublishRoll(_roll.PendingRollTargetSegmentIndex);
    }

    private void CompleteSegmentRollOnJournalThread()
    {
        _roll.SetCurrentSegmentIndex(_roll.PendingRollTargetSegmentIndex);
        var segmentPath = JournalReadPath.BuildSegmentPath(_owner.Options.DataDir, _roll.CurrentSegmentIndex);
        _roll.SetActiveSegmentPath(segmentPath);
        _owner.SegmentWriter.OpenSegment(segmentPath, false);
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        _owner.SegmentWriter.Write(header, 0);
        _owner.SetActiveSegmentWrittenBytes(JournalFraming.FileHeaderSize);
        _owner.AddJournalTotalBytes(JournalFraming.FileHeaderSize);
        _roll.IncrementJournalSegmentCount();
        _owner.SetDirty(false);
        _roll.SetSegmentRollInFlight(false);
    }

    private void CompleteStagedAppend(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        try
        {
            _owner.Host.DecrementQueuedAppends();
        }
        finally
        {
            if (frameBytes != null)
                ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
        }

        CompleteJournalWorkItem(item);
    }

    private void CompleteWriteBatch(int spanLength, long offset, bool notifyGroupCommit)
    {
        _owner.SetActiveSegmentWrittenBytes(offset + spanLength);
        _owner.AddJournalTotalBytes(spanLength);
        _owner.SetDirty(true);

        for (var i = 0; i < _owner.WriteBatch.PendingAppends.Count; i++)
            CompleteStagedAppend(_owner.WriteBatch.PendingAppends[i]);

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
        try
        {
            WriteAppendFrame(item);
        }
        catch (JournalCapacityExceededException ex)
        {
            FailAppendWorkItem(item, ex);
            return;
        }
        catch (IOException)
        {
            ReleaseQueuedAppendResources(item);
            throw;
        }
        catch (ObjectDisposedException)
        {
            ReleaseQueuedAppendResources(item);
            throw;
        }

        ReleaseQueuedAppendResources(item);

        if (_owner.Options.IsJournalGroupCommitEnabled)
            _owner.GroupCommit?.DrainDueBatchesOnJournalThread();

        CompleteJournalWorkItem(item);
    }

    private void ProcessAppendWithDurability(JournalWorkItem item)
    {
        var ack = item.Ack ?? throw new InvalidOperationException("AppendWithDurability work item is missing a durability ack.");
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
        catch (IOException)
        {
            ReleaseQueuedAppendResources(item);
            throw;
        }
        catch (ObjectDisposedException)
        {
            ReleaseQueuedAppendResources(item);
            throw;
        }

        ReleaseQueuedAppendResources(item);
    }

    private void ReleaseQueuedAppendResources(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        try
        {
            _owner.Host.DecrementQueuedAppends();
        }
        finally
        {
            if (frameBytes != null)
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
        var frameBytes = item.FrameBytes ?? throw new InvalidOperationException("Append work item is missing frame bytes.");
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
        catch (IOException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
        catch (ObjectDisposedException)
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
        catch (IOException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
        catch (ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
    }
}
