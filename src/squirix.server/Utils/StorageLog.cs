using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for durable storage.</summary>
internal static class StorageLog
{
    internal static void DataDirectoryLockNotEnforced(ILogger logger, string directory)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.DataDirectoryLockNotEnforcedEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(directory));
    }

    internal static void DurableMutationOutcomeUnknown(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.DurableMutationOutcomeUnknownEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create());
    }

    internal static void JournalAbandonedAppendsDrained(ILogger logger, int count, long bytes)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalAbandonedAppendsDrainedEvent.Write(logger, LogLevel.Warning, null, (count, bytes));
    }

    internal static void JournalFsyncSlow(ILogger logger, long elapsedMs)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalFsyncSlowEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(elapsedMs));
    }

    internal static void JournalHeaderRestored(ILogger logger, string path, long originalLength, long discardedBytes)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalHeaderRestoredEvent.Write(logger, LogLevel.Warning, null, (path, originalLength, discardedBytes));
    }

    internal static void JournalMutationGateHeldLong(ILogger logger, long heldMs, string holder)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalMutationGateHeldLongEvent.Write(logger, LogLevel.Warning, null, (heldMs, holder));
    }

    internal static void JournalTornCreationRewritten(ILogger logger, string path, long originalLength, long discardedBytes)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalTornCreationRewrittenEvent.Write(logger, LogLevel.Warning, null, (path, originalLength, discardedBytes));
    }

    internal static void JournalTornTailTruncated(ILogger logger, string path, long originalLength, long discardedBytes)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalTornTailTruncatedEvent.Write(logger, LogLevel.Warning, null, (path, originalLength, discardedBytes));
    }

    internal static void JournalWaitCanceledWhileStalled(ILogger logger, string waitingFor, string operation, long ioMs, string holder, long heldMs)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalWaitCanceledWhileStalledEvent.Write(logger, LogLevel.Warning, null, (waitingFor, operation, ioMs, holder, heldMs));
    }

    internal static void ManifestPublisherLeakedOnShutdownTimeout(ILogger logger, TimeSpan budget)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.PublisherLeakedOnShutdownTimeoutEvent.Write(logger, LogLevel.Error, null, ValueTuple.Create(budget));
    }

    internal static void ManifestRetentionCleanupFailed(ILogger logger, Exception exception, string artifactKind)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ManifestRetentionCleanupFailedEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create(artifactKind));
    }

    internal static void ManifestRetentionDeleteFailed(ILogger logger, string artifactKind, string path)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ManifestRetentionDeleteFailedEvent.Write(logger, LogLevel.Warning, null, (artifactKind, path));
    }

    internal static void DataDirectoryPermissionsTightened(ILogger logger, string directory, int count)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.DataDirectoryPermissionsTightenedEvent.Write(logger, LogLevel.Warning, null, (directory, count));
    }

    internal static void DataDirectoryRootNotRestricted(ILogger logger, string directory)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.DataDirectoryRootNotRestrictedEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(directory));
    }

    internal static void JournalSegmentLeavesNoHeadroom(ILogger logger, int segmentMb, int totalMb, int segmentCount)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.JournalSegmentLeavesNoHeadroomEvent.Write(logger, LogLevel.Warning, null, (segmentMb, totalMb, segmentCount));
    }

    private static class Events
    {
        internal static readonly LogEvent DataDirectoryLockNotEnforcedEvent = new(1023, nameof(StorageLog.DataDirectoryLockNotEnforced), "Exclusive ownership of data directory {Directory} is not enforced by the operating system: file locking is disabled (DOTNET_SYSTEM_IO_DISABLEFILELOCKING) or unsupported by the file system, and on NFS only other processes are excluded; another node started on the same directory will not be detected");

        internal static readonly LogEvent DurableMutationOutcomeUnknownEvent = new(1015, nameof(StorageLog.DurableMutationOutcomeUnknown), "Durable mutation outcome is unknown after its journal frame entered the ring; the cause is reported here and the caller gets COMMIT_OUTCOME_UNKNOWN");

        internal static readonly LogEvent JournalAbandonedAppendsDrainedEvent = new(1011, nameof(StorageLog.JournalAbandonedAppendsDrained), "Journal failure drained {Count} abandoned appends; {Bytes} bytes quarantined until the I/O thread joins");

        internal static readonly LogEvent JournalFsyncSlowEvent = new(1012, nameof(StorageLog.JournalFsyncSlow), "Journal fsync took {ElapsedMs} ms");

        internal static readonly LogEvent JournalHeaderRestoredEvent = new(1016, nameof(StorageLog.JournalHeaderRestored), "Journal segment {Path} had a damaged file header in front of intact frames; the header was restored without discarding data (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)");

        internal static readonly LogEvent JournalMutationGateHeldLongEvent = new(1013, nameof(StorageLog.JournalMutationGateHeldLong), "Journal mutation gate held for {HeldMs} ms by {Holder}");

        internal static readonly LogEvent JournalTornCreationRewrittenEvent = new(1017, nameof(StorageLog.JournalTornCreationRewritten), "Journal segment {Path} held only a torn creation (no frames); a fresh file header was written (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)");

        internal static readonly LogEvent JournalTornTailTruncatedEvent = new(1018, nameof(StorageLog.JournalTornTailTruncated), "Journal segment {Path} ended in a torn frame; the tail was truncated (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)");

        internal static readonly LogEvent JournalWaitCanceledWhileStalledEvent = new(1014, nameof(StorageLog.JournalWaitCanceledWhileStalled), "Journal wait for {WaitingFor} canceled while stalled: journal I/O {Operation} in progress for {IoMs} ms, mutation gate held by {Holder} for {HeldMs} ms");

        internal static readonly LogEvent PublisherLeakedOnShutdownTimeoutEvent = new(1019, nameof(StorageLog.ManifestPublisherLeakedOnShutdownTimeout), "Manifest publisher did not stop within the shutdown budget of {Budget}; manifest writes and rolls it has not yet taken are refused and its worker thread is leaked");

        internal static readonly LogEvent ManifestRetentionCleanupFailedEvent = new(1009, nameof(StorageLog.ManifestRetentionCleanupFailed), "Manifest retention cleanup for {ArtifactKind} failed");

        internal static readonly LogEvent ManifestRetentionDeleteFailedEvent = new(1008, nameof(StorageLog.ManifestRetentionDeleteFailed), "Manifest retention failed to delete {ArtifactKind} artifact at {Path}");

        internal static readonly LogEvent DataDirectoryPermissionsTightenedEvent = new(1024, nameof(StorageLog.DataDirectoryPermissionsTightened), "Data directory {Directory} held {Count} node files or directories readable by other users; they were restricted to the owner (0600 files, 0700 directories)");

        internal static readonly LogEvent DataDirectoryRootNotRestrictedEvent = new(1025, nameof(StorageLog.DataDirectoryRootNotRestricted), "Data directory {Directory} is accessible to other users and could not be restricted to the owner, for example because another user owns it; point the data directory at a subdirectory the node creates itself");

        internal static readonly LogEvent JournalSegmentLeavesNoHeadroomEvent = new(1026, nameof(StorageLog.JournalSegmentLeavesNoHeadroom), "Journal segment size {SegmentMb} MB plus the largest journal frame, or the high-water segment count, does not fit below the high-water mark of the {TotalMb} MB journal quota and the {SegmentCount} segment cap; a snapshot at the high-water mark may free no segment, and a state transfer to this node can stall. Lower JournalMaxSegmentMb or raise JournalMaxTotalBytesMb and JournalMaxSegmentCount");
    }
}
