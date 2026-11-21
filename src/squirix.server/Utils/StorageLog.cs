using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for durable storage.</summary>
internal static class StorageLog
{
    internal static void DataDirectoryLockNotEnforced(IServerLogger logger, string directory)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.DataDirectoryLockNotEnforcedEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(directory));
    }

    internal static void DataDirectoryPermissionsTightened(IServerLogger logger, string directory, int count)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.DataDirectoryPermissionsTightenedEvent.Write(logger, ServerLogLevel.Warning, null, (directory, count));
    }

    internal static void DataDirectoryRootNotRestricted(IServerLogger logger, string directory)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.DataDirectoryRootNotRestrictedEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(directory));
    }

    internal static void DurableMutationOutcomeUnknown(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.DurableMutationOutcomeUnknownEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create());
    }

    internal static void JournalAbandonedAppendsDrained(IServerLogger logger, int count, long bytes)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalAbandonedAppendsDrainedEvent.Write(logger, ServerLogLevel.Warning, null, (count, bytes));
    }

    internal static void JournalFsyncSlow(IServerLogger logger, long elapsedMs)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalFsyncSlowEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(elapsedMs));
    }

    internal static void JournalHeaderRestored(IServerLogger logger, string path, long originalLength, long discardedBytes)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalHeaderRestoredEvent.Write(logger, ServerLogLevel.Warning, null, (path, originalLength, discardedBytes));
    }

    internal static void JournalMutationGateHeldLong(IServerLogger logger, long heldMs, string holder)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalMutationGateHeldLongEvent.Write(logger, ServerLogLevel.Warning, null, (heldMs, holder));
    }

    internal static void JournalSegmentLeavesNoHeadroom(IServerLogger logger, int segmentMb, int totalMb, int segmentCount)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalSegmentLeavesNoHeadroomEvent.Write(logger, ServerLogLevel.Warning, null, (segmentMb, totalMb, segmentCount));
    }

    internal static void JournalTornCreationRewritten(IServerLogger logger, string path, long originalLength, long discardedBytes)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalTornCreationRewrittenEvent.Write(logger, ServerLogLevel.Warning, null, (path, originalLength, discardedBytes));
    }

    internal static void JournalTornTailTruncated(IServerLogger logger, string path, long originalLength, long discardedBytes)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalTornTailTruncatedEvent.Write(logger, ServerLogLevel.Warning, null, (path, originalLength, discardedBytes));
    }

    internal static void JournalWaitCanceledWhileStalled(IServerLogger logger, string waitingFor, string operation, long ioMs, string holder, long heldMs)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.JournalWaitCanceledWhileStalledEvent.Write(logger, ServerLogLevel.Warning, null, (waitingFor, operation, ioMs, holder, heldMs));
    }

    internal static void ManifestPublisherLeakedOnShutdownTimeout(IServerLogger logger, TimeSpan budget)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.PublisherLeakedOnShutdownTimeoutEvent.Write(logger, ServerLogLevel.Error, null, ValueTuple.Create(budget));
    }

    internal static void ManifestRetentionCleanupFailed(IServerLogger logger, Exception exception, string artifactKind)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ManifestRetentionCleanupFailedEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create(artifactKind));
    }

    internal static void ManifestRetentionDeleteFailed(IServerLogger logger, string artifactKind, string path)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ManifestRetentionDeleteFailedEvent.Write(logger, ServerLogLevel.Warning, null, (artifactKind, path));
    }

    private static class Events
    {
        internal static readonly LogEvent DataDirectoryLockNotEnforcedEvent = new(1023, nameof(DataDirectoryLockNotEnforced), "Exclusive ownership of data directory {Directory} is not enforced by the operating system: file locking is disabled (DOTNET_SYSTEM_IO_DISABLEFILELOCKING) or unsupported by the file system, and on NFS only other processes are excluded; another node started on the same directory will not be detected");

        internal static readonly LogEvent DataDirectoryPermissionsTightenedEvent = new(1024, nameof(DataDirectoryPermissionsTightened), "Data directory {Directory} held {Count} node files or directories readable by other users; they were restricted to the owner (0600 files, 0700 directories)");

        internal static readonly LogEvent DataDirectoryRootNotRestrictedEvent = new(1025, nameof(DataDirectoryRootNotRestricted), "Data directory {Directory} is accessible to other users and could not be restricted to the owner, for example because another user owns it; point the data directory at a subdirectory the node creates itself");

        internal static readonly LogEvent DurableMutationOutcomeUnknownEvent = new(1015, nameof(DurableMutationOutcomeUnknown), "Durable mutation outcome is unknown after its journal frame entered the ring; the cause is reported here and the caller gets COMMIT_OUTCOME_UNKNOWN");

        internal static readonly LogEvent JournalAbandonedAppendsDrainedEvent = new(1011, nameof(JournalAbandonedAppendsDrained), "Journal failure drained {Count} abandoned appends; {Bytes} bytes quarantined until the I/O thread joins");

        internal static readonly LogEvent JournalFsyncSlowEvent = new(1012, nameof(JournalFsyncSlow), "Journal fsync took {ElapsedMs} ms");

        internal static readonly LogEvent JournalHeaderRestoredEvent = new(1016, nameof(JournalHeaderRestored), "Journal segment {Path} had a damaged file header in front of intact frames; the header was restored without discarding data (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)");

        internal static readonly LogEvent JournalMutationGateHeldLongEvent = new(1013, nameof(JournalMutationGateHeldLong), "Journal mutation gate held for {HeldMs} ms by {Holder}");

        internal static readonly LogEvent JournalSegmentLeavesNoHeadroomEvent = new(1026, nameof(JournalSegmentLeavesNoHeadroom), "Journal segment size {SegmentMb} MB plus the largest journal frame, or the high-water segment count, does not fit below the high-water mark of the {TotalMb} MB journal quota and the {SegmentCount} segment cap; a snapshot at the high-water mark may free no segment, and a state transfer to this node can stall. Lower JournalMaxSegmentMb or raise JournalMaxTotalBytesMb and JournalMaxSegmentCount");

        internal static readonly LogEvent JournalTornCreationRewrittenEvent = new(1017, nameof(JournalTornCreationRewritten), "Journal segment {Path} held only a torn creation (no frames); a fresh file header was written (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)");

        internal static readonly LogEvent JournalTornTailTruncatedEvent = new(1018, nameof(JournalTornTailTruncated), "Journal segment {Path} ended in a torn frame; the tail was truncated (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)");

        internal static readonly LogEvent JournalWaitCanceledWhileStalledEvent = new(1014, nameof(JournalWaitCanceledWhileStalled), "Journal wait for {WaitingFor} canceled while stalled: journal I/O {Operation} in progress for {IoMs} ms, mutation gate held by {Holder} for {HeldMs} ms");

        internal static readonly LogEvent ManifestRetentionCleanupFailedEvent = new(1009, nameof(ManifestRetentionCleanupFailed), "Manifest retention cleanup for {ArtifactKind} failed");

        internal static readonly LogEvent ManifestRetentionDeleteFailedEvent = new(1008, nameof(ManifestRetentionDeleteFailed), "Manifest retention failed to delete {ArtifactKind} artifact at {Path}");

        internal static readonly LogEvent PublisherLeakedOnShutdownTimeoutEvent = new(1019, nameof(ManifestPublisherLeakedOnShutdownTimeout), "Manifest publisher did not stop within the shutdown budget of {Budget}; manifest writes and rolls it has not yet taken are refused and its worker thread is leaked");
    }
}
