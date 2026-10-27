using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <content>Journal storage lifecycle logs.</content>
internal static partial class ServerLog
{
    [LoggerMessage(
        EventId = 1023,
        Level = LogLevel.Warning,
        Message = "Exclusive ownership of data directory {Directory} is not enforced by the operating system: file locking is disabled (DOTNET_SYSTEM_IO_DISABLEFILELOCKING) " +
                  "or unsupported by the file system, and on NFS only other processes are excluded; another node started on the same directory will not be detected")]
    internal static partial void DataDirectoryLockNotEnforced(ILogger logger, string directory);

    [LoggerMessage(
        EventId = 1015,
        Level = LogLevel.Warning,
        Message = "Durable mutation outcome is unknown after its journal frame entered the ring; the cause is reported here and the caller gets COMMIT_OUTCOME_UNKNOWN")]
    internal static partial void DurableMutationOutcomeUnknown(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 1011, Level = LogLevel.Warning, Message = "Journal failure drained {Count} abandoned appends; {Bytes} bytes quarantined until the I/O thread joins")]
    internal static partial void JournalAbandonedAppendsDrained(ILogger logger, int count, long bytes);

    [LoggerMessage(EventId = 1012, Level = LogLevel.Warning, Message = "Journal fsync took {ElapsedMs} ms")]
    internal static partial void JournalFsyncSlow(ILogger logger, long elapsedMs);

    [LoggerMessage(
        EventId = 1016,
        Level = LogLevel.Warning,
        Message = "Journal segment {Path} had a damaged file header in front of intact frames; the header was restored without discarding data (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)")]
    internal static partial void JournalHeaderRestored(ILogger logger, string path, long originalLength, long discardedBytes);

    [LoggerMessage(EventId = 1013, Level = LogLevel.Warning, Message = "Journal mutation gate held for {HeldMs} ms by {Holder}")]
    internal static partial void JournalMutationGateHeldLong(ILogger logger, long heldMs, string holder);

    [LoggerMessage(
        EventId = 1017,
        Level = LogLevel.Warning,
        Message = "Journal segment {Path} held only a torn creation (no frames); a fresh file header was written (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)")]
    internal static partial void JournalTornCreationRewritten(ILogger logger, string path, long originalLength, long discardedBytes);

    [LoggerMessage(
        EventId = 1018,
        Level = LogLevel.Warning,
        Message = "Journal segment {Path} ended in a torn frame; the tail was truncated (original length {OriginalLength} bytes, {DiscardedBytes} bytes discarded)")]
    internal static partial void JournalTornTailTruncated(ILogger logger, string path, long originalLength, long discardedBytes);

    [LoggerMessage(EventId = 1014, Level = LogLevel.Warning, Message = "Journal wait for {WaitingFor} canceled while stalled: journal I/O {Operation} in progress for {IoMs} ms, mutation gate held by {Holder} for {HeldMs} ms")]
    internal static partial void JournalWaitCanceledWhileStalled(ILogger logger, string waitingFor, string operation, long ioMs, string holder, long heldMs);

    [LoggerMessage(
        EventId = 1019,
        Level = LogLevel.Error,
        Message = "Manifest publisher did not stop within the shutdown budget of {Budget}; manifest writes and rolls it has not yet taken are refused and its worker thread is leaked")]
    internal static partial void ManifestPublisherLeakedOnShutdownTimeout(ILogger logger, TimeSpan budget);

    [LoggerMessage(EventId = 1009, Level = LogLevel.Warning, Message = "Manifest retention cleanup for {ArtifactKind} failed")]
    internal static partial void ManifestRetentionCleanupFailed(ILogger logger, Exception exception, string artifactKind);

    [LoggerMessage(EventId = 1008, Level = LogLevel.Warning, Message = "Manifest retention failed to delete {ArtifactKind} artifact at {Path}")]
    internal static partial void ManifestRetentionDeleteFailed(ILogger logger, string artifactKind, string path);

    [LoggerMessage(
        EventId = 1024,
        Level = LogLevel.Warning,
        Message = "Data directory {Directory} held {Count} node files or directories readable by other users; they were restricted to the owner (0600 files, 0700 directories)")]
    internal static partial void DataDirectoryPermissionsTightened(ILogger logger, string directory, int count);

    [LoggerMessage(
        EventId = 1025,
        Level = LogLevel.Warning,
        Message = "Data directory {Directory} is accessible to other users and could not be restricted to the owner, for example because another user owns it; point the data directory at a subdirectory the node creates itself")]
    internal static partial void DataDirectoryRootNotRestricted(ILogger logger, string directory);

    [LoggerMessage(
        EventId = 1026,
        Level = LogLevel.Warning,
        Message =
            "Journal segment size {SegmentMb} MB plus the largest journal frame, or the high-water segment count, does not fit below the high-water mark of the {TotalMb} MB journal quota and the {SegmentCount} segment cap; a snapshot at the high-water mark may free no segment, and a state transfer to this node can stall. Lower JournalMaxSegmentMb or raise JournalMaxTotalBytesMb and JournalMaxSegmentCount")]
    internal static partial void JournalSegmentLeavesNoHeadroom(ILogger logger, int segmentMb, int totalMb, int segmentCount);
}
