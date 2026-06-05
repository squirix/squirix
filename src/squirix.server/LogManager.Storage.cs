using System;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server;

/// <summary>
/// Compaction and journal storage lifecycle logs.
/// </summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 1002, Level = LogLevel.Warning, Message = "Compaction backoff after {Failures} failures: delaying {DelayMs} ms")]
    internal static partial void CompactionBackoff(ILogger logger, int failures, int delayMs);

    [LoggerMessage(EventId = 1004, Level = LogLevel.Information, Message = "Compaction done at {Utc}")]
    internal static partial void CompactionDone(ILogger logger, DateTime utc);

    [LoggerMessage(EventId = 1005, Level = LogLevel.Error, Message = "Compaction failed")]
    internal static partial void CompactionFailed(ILogger logger);

    [LoggerMessage(EventId = 1003, Level = LogLevel.Information, Message = "Compaction start: snapshotIndex={Index}, tailSegments={Segments}, tailBytes={Bytes}")]
    internal static partial void CompactionStart(ILogger logger, int index, int segments, long bytes);

    [LoggerMessage(EventId = 1001, Level = LogLevel.Debug, Message = "Compaction state {Prev} -> {Next}")]
    internal static partial void CompactionStateChanged(ILogger logger, CompactionState prev, CompactionState next);

    [LoggerMessage(EventId = 1009, Level = LogLevel.Warning, Message = "Manifest retention cleanup for {ArtifactKind} failed")]
    internal static partial void ManifestRetentionCleanupFailed(ILogger logger, Exception exception, string artifactKind);

    [LoggerMessage(EventId = 1008, Level = LogLevel.Warning, Message = "Manifest retention failed to delete {ArtifactKind} artifact at {Path}")]
    internal static partial void ManifestRetentionDeleteFailed(ILogger logger, string artifactKind, string path);

    [LoggerMessage(EventId = 1007, Level = LogLevel.Information, Message = "Manual journal compaction finished.")]
    internal static partial void ManualCompactionFinished(ILogger logger);

    [LoggerMessage(EventId = 1006, Level = LogLevel.Information, Message = "Manual journal compaction starting (snapshotIndex={Index})")]
    internal static partial void ManualCompactionStart(ILogger logger, int index);
}
