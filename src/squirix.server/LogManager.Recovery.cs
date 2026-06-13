using Microsoft.Extensions.Logging;

namespace Squirix.Server;

/// <summary>
/// Recovery and journal replay/logging diagnostics.
/// </summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 2004, Level = LogLevel.Error, Message = "Background journal recovery failed.")]
    internal static partial void JournalRecoveryFailed(ILogger logger);

    [LoggerMessage(
        EventId = 2008,
        Level = LogLevel.Warning,
        Message = "journal logging failed for CAS (keyHint: {KeyHint}, expectedVersion: {ExpectedVersion}). API call succeeded.")]
    internal static partial void JournalRecoveryFailedForCas(ILogger logger, string keyHint, long expectedVersion);

    [LoggerMessage(EventId = 2007, Level = LogLevel.Warning, Message = "journal logging failed for Increment (keyHint: {KeyHint}, delta: {Delta}). API call succeeded.")]
    internal static partial void JournalRecoveryFailedForIncrement(ILogger logger, string keyHint, long delta);

    [LoggerMessage(EventId = 2005, Level = LogLevel.Warning, Message = "journal logging failed for Insert (keyHint: {KeyHint}). API call succeeded.")]
    internal static partial void JournalRecoveryFailedForInsert(ILogger logger, string keyHint);

    [LoggerMessage(EventId = 2003, Level = LogLevel.Information, Message = "Recovery complete. fromSegment={FromSegment}, lastAppliedSeq={Seq}")]
    internal static partial void RecoveryComplete(ILogger logger, int fromSegment, ulong seq);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Warning, Message = "Failed to load snapshot at {Path}. Falling back to journal-only recovery.")]
    internal static partial void RecoveryFailedToLoadSnapshot(ILogger logger, string path);

    [LoggerMessage(EventId = 2001, Level = LogLevel.Information, Message = "Loaded snapshot index={Index}, lastAppliedSeq={Seq}")]
    internal static partial void RecoveryLoadedSnapshot(ILogger logger, int index, ulong seq);

    [LoggerMessage(
        EventId = 2009,
        Level = LogLevel.Information,
        Message =
            "Recovery replay boundary: snapshotPresent={SnapshotPresent}, manifestCurrentJournal={ManifestCurrentJournal}, firstAvailableJournal={FirstAvailableJournal}, chosenFromSegment={ChosenFromSegment}")]
    internal static partial void RecoveryReplayBoundary(ILogger logger, bool snapshotPresent, int manifestCurrentJournal, int firstAvailableJournal, int chosenFromSegment);
}
