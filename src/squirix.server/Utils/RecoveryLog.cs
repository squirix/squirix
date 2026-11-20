using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for journal and snapshot recovery.</summary>
internal static class RecoveryLog
{
    internal static void JournalRecoveryFailed(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.JournalRecoveryFailedEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void RecoveryComplete(ILogger logger, int fromSegment, ulong seq)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.RecoveryCompleteEvent.Write(logger, LogLevel.Information, null, (fromSegment, seq));
    }

    internal static void RecoveryFailedToLoadSnapshot(ILogger logger, string path)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.RecoveryFailedToLoadSnapshotEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(path));
    }

    internal static void RecoveryLoadedSnapshot(ILogger logger, int index, ulong seq)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.RecoveryLoadedSnapshotEvent.Write(logger, LogLevel.Information, null, (index, seq));
    }

    internal static void RecoveryReplayBoundary(ILogger logger, bool snapshotPresent, int manifestCurrentJournal, int firstAvailableJournal, int chosenFromSegment)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.RecoveryReplayBoundaryEvent.Write(logger, LogLevel.Information, null, (snapshotPresent, manifestCurrentJournal, firstAvailableJournal, chosenFromSegment));
    }

    private static class Events
    {
        internal static readonly LogEvent JournalRecoveryFailedEvent = new(2004, nameof(RecoveryLog.JournalRecoveryFailed), "Background journal recovery failed.");

        internal static readonly LogEvent RecoveryCompleteEvent = new(2003, nameof(RecoveryLog.RecoveryComplete), "Recovery complete. fromSegment={FromSegment}, lastAppliedSeq={Seq}");

        internal static readonly LogEvent RecoveryFailedToLoadSnapshotEvent = new(2002, nameof(RecoveryLog.RecoveryFailedToLoadSnapshot), "Failed to load snapshot at {Path}. Falling back to journal-only recovery.");

        internal static readonly LogEvent RecoveryLoadedSnapshotEvent = new(2001, nameof(RecoveryLog.RecoveryLoadedSnapshot), "Loaded snapshot index={Index}, lastAppliedSeq={Seq}");

        internal static readonly LogEvent RecoveryReplayBoundaryEvent = new(2009, nameof(RecoveryLog.RecoveryReplayBoundary), "Recovery replay boundary: snapshotPresent={SnapshotPresent}, manifestCurrentJournal={ManifestCurrentJournal}, firstAvailableJournal={FirstAvailableJournal}, chosenFromSegment={ChosenFromSegment}");
    }
}
