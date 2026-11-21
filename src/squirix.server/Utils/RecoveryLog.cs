using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for journal and snapshot recovery.</summary>
internal static class RecoveryLog
{
    internal static void JournalRecoveryFailed(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.JournalRecoveryFailedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void RecoveryComplete(IServerLogger logger, int fromSegment, ulong seq)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.RecoveryCompleteEvent.Write(logger, ServerLogLevel.Information, null, (fromSegment, seq));
    }

    internal static void RecoveryFailedToLoadSnapshot(IServerLogger logger, string path)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.RecoveryFailedToLoadSnapshotEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(path));
    }

    internal static void RecoveryLoadedSnapshot(IServerLogger logger, int index, ulong seq)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.RecoveryLoadedSnapshotEvent.Write(logger, ServerLogLevel.Information, null, (index, seq));
    }

    internal static void RecoveryReplayBoundary(IServerLogger logger, bool snapshotPresent, int manifestCurrentJournal, int firstAvailableJournal, int chosenFromSegment)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.RecoveryReplayBoundaryEvent.Write(logger, ServerLogLevel.Information, null, (snapshotPresent, manifestCurrentJournal, firstAvailableJournal, chosenFromSegment));
    }

    private static class Events
    {
        internal static readonly LogEvent JournalRecoveryFailedEvent = new(2004, nameof(JournalRecoveryFailed), "Background journal recovery failed.");

        internal static readonly LogEvent RecoveryCompleteEvent = new(2003, nameof(RecoveryComplete), "Recovery complete. fromSegment={FromSegment}, lastAppliedSeq={Seq}");

        internal static readonly LogEvent RecoveryFailedToLoadSnapshotEvent = new(2002, nameof(RecoveryFailedToLoadSnapshot), "Failed to load snapshot at {Path}. Falling back to journal-only recovery.");

        internal static readonly LogEvent RecoveryLoadedSnapshotEvent = new(2001, nameof(RecoveryLoadedSnapshot), "Loaded snapshot index={Index}, lastAppliedSeq={Seq}");

        internal static readonly LogEvent RecoveryReplayBoundaryEvent = new(2009, nameof(RecoveryReplayBoundary), "Recovery replay boundary: snapshotPresent={SnapshotPresent}, manifestCurrentJournal={ManifestCurrentJournal}, firstAvailableJournal={FirstAvailableJournal}, chosenFromSegment={ChosenFromSegment}");
    }
}
