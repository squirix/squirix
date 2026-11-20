using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for replica maintenance.</summary>
internal static class ReplicaMaintenanceLog
{
    internal static void ReplicaLogMaintenanceRetry(ILogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaLogMaintenanceRetryEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaLogCompactionChanged(ILogger logger, string groupId, string outcome)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaLogCompactionChangedEvent.Write(logger, LogLevel.Information, null, (groupId, outcome));
    }

    internal static void ReplicaLogCompactionSnapshotTooLarge(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.LogCompactionSnapshotTooLargeEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaLogCompactedPastFollower(ILogger logger, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaLogCompactedPastFollowerEvent.Write(logger, LogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaAppliedIndexReseeded(ILogger logger, string groupId, ulong from, ulong to)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaAppliedIndexReseededEvent.Write(logger, LogLevel.Warning, null, (groupId, from, to));
    }

    internal static void ReplicaExpirationLeakedOnShutdown(ILogger logger, TimeSpan budget)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ReplicaExpirationLeakedOnShutdownEvent.Write(logger, LogLevel.Error, null, ValueTuple.Create(budget));
    }

    internal static void ReplicaExpirationSweepFailed(ILogger logger, string groupId, int expired, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ReplicaExpirationSweepFailedEvent.Write(logger, LogLevel.Error, exception, (groupId, expired));
    }

    internal static void ReplicaExpirationSweepStopped(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ReplicaExpirationSweepStoppedEvent.Write(logger, LogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void ReplicaPromotionRetry(ILogger logger, string groupId, ulong term, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ReplicaPromotionRetryEvent.Write(logger, LogLevel.Debug, exception, (groupId, term));
    }

    internal static void ReplicaPassSkippedWhileLeading(ILogger logger, string groupId, string pass)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ReplicaPassSkippedWhileLeadingEvent.Write(logger, LogLevel.Debug, null, (groupId, pass));
    }

    internal static void ReplicaPromotionStorageRetry(ILogger logger, string groupId, ulong term, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaPromotionStorageRetryEvent.Write(logger, LogLevel.Warning, exception, (groupId, term));
    }

    internal static void ReplicaStaticLeaderTermAboveOne(ILogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaStaticLeaderTermAboveOneEvent.Write(logger, LogLevel.Warning, null, (groupId, term));
    }

    private static class Events
    {
        internal static readonly LogEvent ReplicaLogMaintenanceRetryEvent = new(4011, nameof(ReplicaMaintenanceLog.ReplicaLogMaintenanceRetry), "Replica group {GroupId} log maintenance failed and will be retried; the group log keeps its applied entries until it succeeds");

        internal static readonly LogEvent ReplicaLogCompactionChangedEvent = new(4012, nameof(ReplicaMaintenanceLog.ReplicaLogCompactionChanged), "Replica group {GroupId} log compaction outcome changed to {Outcome}");

        internal static readonly LogEvent LogCompactionSnapshotTooLargeEvent = new(4013, nameof(ReplicaMaintenanceLog.ReplicaLogCompactionSnapshotTooLarge), "Replica group {GroupId} log compaction is stalled: its snapshot would exceed the maximum snapshot size until idempotency outcomes age out");

        internal static readonly LogEvent ReplicaLogCompactedPastFollowerEvent = new(4071, nameof(ReplicaMaintenanceLog.ReplicaLogCompactedPastFollower), "Replica group {GroupId} log reached its retention limit and was compacted while a follower is not ready or stayed behind the commit index; that follower is repaired by catch-up or a state transfer");

        internal static readonly LogEvent ReplicaAppliedIndexReseededEvent = new(4027, nameof(ReplicaMaintenanceLog.ReplicaAppliedIndexReseeded), "Replica group {GroupId} applied index was raised from {From} to {To} because the log is applied beyond what memory received; the entries in between were never applied here");

        internal static readonly LogEvent ReplicaExpirationLeakedOnShutdownEvent = new(4032, nameof(ReplicaMaintenanceLog.ReplicaExpirationLeakedOnShutdown), "Replica expiration did not drain within the shutdown budget of {Budget}; the expiry still running is left to finish on its own");

        internal static readonly LogEvent ReplicaExpirationSweepFailedEvent = new(4033, nameof(ReplicaMaintenanceLog.ReplicaExpirationSweepFailed), "Replica group {GroupId} expiration sweep could not commit a tombstone after {Expired} keys of the pass; the group waits for the next pass");

        internal static readonly LogEvent ReplicaExpirationSweepStoppedEvent = new(4034, nameof(ReplicaMaintenanceLog.ReplicaExpirationSweepStopped), "Replica expiration sweep stopped because the host is shutting down");

        internal static readonly LogEvent ReplicaPromotionRetryEvent = new(4035, nameof(ReplicaMaintenanceLog.ReplicaPromotionRetry), "Replica group {GroupId} could not start leading term {Term} and retries on the next election tick");

        internal static readonly LogEvent ReplicaPassSkippedWhileLeadingEvent = new(4047, nameof(ReplicaMaintenanceLog.ReplicaPassSkippedWhileLeading), "Replica group {GroupId} {Pass} passes are skipped while this node leads the group");

        internal static readonly LogEvent ReplicaPromotionStorageRetryEvent = new(4048, nameof(ReplicaMaintenanceLog.ReplicaPromotionStorageRetry), "Replica group {GroupId} could not start leading term {Term} because its local storage failed, or the journal or memory refused the write; retried on the next election tick");

        internal static readonly LogEvent ReplicaStaticLeaderTermAboveOneEvent = new(4050, nameof(ReplicaMaintenanceLog.ReplicaStaticLeaderTermAboveOne), "Replica group {GroupId} log is at term {Term}, which only an election sets; this node leads the group without elections, in term one only, so the group stays blocked");
    }
}
