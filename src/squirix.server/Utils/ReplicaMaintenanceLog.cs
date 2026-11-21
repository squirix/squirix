using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for replica maintenance.</summary>
internal static class ReplicaMaintenanceLog
{
    internal static void ReplicaAppliedIndexReseeded(IServerLogger logger, string groupId, ulong from, ulong to)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaAppliedIndexReseededEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, from, to));
    }

    internal static void ReplicaExpirationLeakedOnShutdown(IServerLogger logger, TimeSpan budget)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.ReplicaExpirationLeakedOnShutdownEvent.Write(logger, ServerLogLevel.Error, null, ValueTuple.Create(budget));
    }

    internal static void ReplicaExpirationSweepFailed(IServerLogger logger, string groupId, int expired, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.ReplicaExpirationSweepFailedEvent.Write(logger, ServerLogLevel.Error, exception, (groupId, expired));
    }

    internal static void ReplicaExpirationSweepStopped(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaExpirationSweepStoppedEvent.Write(logger, ServerLogLevel.Debug, null, ValueTuple.Create());
    }

    internal static void ReplicaLogCompactedPastFollower(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaLogCompactedPastFollowerEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaLogCompactionChanged(IServerLogger logger, string groupId, string outcome)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ReplicaLogCompactionChangedEvent.Write(logger, ServerLogLevel.Information, null, (groupId, outcome));
    }

    internal static void ReplicaLogCompactionSnapshotTooLarge(IServerLogger logger, string groupId)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.LogCompactionSnapshotTooLargeEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(groupId));
    }

    internal static void ReplicaLogMaintenanceRetry(IServerLogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaLogMaintenanceRetryEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaPassSkippedWhileLeading(IServerLogger logger, string groupId, string pass)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaPassSkippedWhileLeadingEvent.Write(logger, ServerLogLevel.Debug, null, (groupId, pass));
    }

    internal static void ReplicaPromotionRetry(IServerLogger logger, string groupId, ulong term, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ReplicaPromotionRetryEvent.Write(logger, ServerLogLevel.Debug, exception, (groupId, term));
    }

    internal static void ReplicaPromotionStorageRetry(IServerLogger logger, string groupId, ulong term, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaPromotionStorageRetryEvent.Write(logger, ServerLogLevel.Warning, exception, (groupId, term));
    }

    internal static void ReplicaStaticLeaderTermAboveOne(IServerLogger logger, string groupId, ulong term)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ReplicaStaticLeaderTermAboveOneEvent.Write(logger, ServerLogLevel.Warning, null, (groupId, term));
    }

    private static class Events
    {
        internal static readonly LogEvent LogCompactionSnapshotTooLargeEvent = new(4013, nameof(ReplicaLogCompactionSnapshotTooLarge), "Replica group {GroupId} log compaction is stalled: its snapshot would exceed the maximum snapshot size until idempotency outcomes age out");

        internal static readonly LogEvent ReplicaAppliedIndexReseededEvent = new(4027, nameof(ReplicaAppliedIndexReseeded), "Replica group {GroupId} applied index was raised from {From} to {To} because the log is applied beyond what memory received; the entries in between were never applied here");

        internal static readonly LogEvent ReplicaExpirationLeakedOnShutdownEvent = new(4032, nameof(ReplicaExpirationLeakedOnShutdown), "Replica expiration did not drain within the shutdown budget of {Budget}; the expiry still running is left to finish on its own");

        internal static readonly LogEvent ReplicaExpirationSweepFailedEvent = new(4033, nameof(ReplicaExpirationSweepFailed), "Replica group {GroupId} expiration sweep could not commit a tombstone after {Expired} keys of the pass; the group waits for the next pass");

        internal static readonly LogEvent ReplicaExpirationSweepStoppedEvent = new(4034, nameof(ReplicaExpirationSweepStopped), "Replica expiration sweep stopped because the host is shutting down");

        internal static readonly LogEvent ReplicaLogCompactedPastFollowerEvent = new(4071, nameof(ReplicaLogCompactedPastFollower), "Replica group {GroupId} log reached its retention limit and was compacted while a follower is not ready or stayed behind the commit index; that follower is repaired by catch-up or a state transfer");

        internal static readonly LogEvent ReplicaLogCompactionChangedEvent = new(4012, nameof(ReplicaLogCompactionChanged), "Replica group {GroupId} log compaction outcome changed to {Outcome}");
        internal static readonly LogEvent ReplicaLogMaintenanceRetryEvent = new(4011, nameof(ReplicaLogMaintenanceRetry), "Replica group {GroupId} log maintenance failed and will be retried; the group log keeps its applied entries until it succeeds");

        internal static readonly LogEvent ReplicaPassSkippedWhileLeadingEvent = new(4047, nameof(ReplicaPassSkippedWhileLeading), "Replica group {GroupId} {Pass} passes are skipped while this node leads the group");

        internal static readonly LogEvent ReplicaPromotionRetryEvent = new(4035, nameof(ReplicaPromotionRetry), "Replica group {GroupId} could not start leading term {Term} and retries on the next election tick");

        internal static readonly LogEvent ReplicaPromotionStorageRetryEvent = new(4048, nameof(ReplicaPromotionStorageRetry), "Replica group {GroupId} could not start leading term {Term} because its local storage failed, or the journal or memory refused the write; retried on the next election tick");

        internal static readonly LogEvent ReplicaStaticLeaderTermAboveOneEvent = new(4050, nameof(ReplicaStaticLeaderTermAboveOne), "Replica group {GroupId} log is at term {Term}, which only an election sets; this node leads the group without elections, in term one only, so the group stays blocked");
    }
}
