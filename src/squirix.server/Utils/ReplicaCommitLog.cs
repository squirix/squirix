using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for replicated commits.</summary>
internal static class ReplicaCommitLog
{
    internal static void ReplicaCommitterLeakedOnShutdownTimeout(ILogger logger, TimeSpan budget)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.CommitterLeakedOnShutdownTimeoutEvent.Write(logger, LogLevel.Error, null, ValueTuple.Create(budget));
    }

    internal static void ReplicaCommitOutcomeUnknown(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaCommitOutcomeUnknownEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create());
    }

    internal static void ReplicaPendingApplyFailed(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaPendingApplyFailedEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create());
    }

    internal static void ReplicaCoordinatorLeakedOnShutdown(ILogger logger, TimeSpan budget)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ReplicaCoordinatorLeakedOnShutdownEvent.Write(logger, LogLevel.Error, null, ValueTuple.Create(budget));
    }

    internal static void ReplicaTailOfOlderTerm(ILogger logger, ulong lastIndex, ulong term)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaTailOfOlderTermEvent.Write(logger, LogLevel.Warning, null, (lastIndex, term));
    }

    internal static void ReplicaInconsistentRecord(ILogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ReplicaInconsistentRecordEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaInconsistentDecision(ILogger logger, string groupId, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ReplicaInconsistentDecisionEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create(groupId));
    }

    internal static void ReplicaOutcomesRestored(ILogger logger, string groupId, int count)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ReplicaOutcomesRestoredEvent.Write(logger, LogLevel.Information, null, (groupId, count));
    }

    internal static void ReplicaCoordinatorAbandonedWorkFaulted(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.CoordinatorAbandonedWorkFaultedEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void DurablePublishRetried(ILogger logger, string path, int attempts, string holders)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.DurablePublishRetriedEvent.Write(logger, LogLevel.Warning, null, (path, attempts, holders));
    }

    internal static void ReplicaLocalAppendFailed(ILogger logger, Exception exception, string groupId)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ReplicaLocalAppendFailedEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create(groupId));
    }

    private static class Events
    {
        internal static readonly LogEvent CommitterLeakedOnShutdownTimeoutEvent = new(4005, nameof(ReplicaCommitLog.ReplicaCommitterLeakedOnShutdownTimeout), "Replica group committer did not drain within the shutdown budget of {Budget}; the in-flight commit keeps its coordinator and gate, which are leaked");

        internal static readonly LogEvent ReplicaCommitOutcomeUnknownEvent = new(4006, nameof(ReplicaCommitLog.ReplicaCommitOutcomeUnknown), "Replica commit outcome is unknown after the local append; the cause is reported here and the caller gets COMMIT_OUTCOME_UNKNOWN");

        internal static readonly LogEvent ReplicaPendingApplyFailedEvent = new(4007, nameof(ReplicaCommitLog.ReplicaPendingApplyFailed), "Replica group entries past their majority could not be applied to memory yet; new writes and resyncs are refused until they are");

        internal static readonly LogEvent ReplicaCoordinatorLeakedOnShutdownEvent = new(4008, nameof(ReplicaCommitLog.ReplicaCoordinatorLeakedOnShutdown), "Replica commit coordinator did not drain within the shutdown budget of {Budget}; the in-flight commit keeps its gates and log-index sequencer, which are leaked");

        internal static readonly LogEvent ReplicaTailOfOlderTermEvent = new(4010, nameof(ReplicaCommitLog.ReplicaTailOfOlderTerm), "Replica group leader tail through index {LastIndex} holds no entry of the current term {Term}; it is not committed by counting replicas, and writes stay refused until a current-term entry commits it");

        internal static readonly LogEvent ReplicaInconsistentRecordEvent = new(4014, nameof(ReplicaCommitLog.ReplicaInconsistentRecord), "Replica group {GroupId} refused an inconsistent log record and did not apply it; the entry stays pending, writes are refused, and the record needs operator attention");

        internal static readonly LogEvent ReplicaInconsistentDecisionEvent = new(4015, nameof(ReplicaCommitLog.ReplicaInconsistentDecision), "Replica group {GroupId} decided an inconsistent record at prepare; the write was refused and nothing was appended");

        internal static readonly LogEvent ReplicaOutcomesRestoredEvent = new(4016, nameof(ReplicaCommitLog.ReplicaOutcomesRestored), "Replica group {GroupId} rebuilt {Count} idempotency outcomes of committed log entries after the restart");

        internal static readonly LogEvent CoordinatorAbandonedWorkFaultedEvent = new(4017, nameof(ReplicaCommitLog.ReplicaCoordinatorAbandonedWorkFaulted), "Replica commit coordinator work abandoned at shutdown failed after the shutdown budget expired");

        internal static readonly LogEvent DurablePublishRetriedEvent = new(4020, nameof(ReplicaCommitLog.DurablePublishRetried), "Replica group durable publish from temp file {Path} needed {Attempts} attempts; holders: {Holders}");

        internal static readonly LogEvent ReplicaLocalAppendFailedEvent = new(4075, nameof(ReplicaCommitLog.ReplicaLocalAppendFailed), "Replica group {GroupId} leader could not append to its own log; writes are refused, and an elected leader gives the group up if this lasts and the other members can elect a leader");
    }
}
