using System;

namespace Squirix.Server.Node.Services;

/// <summary>Stable lower-case names of <see cref="ReplicaLogCompactionOutcome" /> values for logs and metric labels.</summary>
internal static class ReplicaLogCompactionOutcomeNames
{
    /// <summary>Returns the stable name of an outcome.</summary>
    /// <param name="outcome">The compaction outcome.</param>
    /// <returns>The snake-case outcome name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The outcome is not a named value.</exception>
    internal static string Of(ReplicaLogCompactionOutcome outcome) => outcome switch
    {
        ReplicaLogCompactionOutcome.BelowThreshold => "below_threshold",
        ReplicaLogCompactionOutcome.Compacted => "compacted",
        ReplicaLogCompactionOutcome.FollowerNotReady => "follower_not_ready",
        ReplicaLogCompactionOutcome.FollowerBehind => "follower_behind",
        ReplicaLogCompactionOutcome.PendingApply => "pending_apply",
        ReplicaLogCompactionOutcome.UncommittedTail => "uncommitted_tail",
        ReplicaLogCompactionOutcome.UnresolvedOutcome => "unresolved_outcome",
        ReplicaLogCompactionOutcome.NotReady => "not_ready",
        ReplicaLogCompactionOutcome.SnapshotTooLarge => "snapshot_too_large",
        ReplicaLogCompactionOutcome.Busy => "busy",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported replica log compaction outcome."),
    };
}
