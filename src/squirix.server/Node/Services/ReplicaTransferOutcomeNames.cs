using System;

namespace Squirix.Server.Node.Services;

/// <summary>Stable lower-case names of <see cref="ReplicaTransferOutcome" /> values for logs and metric labels.</summary>
internal static class ReplicaTransferOutcomeNames
{
    /// <summary>Returns the stable name of an outcome.</summary>
    /// <param name="outcome">The transfer outcome.</param>
    /// <returns>The snake-case outcome name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The outcome is not a named value.</exception>
    internal static string Of(ReplicaTransferOutcome outcome) => outcome switch
    {
        ReplicaTransferOutcome.Transferred => "transferred",
        ReplicaTransferOutcome.NotLeading => "capture_not_leading",
        ReplicaTransferOutcome.PendingApply => "capture_pending_apply",
        ReplicaTransferOutcome.UnresolvedOutcome => "capture_unresolved_outcome",
        ReplicaTransferOutcome.NothingApplied => "capture_nothing_applied",
        ReplicaTransferOutcome.NotReady => "capture_not_ready",
        ReplicaTransferOutcome.SnapshotTooLarge => "capture_snapshot_too_large",
        ReplicaTransferOutcome.FollowerRefused => "follower_refused",
        ReplicaTransferOutcome.Unreachable => "unreachable",
        ReplicaTransferOutcome.Superseded => "superseded",
        ReplicaTransferOutcome.Aborted => "aborted",
        ReplicaTransferOutcome.CeilingUnavailable => "ceiling_unavailable",
        ReplicaTransferOutcome.Yielded => "yielded",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported replica transfer outcome."),
    };
}
