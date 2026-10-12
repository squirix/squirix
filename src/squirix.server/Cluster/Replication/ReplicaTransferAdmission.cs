using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Decides from the durable status of a copy whether a state transfer may begin; nothing here writes.</summary>
internal static class ReplicaTransferAdmission
{
    /// <summary>Finds the reason a copy cannot take a transfer, in the order the sender is told.</summary>
    /// <param name="header">Who sends the transfer.</param>
    /// <param name="baseline">The point the transfer is taken at.</param>
    /// <param name="status">The durable status of the copy.</param>
    /// <param name="leads">Whether this node leads the group.</param>
    /// <returns>The refusal code, or <see langword="null" /> when the copy may take the transfer.</returns>
    /// <remarks>An unseeded copy is not repaired by a transfer; the first start of the group seeds it.</remarks>
    internal static string? Refusal(in ReplicaTransferHeader header, in ReplicaTransferBaseline baseline, in FollowerLogStatus status, bool leads) => true switch
    {
        _ when status.IsTopologyMismatch(baseline.Fingerprint, baseline.Generation) => RefusalCodes.TopologyMismatch,
        _ when header.LeaderTerm < status.CurrentTerm => RefusalCodes.StaleTerm,
        _ when status.MemberState == GroupMemberState.Unseeded => RefusalCodes.Unseeded,
        _ when status.Readiness != FollowerLogReadiness.Ready || baseline.Index < status.CommitIndex || leads => RefusalCodes.NotReady,
        _ => null,
    };
}
