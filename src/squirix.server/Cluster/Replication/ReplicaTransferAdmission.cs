using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Decides from the durable status of a copy whether a state transfer may begin; nothing here writes.</summary>
internal static class ReplicaTransferAdmission
{
    /// <summary>Finds the reason a copy cannot take a transfer, in the order the sender is told.</summary>
    /// <param name="header">Who sends the transfer.</param>
    /// <param name="baseline">The point the transfer is taken at.</param>
    /// <param name="status">The durable status of the copy.</param>
    /// <param name="view">The election view of this node in the group: whether it leads and which leader it knows.</param>
    /// <returns>The refusal code, or <see langword="null" /> when the copy may take the transfer.</returns>
    /// <remarks>
    /// An unseeded copy takes a transfer only from a leader that checked the term ceiling (#1126); otherwise the first start of the group seeds it.
    /// A term has one leader, so a transfer of the term of a known other leader is stale. A declared index without a term, or with a term above the
    /// sender's, is malformed.
    /// </remarks>
    internal static string? Refusal(in ReplicaTransferHeader header, in ReplicaTransferBaseline baseline, in FollowerLogStatus status, in GroupLeaderView view) => true switch
    {
        _ when status.IsTopologyMismatch(baseline.Fingerprint, baseline.Generation) => RefusalCodes.TopologyMismatch,
        _ when header.LeaderTerm < status.CurrentTerm || view.Known.LeadsTermInstead(header.LeaderNodeId, header.LeaderTerm) => RefusalCodes.StaleTerm,
        _ when baseline.IndexTerm == 0UL || baseline.IndexTerm > header.LeaderTerm => RefusalCodes.ChecksumMismatch,
        _ when status.MemberState == GroupMemberState.Unseeded && !baseline.CeilingChecked => RefusalCodes.Unseeded,
        _ when status.Readiness != FollowerLogReadiness.Ready || baseline.Index < status.CommitIndex || view.HasAuthority || view.LedWithoutAuthority => RefusalCodes.NotReady,
        _ => null,
    };
}
