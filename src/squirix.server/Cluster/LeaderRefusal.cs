using System;
using Grpc.Core;
using Squirix.Server.Errors;

namespace Squirix.Server.Cluster;

/// <summary>Decides and builds the refusal of an operation on a replica group this node may not serve, before anything is appended.</summary>
/// <remarks>
/// Every refusal is definite: nothing was written. A stale term fences first, then a known other leader, then the lack of any leader,
/// in the order of <c language="csharp">LeaderAuthorityGate.CheckWrite</c>.
/// </remarks>
internal static class LeaderRefusal
{
    /// <summary>Classifies what this node knows of the leader of a group.</summary>
    /// <param name="view">The leader view of the group.</param>
    /// <param name="self">This node.</param>
    /// <returns><see cref="LeaderRefusalKind.None" /> when this node leads the group with authority; otherwise why it refuses.</returns>
    internal static LeaderRefusalKind Classify(in GroupLeaderView view, string self) => view switch
    {
        { HasAuthority: true } => LeaderRefusalKind.None,
        { LedWithoutAuthority: true } when view.HighestObservedTerm > view.Term => LeaderRefusalKind.StaleTerm,
        { HasLeader: true } when !string.Equals(view.Known.NodeId, self, StringComparison.Ordinal) => LeaderRefusalKind.StaleOwner,
        _ => LeaderRefusalKind.NoLeader,
    };

    /// <summary>Builds the refusal of a kind.</summary>
    /// <param name="kind">The refusal kind; <see cref="LeaderRefusalKind.None" /> refuses like <see cref="LeaderRefusalKind.NoLeader" />.</param>
    /// <param name="view">The leader view the kind was classified from.</param>
    /// <param name="self">This node.</param>
    /// <param name="leaderHint">Whether the refusal names the known leader in the leader hint trailers; set only when elections lead the group.</param>
    /// <returns>
    /// The stale-term failure, the stale-owner failure naming the known leader, or the retryable Unavailable refusal. A caller that found
    /// the authority of <paramref name="view" /> unusable (another leadership holds the commit gate) passes <see cref="LeaderRefusalKind.None" />.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind" /> is not a named value.</exception>
    internal static RpcException Create(LeaderRefusalKind kind, in GroupLeaderView view, string self, bool leaderHint) => kind switch
    {
        LeaderRefusalKind.StaleTerm => StaleTermFailure.Create(leaderHint ? view.Known.NodeId : null, view.Known.Term),
        LeaderRefusalKind.StaleOwner => leaderHint ? StaleOwnerFailure.Create(view.Known.NodeId, self, view.Known.Term) : StaleOwnerFailure.Create(view.Known.NodeId, self),
        LeaderRefusalKind.None or LeaderRefusalKind.NoLeader => ServerOpContract.NoLeaderAuthority(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported enum value."),
    };
}
