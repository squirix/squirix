using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

/// <summary>The leader table of a node whose groups no election leads: the ring owner of every group leads it, always in term one.</summary>
/// <remarks>
/// It reproduces static ownership exactly: this node has authority over its own group only, every other group routes to its owner, no
/// route is ever refuted or learned, and a leader is always known, so nothing waits.
/// </remarks>
[Immutable]
internal sealed class StaticLeaderTable : IGroupLeaderTable
{
    /// <summary>The term every static leader leads.</summary>
    internal const ulong StaticTerm = 1;

    private readonly string _selfId;

    /// <summary>Initializes a new instance of the <see cref="StaticLeaderTable" /> class.</summary>
    /// <param name="selfId">The identifier of this node, the leader of its own group.</param>
    internal StaticLeaderTable(string selfId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
        _selfId = selfId;
    }

    /// <inheritdoc />
    public bool HasLocalAuthority(string groupId, out ulong term)
    {
        var own = IsOwn(groupId);
        term = own ? StaticTerm : 0;
        return own;
    }

    /// <inheritdoc />
    /// <remarks>The ring owner always leads statically, so nothing is learned.</remarks>
    public void Learn(string groupId, in LeaderRoute hint)
    {
    }

    /// <inheritdoc />
    public GroupLeaderView Read(string groupId) =>
        new(true, IsOwn(groupId), false, StaticTerm, StaticTerm, new LeaderRoute(groupId, StaticTerm));

    /// <inheritdoc />
    public void Refute(string groupId, in LeaderRoute route)
    {
    }

    /// <inheritdoc />
    public bool TryGetLeader(string groupId, out LeaderRoute route)
    {
        route = new LeaderRoute(groupId, StaticTerm);
        return true;
    }

    /// <inheritdoc />
    public bool TryGetLearnedLeader(string groupId, out LeaderRoute route)
    {
        route = default;
        return false;
    }

    /// <inheritdoc />
    /// <remarks>A statically led group never waits: its ring owner has authority from the start, and no other node ever gets it.</remarks>
    public ValueTask<bool> WaitForAuthorityAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken) => ValueTask.FromResult(HasLocalAuthority(groupId, out _));

    /// <inheritdoc />
    public ValueTask<bool> WaitForLeaderAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken) => ValueTask.FromResult(true);

    /// <inheritdoc />
    public ValueTask<LeaderRoute> WaitForLeaderChangeAsync(string groupId, LeaderRoute previous, TimeSpan timeout, CancellationToken cancellationToken) => ValueTask.FromResult(default(LeaderRoute));

    private bool IsOwn(string groupId) => string.Equals(groupId, _selfId, StringComparison.Ordinal);
}
