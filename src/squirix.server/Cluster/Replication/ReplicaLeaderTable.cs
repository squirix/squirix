using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The leader table of a node with replicated groups, read from the election state of each served group.</summary>
/// <remarks>
/// The election state is the only source: the table keeps no copy that could lag behind a step-down, so authority disappears from it at
/// the moment the state revokes it.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaLeaderTable : IGroupLeaderTable
{
    private readonly ReplicaGroupRegistry _registry;
    private readonly string _selfId;

    /// <summary>Initializes a new instance of the <see cref="ReplicaLeaderTable" /> class.</summary>
    /// <param name="registry">The registry holding the election state of every served group.</param>
    /// <param name="selfId">The identifier of this node, the leader of the groups it has authority in.</param>
    internal ReplicaLeaderTable(ReplicaGroupRegistry registry, string selfId)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
        _registry = registry;
        _selfId = selfId;
    }

    /// <inheritdoc />
    public bool HasLocalAuthority(string groupId, out ulong term)
    {
        term = 0;
        return _registry.TryGetState(groupId, out var state) && state.TryGetAuthority(out term);
    }

    /// <inheritdoc />
    public bool TryGetLeader(string groupId, out LeaderRoute route)
    {
        route = default;
        if (!_registry.TryGetState(groupId, out var state))
            return false;

        if (state.TryGetAuthority(out var term))
        {
            route = new LeaderRoute(_selfId, term);
            return true;
        }

        if (!state.TryGetKnownLeader(out var leaderId, out term))
            return false;

        route = new LeaderRoute(leaderId, term);
        return true;
    }
}
