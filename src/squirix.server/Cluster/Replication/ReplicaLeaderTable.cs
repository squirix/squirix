using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The leader table of a node with replicated groups, read from the election state of each served group.</summary>
/// <remarks>
/// The election state is the only source of authority and of the known leader: the table keeps no copy that could lag behind a step-down,
/// so authority disappears from it at the moment the state revokes it. The table only remembers, per served group, the one route that
/// answered as stale, and hides it until the state reports another route; that memory is process-local and never durable.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaLeaderTable : IGroupLeaderTable
{
    private readonly ConcurrentDictionary<string, LeaderRoute> _refuted = new(StringComparer.Ordinal);
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
        if (!_registry.TryGetState(groupId, out var state))
            return false;

        var view = state.ReadRoute();
        if (view.HasAuthority)
            term = view.Term;

        return view.HasAuthority;
    }

    /// <inheritdoc />
    public GroupLeaderView Read(string groupId)
    {
        if (!_registry.TryGetState(groupId, out var state))
            return default;

        var view = state.ReadRoute();
        if (view.HasAuthority)
            return view with { Known = new LeaderRoute(_selfId, view.Term) };

        if (!view.HasLeader || !_refuted.TryGetValue(groupId, out var refuted))
            return view;

        if (view.Known == refuted)
            return view with { Known = default };

        // The state reports another route: the refutation is spent. Only the refutation read here is removed, never a newer one.
        _ = _refuted.TryRemove(new KeyValuePair<string, LeaderRoute>(groupId, refuted));
        return view;
    }

    /// <inheritdoc />
    public void Refute(string groupId, in LeaderRoute route)
    {
        if (string.IsNullOrEmpty(route.NodeId) || !_registry.TryGetState(groupId, out _))
            return;

        _refuted[groupId] = route;
    }

    /// <inheritdoc />
    public bool TryGetLeader(string groupId, out LeaderRoute route)
    {
        var view = Read(groupId);
        route = view.Known;
        return view.HasLeader;
    }

    /// <inheritdoc />
    /// <remarks>The wait runs on the election clock of the group and ends as soon as its state publishes a route change that names a leader.</remarks>
    public async ValueTask<bool> WaitForLeaderAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (!_registry.TryGetState(groupId, out var state))
            return false;

        var clock = state.Clock;
        var started = clock.GetTimestamp();
        while (true)
        {
            // The version is read before the check, so a change published between the check and the wait ends the wait at once.
            var version = state.RouteChanged.Version;
            if (TryGetLeader(groupId, out _))
                return true;

            var remaining = timeout - clock.GetElapsedTime(started);
            if (!await state.RouteChanged.WaitAsync(version, remaining, clock, cancellationToken).ConfigureAwait(false))
                return TryGetLeader(groupId, out _);
        }
    }
}
