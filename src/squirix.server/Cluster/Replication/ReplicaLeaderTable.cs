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
/// so authority disappears from it at the moment the state revokes it. The table only remembers, per served group, the refuted route of
/// the highest term, and hides every known leader of that term or below until the state reports a leader of a higher term; that memory
/// is process-local and never durable.
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
    /// <exception cref="ArgumentOutOfRangeException">The election options of the registry hold an unbounded wait for a leader.</exception>
    internal ReplicaLeaderTable(ReplicaGroupRegistry registry, string selfId)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
        ElectionTimerOptions.EnsureValidLeaderWait(registry.Election);
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

        if (view.Known.Term <= refuted.Term)
            return view with { Known = default };

        // The state reports a leader of a higher term: the refutation is spent. Only the refutation read here is removed, never a newer one.
        _ = _refuted.TryRemove(new KeyValuePair<string, LeaderRoute>(groupId, refuted));
        return view;
    }

    /// <inheritdoc />
    public void Refute(string groupId, in LeaderRoute route)
    {
        if (string.IsNullOrEmpty(route.NodeId) || !_registry.TryGetState(groupId, out _))
            return;

        // The refutation of the higher term is kept, so a late refutation of an older route cannot bring back a newer stale one.
        _ = _refuted.AddOrUpdate(groupId, static (_, added) => added, static (_, kept, added) => added.Term >= kept.Term ? added : kept, route);
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

            // A spent finite timeout becomes zero: a remainder of exactly -1 ms would otherwise read as an infinite wait.
            var remaining = timeout;
            if (timeout != Timeout.InfiniteTimeSpan)
            {
                remaining = timeout - clock.GetElapsedTime(started);
                if (remaining < TimeSpan.Zero)
                    remaining = TimeSpan.Zero;
            }

            if (!await state.RouteChanged.WaitAsync(version, remaining, clock, cancellationToken).ConfigureAwait(false))
                return TryGetLeader(groupId, out _);
        }
    }
}
