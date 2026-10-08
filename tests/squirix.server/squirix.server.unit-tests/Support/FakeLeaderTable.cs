using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.UnitTests.Support;

/// <summary>An election-led leader table of one group whose known leader changes on a refutation or a wait as the test configures.</summary>
/// <remarks>A stateful fake: the router reads the table again after it refutes a route, so a double without state cannot model the reroute.</remarks>
[Mutable]
internal sealed class FakeLeaderTable : IGroupLeaderTable
{
    private readonly string _self;
    private LeaderRoute _leader;

    /// <summary>Initializes a new instance of the <see cref="FakeLeaderTable" /> class.</summary>
    /// <param name="self">This node.</param>
    /// <param name="leader">The known leader; <see langword="default" /> when none.</param>
    internal FakeLeaderTable(string self, LeaderRoute leader)
    {
        _self = self;
        _leader = leader;
    }

    /// <summary>Gets the known leader after a refutation; <see langword="default" /> (none) unless set.</summary>
    internal LeaderRoute AfterRefute { get; init; }

    /// <summary>Gets the leader a wait learns; a wait learns none unless set.</summary>
    internal LeaderRoute AfterWait { get; init; }

    /// <summary>Gets a value indicating whether a wait blocks until its cancellation; a wait returns at once unless set.</summary>
    internal bool BlockWaits { get; init; }

    /// <summary>Gets the refuted routes, in order.</summary>
    internal List<LeaderRoute> Refuted { get; } = [];

    /// <summary>Gets a value indicating whether this node serves the group; <see langword="true" /> unless set.</summary>
    internal bool Served { get; init; } = true;

    /// <summary>Gets the timeouts of the waits, in order.</summary>
    internal List<TimeSpan> Waits { get; } = [];

    /// <inheritdoc />
    public bool HasLocalAuthority(string groupId, out ulong term)
    {
        var self = string.Equals(_leader.NodeId, _self, StringComparison.Ordinal);
        term = self ? _leader.Term : 0;
        return self;
    }

    /// <inheritdoc />
    public GroupLeaderView Read(string groupId) => Served ? new GroupLeaderView(true, false, false, _leader.Term, _leader.Term, _leader) : default;

    /// <inheritdoc />
    public void Refute(string groupId, in LeaderRoute route)
    {
        Refuted.Add(route);
        _leader = AfterRefute;
    }

    /// <inheritdoc />
    public bool TryGetLeader(string groupId, out LeaderRoute route)
    {
        route = _leader;
        return !string.IsNullOrEmpty(route.NodeId);
    }

    /// <inheritdoc />
    public ValueTask<bool> WaitForLeaderAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Waits.Add(timeout);
        if (BlockWaits)
        {
            var blocked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = cancellationToken.Register(static (state, token) => _ = state is TaskCompletionSource<bool> wait && wait.TrySetCanceled(token), blocked);
            return new ValueTask<bool>(blocked.Task);
        }

        if (!string.IsNullOrEmpty(AfterWait.NodeId))
            _leader = AfterWait;

        return ValueTask.FromResult(!string.IsNullOrEmpty(_leader.NodeId));
    }
}
