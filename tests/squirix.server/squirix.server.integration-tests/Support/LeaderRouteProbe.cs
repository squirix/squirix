using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Pins a stale leader route in the leader table of one entry node until the router refutes it, and records every write the entry node
/// forwards, with its target and operation id.
/// </summary>
/// <remarks>Register it on every node: only the node named by <see cref="Arm" /> is affected.</remarks>
[ThreadSafe]
internal sealed class LeaderRouteProbe
{
    private readonly List<(string Target, string OperationId)> _forwards = [];
    private readonly Lock _gate = new();
    private string? _entry;
    private string? _group;
    private LeaderRoute _pinned;
    private bool _refuted;

    /// <summary>Gets a value indicating whether the router refuted the pinned route.</summary>
    internal bool Refuted
    {
        get
        {
            lock (_gate)
                return _refuted;
        }
    }

    /// <summary>Makes the leader table of the entry node report <paramref name="pinned" /> as the leader of a group until it is refuted.</summary>
    /// <param name="entry">The entry node.</param>
    /// <param name="group">The group.</param>
    /// <param name="pinned">The stale route to report.</param>
    internal void Arm(string entry, string group, LeaderRoute pinned)
    {
        lock (_gate)
        {
            (_entry, _group, _pinned, _refuted) = (entry, group, pinned, false);
            _forwards.Clear();
        }
    }

    /// <summary>Gets the writes the entry node forwarded since it was armed, in order.</summary>
    /// <returns>The targets and operation ids.</returns>
    internal (string Target, string OperationId)[] Forwards()
    {
        lock (_gate)
            return [.. _forwards];
    }

    /// <summary>Wraps the leader table and the client pool of a node.</summary>
    /// <param name="services">The node service collection.</param>
    internal void Register(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        Decorate<IGroupLeaderTable>(services, (sp, inner) => new PinnedTable(inner, sp.GetRequiredService<TopologyOptions>().NodeId, this));
        Decorate<IServerClientPool>(services, (sp, inner) => new RecordingPool(inner, sp.GetRequiredService<TopologyOptions>().NodeId, this));
    }

    private static void Decorate<T>(IServiceCollection services, Func<IServiceProvider, T, T> wrap)
        where T : class
    {
        ServiceDescriptor? original = null;
        for (var i = services.Count - 1; i >= 0 && original == null; i--)
        {
            if (services[i].ServiceType == typeof(T))
                original = services[i];
        }

        var found = ThrowHelper.Required(original, $"The node registers no {typeof(T).Name}.");
        _ = services.Remove(found);
        _ = services.AddSingleton(sp => wrap(sp, Create<T>(found, sp)));
    }

    private static T Create<T>(ServiceDescriptor descriptor, IServiceProvider sp)
        where T : class
    {
        var instance = descriptor.ImplementationInstance ?? descriptor.ImplementationFactory?.Invoke(sp);
        return ThrowHelper.Required(instance as T, $"The registration of {typeof(T).Name} built another type.");
    }

    private bool TryGetPinned(string node, string group, out LeaderRoute route)
    {
        lock (_gate)
        {
            var pinned = !_refuted && string.Equals(node, _entry, StringComparison.Ordinal) && string.Equals(group, _group, StringComparison.Ordinal);
            route = pinned ? _pinned : default;
            return pinned;
        }
    }

    private bool TryRefute(string node, string group, in LeaderRoute route)
    {
        if (!TryGetPinned(node, group, out var pinned) || !string.Equals(route.NodeId, pinned.NodeId, StringComparison.Ordinal))
            return false;

        lock (_gate)
            _refuted = true;

        return true;
    }

    private void RecordForward(string node, string target, string operationId)
    {
        lock (_gate)
        {
            if (string.Equals(node, _entry, StringComparison.Ordinal))
                _forwards.Add((target, operationId));
        }
    }

    /// <summary>The leader table of a node, reporting the pinned route on the entry node until it is refuted.</summary>
    [ThreadSafe]
    private sealed class PinnedTable : IGroupLeaderTable
    {
        private readonly IGroupLeaderTable _inner;
        private readonly string _node;
        private readonly LeaderRouteProbe _probe;

        internal PinnedTable(IGroupLeaderTable inner, string node, LeaderRouteProbe probe)
        {
            _inner = inner;
            _node = node;
            _probe = probe;
        }

        public bool HasLocalAuthority(string groupId, out ulong term) => _inner.HasLocalAuthority(groupId, out term);

        public GroupLeaderView Read(string groupId) => _inner.Read(groupId);

        public void Refute(string groupId, in LeaderRoute route)
        {
            // The pinned route is fake: its refutation only unpins it, so the election state of the node stays untouched.
            if (!_probe.TryRefute(_node, groupId, in route))
                _inner.Refute(groupId, in route);
        }

        public bool TryGetLeader(string groupId, out LeaderRoute route) => _probe.TryGetPinned(_node, groupId, out route) || _inner.TryGetLeader(groupId, out route);

        public ValueTask<bool> WaitForLeaderAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken) =>
            _inner.WaitForLeaderAsync(groupId, timeout, cancellationToken);
    }

    /// <summary>The client pool of a node, recording the writes the entry node forwards.</summary>
    [ThreadSafe]
    private sealed class RecordingPool : IServerClientPool
    {
        private readonly IServerClientPool _inner;
        private readonly string _node;
        private readonly LeaderRouteProbe _probe;

        internal RecordingPool(IServerClientPool inner, string node, LeaderRouteProbe probe)
        {
            _inner = inner;
            _node = node;
            _probe = probe;
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();

        public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => new RecordingClient(_inner.ForNode(nodeId), nodeId, this);

        public ServerChannelLease LeaseChannel(string nodeId, CancellationToken cancellationToken) => _inner.LeaseChannel(nodeId, cancellationToken);

        public IServerCallPolicy PolicyFor(string nodeId) => _inner.PolicyFor(nodeId);

        internal void Record(string target, string operationId) => _probe.RecordForward(_node, target, operationId);
    }

    /// <summary>A peer client that records the forwarded writes.</summary>
    [Immutable]
    private sealed class RecordingClient : SquirixCacheService.SquirixCacheServiceClient
    {
        private readonly SquirixCacheService.SquirixCacheServiceClient _inner;
        private readonly RecordingPool _pool;
        private readonly string _target;

        internal RecordingClient(SquirixCacheService.SquirixCacheServiceClient inner, string target, RecordingPool pool)
        {
            _inner = inner;
            _target = target;
            _pool = pool;
        }

        public override AsyncUnaryCall<SetAsyncResponse> SetEntryAsync(SetEntryAsyncRequest request, CallOptions options)
        {
            _pool.Record(_target, request.OperationId);
            return _inner.SetEntryAsync(request, options);
        }
    }
}
