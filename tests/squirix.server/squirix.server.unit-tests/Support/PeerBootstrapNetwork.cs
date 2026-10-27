using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Nodes that call each other for the first start of their groups the way the replication adapter wakes the callee: every call that reaches a running node notifies its peer contact.</summary>
/// <remarks>A stateful fake: which nodes are up changes between calls, and the calls each node made are counted across the whole run.</remarks>
[ThreadSafe]
internal sealed class PeerBootstrapNetwork
{
    private readonly ConcurrentDictionary<string, ReplicaPeerContact?> _contacts = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _statusCallers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _statusCalls = new(StringComparer.Ordinal);
    private readonly ReplicaMemberStatus _status;

    /// <summary>Initializes a new instance of the <see cref="PeerBootstrapNetwork" /> class.</summary>
    /// <param name="status">The status every node that is up reports for the group of the caller.</param>
    internal PeerBootstrapNetwork(in ReplicaMemberStatus status)
    {
        _status = status;
    }

    /// <summary>Gets the number of status calls a node made.</summary>
    /// <param name="caller">The node.</param>
    /// <returns>The number of calls, including those to nodes that were down.</returns>
    internal int StatusCallsOf(string caller) => _statusCalls.TryGetValue(caller, out var calls) ? calls : 0;

    /// <summary>Brings a node up; calls to it notify its contact from then on.</summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="contact">The record of the peers that called the node; <see langword="null" /> for a node that runs no first start.</param>
    internal void Up(string nodeId, ReplicaPeerContact? contact) => _contacts[nodeId] = contact;

    /// <summary>Brings a node up and lets it call every node that runs a first start, as its own first iteration does.</summary>
    /// <param name="nodeId">The node.</param>
    internal void Arrive(string nodeId)
    {
        _contacts[nodeId] = null;
        foreach (var (_, contact) in _contacts)
            contact?.Notify(nodeId);
    }

    /// <summary>Waits until a node made a number of status calls since the last wait consumed them.</summary>
    /// <param name="caller">The node.</param>
    /// <param name="count">The number of status calls to wait for.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the calls were made.</returns>
    internal async Task WaitForStatusCallsAsync(string caller, int count, CancellationToken cancellationToken)
    {
        var calls = _statusCallers.GetOrAdd(caller, static _ => new SemaphoreSlim(0));
        for (var i = 0; i < count; i++)
            await calls.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Creates the gateway a node uses to call the others.</summary>
    /// <param name="caller">The node.</param>
    /// <returns>The gateway.</returns>
    internal IReplicaBootstrapGateway GatewayOf(string caller) => new Gateway(this, caller);

    private Task<ReplicaMemberStatus> StatusAsync(string caller, string target)
    {
        _ = _statusCalls.AddOrUpdate(caller, 1, static (_, count) => count + 1);
        _ = _statusCallers.GetOrAdd(caller, static _ => new SemaphoreSlim(0)).Release();
        return Reach(caller, target) ? Task.FromResult(_status) : Task.FromException<ReplicaMemberStatus>(new IOException("member unreachable"));
    }

    private Task<ReplicaBootstrapOfferResult> OfferAsync(string caller, string target) =>
        Reach(caller, target)
            ? Task.FromResult(new ReplicaBootstrapOfferResult(true, _status.MemberState, string.Empty))
            : Task.FromException<ReplicaBootstrapOfferResult>(new IOException("member unreachable"));

    private bool Reach(string caller, string target)
    {
        if (!_contacts.TryGetValue(target, out var contact))
            return false;

        contact?.Notify(caller);
        return true;
    }

    [Immutable]
    private sealed class Gateway : IReplicaBootstrapGateway
    {
        private readonly string _caller;
        private readonly PeerBootstrapNetwork _network;

        internal Gateway(PeerBootstrapNetwork network, string caller)
        {
            _network = network;
            _caller = caller;
        }

        public Task<ReplicaMemberStatus> GetStatusAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken) => _network.StatusAsync(_caller, nodeId);

        public Task<ReplicaBootstrapOfferResult> OfferAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken) => _network.OfferAsync(_caller, nodeId);
    }
}
