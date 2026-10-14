using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Answers the status calls of a term ceiling as scripted per member; a member without a script is down.</summary>
internal sealed class ScriptedStatusGateway : IReplicaBootstrapGateway
{
    private readonly ConcurrentDictionary<string, ReplicaMemberStatus> _statuses = new(StringComparer.Ordinal);

    /// <summary>Gets the members asked, in the order of the calls.</summary>
    internal ConcurrentQueue<string> Calls { get; } = new();

    /// <inheritdoc />
    public Task<ReplicaMemberStatus> GetStatusAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken)
    {
        Calls.Enqueue(nodeId);
        return _statuses.TryGetValue(nodeId, out var status) ? Task.FromResult(status) : Task.FromException<ReplicaMemberStatus>(new IOException("member is down"));
    }

    /// <inheritdoc />
    public Task<ReplicaBootstrapOfferResult> OfferAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken) =>
        throw new NotSupportedException("A term ceiling makes no offers.");

    /// <summary>Scripts the answer of a member that reports its copy.</summary>
    /// <param name="nodeId">The member.</param>
    /// <param name="state">The state of its copy.</param>
    /// <param name="term">Its durable current term.</param>
    internal void Set(string nodeId, GroupMemberState state, ulong term) =>
        _statuses[nodeId] = new ReplicaMemberStatus(state, true, ReadOnlyMemory<byte>.Empty, 0UL, string.Empty, term);

    /// <summary>Scripts a member that refuses to report its status.</summary>
    /// <param name="nodeId">The member.</param>
    /// <param name="term">The term the refusal carries.</param>
    internal void Refuse(string nodeId, ulong term) =>
        _statuses[nodeId] = new ReplicaMemberStatus(null, false, ReadOnlyMemory<byte>.Empty, 0UL, RefusalCodes.NotReady, term);
}
