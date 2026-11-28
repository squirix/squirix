using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Uses the production default ballot and transfer methods with a supplied vote transport.</summary>
[Immutable]
internal sealed class ReplicaVoteGatewayWithDefaults : IReplicaVoteGateway
{
    private readonly IReplicaVoteGateway _transport;

    /// <summary>Initializes a new instance of the <see cref="ReplicaVoteGatewayWithDefaults" /> class.</summary>
    /// <param name="transport">The transport for the individual pre-vote and vote calls.</param>
    internal ReplicaVoteGatewayWithDefaults(IReplicaVoteGateway transport)
    {
        _transport = transport;
    }

    /// <inheritdoc />
    public Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
        _transport.PreVoteAsync(nodeId, header, lastLogIndex, lastLogTerm, cancellationToken);

    /// <inheritdoc />
    public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
        _transport.RequestVoteAsync(nodeId, header, lastLogIndex, lastLogTerm, cancellationToken);
}
