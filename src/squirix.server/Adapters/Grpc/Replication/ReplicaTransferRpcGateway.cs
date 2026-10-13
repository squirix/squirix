using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Leader-side state transfer RPCs over pooled internode channels.</summary>
[Immutable]
internal sealed class ReplicaTransferRpcGateway : IReplicaTransferGateway
{
    private readonly IServerClientPool _pool;

    /// <summary>Initializes a new instance of the <see cref="ReplicaTransferRpcGateway" /> class.</summary>
    /// <param name="pool">Pooled internode channels keyed by node identifier.</param>
    internal ReplicaTransferRpcGateway(IServerClientPool pool)
    {
        ArgumentNullException.ThrowIfNull(pool);
        _pool = pool;
    }

    /// <inheritdoc />
    public Task<ReplicaTransferReply> AbortAsync(string nodeId, ReplicaRpcHeader header, string transferId, CancellationToken cancellationToken) =>
        ExchangeAsync(nodeId, ReplicaTransferWire.AbortRequest(in header, transferId), cancellationToken);

    /// <inheritdoc />
    public Task<ReplicaTransferReply> BeginAsync(string nodeId, ReplicaRpcHeader header, string transferId, ReplicaTransferBaseline baseline, CancellationToken cancellationToken) =>
        ExchangeAsync(nodeId, ReplicaTransferWire.BeginRequest(in header, transferId, in baseline), cancellationToken);

    /// <inheritdoc />
    public Task<ReplicaTransferReply> SendAsync(string nodeId, ReplicaRpcHeader header, ReplicaTransferMessage message, CancellationToken cancellationToken) =>
        ExchangeAsync(nodeId, ReplicaTransferWire.MessageRequest(in header, in message), cancellationToken);

    private async Task<ReplicaTransferReply> ExchangeAsync(string nodeId, TransferReplicaStateRequest request, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        using var lease = _pool.LeaseChannel(nodeId, cancellationToken);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(lease.Channel);
        var response = await client.TransferReplicaStateAsync(request, cancellationToken: lease.Token).ResponseAsync.ConfigureAwait(false);
        return ReplicaTransferWire.Reply(response);
    }
}
