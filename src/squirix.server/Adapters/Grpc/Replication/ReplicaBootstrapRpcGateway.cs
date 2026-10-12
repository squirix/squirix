using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Owner-side first-start RPCs over pooled internode channels.</summary>
[Immutable]
internal sealed class ReplicaBootstrapRpcGateway : IReplicaBootstrapGateway
{
    private readonly IServerClientPool _pool;

    /// <summary>Initializes a new instance of the <see cref="ReplicaBootstrapRpcGateway" /> class.</summary>
    /// <param name="pool">Pooled internode channels keyed by node identifier.</param>
    internal ReplicaBootstrapRpcGateway(IServerClientPool pool)
    {
        ArgumentNullException.ThrowIfNull(pool);
        _pool = pool;
    }

    /// <inheritdoc />
    public async Task<ReplicaMemberStatus> GetStatusAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        using var lease = _pool.LeaseChannel(nodeId, cancellationToken);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(lease.Channel);
        var request = new GetReplicaStatusRequest { Header = ReplicaRpcGateway.MapHeader(in header) };
        var response = await client.GetReplicaStatusAsync(request, cancellationToken: lease.Token).ResponseAsync.ConfigureAwait(false);
        return new ReplicaMemberStatus(
            GroupMemberStateNames.Parse(response.MemberState),
            string.Equals(response.Readiness, ReplicaReadinessNames.Ready, StringComparison.Ordinal),
            response.TopologyFingerprint.ToByteArray(),
            response.ConfigurationGeneration,
            response.RefusalCode);
    }

    /// <inheritdoc />
    public async Task<ReplicaBootstrapOfferResult> OfferAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        using var lease = _pool.LeaseChannel(nodeId, cancellationToken);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(lease.Channel);
        var request = new OfferReplicaBootstrapRequest { Header = ReplicaRpcGateway.MapHeader(in header) };
        var response = await client.OfferReplicaBootstrapAsync(request, cancellationToken: lease.Token).ResponseAsync.ConfigureAwait(false);
        return new ReplicaBootstrapOfferResult(response.Accepted, GroupMemberStateNames.Parse(response.MemberState), response.RefusalCode);
    }
}
