using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster;

/// <summary>Small pool for squirix gRPC clients keyed by NodeId.</summary>
internal interface IServerClientPool : IAsyncDisposable
{
    SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId);

    IServerCallPolicy PolicyFor(string nodeId);

    /// <summary>Leases the pooled gRPC channel of one peer for a single call that bypasses the peer's call policy.</summary>
    /// <param name="nodeId">Target peer node identifier.</param>
    /// <param name="cancellationToken">The caller's token; the lease links it with the pool's disposal.</param>
    /// <returns>The lease; dispose it once the call completed.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the peer is not pooled.</exception>
    /// <exception cref="ObjectDisposedException">Thrown when the pool started to dispose.</exception>
    ServerChannelLease LeaseChannel(string nodeId, CancellationToken cancellationToken);
}
