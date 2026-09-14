using System;
using System.Collections.Generic;
using Grpc.Net.Client;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster;

/// <summary>Small pool for squirix gRPC clients keyed by NodeId.</summary>
internal interface IServerClientPool : IAsyncDisposable
{
    SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId);

    IServerCallPolicy PolicyFor(string nodeId);

    /// <summary>Gets the pooled gRPC channel for one peer, shared by cache and replication clients.</summary>
    /// <param name="nodeId">Target peer node identifier.</param>
    /// <returns>The pooled channel.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the peer is not pooled.</exception>
    GrpcChannel OpenChannel(string nodeId);
}
