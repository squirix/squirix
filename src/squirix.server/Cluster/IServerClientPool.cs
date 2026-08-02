using System;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster;

/// <summary>Small pool for squirix gRPC clients keyed by NodeId.</summary>
internal interface IServerClientPool : IAsyncDisposable
{
    SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId);

    IServerCallPolicy PolicyFor(string nodeId);
}
