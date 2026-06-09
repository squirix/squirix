using System;
using Squirix.Server.Cluster.Reliability;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Small pool for squirix gRPC clients keyed by NodeId.
/// </summary>
internal interface IClientPool : IAsyncDisposable
{
    SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId);

    ICallPolicy PolicyFor(string nodeId);
}
