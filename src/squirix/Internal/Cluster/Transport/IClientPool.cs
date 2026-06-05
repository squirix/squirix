using System;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>
/// Small pool for squirix gRPC clients keyed by NodeId.
/// </summary>
internal interface IClientPool : IAsyncDisposable
{
    /// <summary>
    /// Initiates draining: reject new calls at policy level if supported and expose metrics as draining.
    /// </summary>
    void BeginDrain();

    SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId);

    ICallPolicy PolicyFor(string nodeId);
}
