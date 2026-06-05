using System;
using Squirix.Server.Contracts;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Grpc;

internal sealed class GrpcCacheOperations<T> : IGrpcCacheOperations<T>
{
    private readonly IInboundEndpointCacheOperations<T> _inbound;

    public GrpcCacheOperations(IInboundEndpointCacheOperations<T> inbound)
    {
        _inbound = inbound ?? throw new ArgumentNullException(nameof(inbound));
    }

    public ICacheApi<T> ForCache(string cacheName) => _inbound.ForCache(cacheName);
}
