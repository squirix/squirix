using System;
using Squirix.Server.Attributes;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Grpc;

[Immutable]
internal sealed class CacheOperations<T> : IGrpcCacheOperations<T>
{
    private readonly IInboundEndpointCacheOperations<T> _inbound;

    public CacheOperations(IInboundEndpointCacheOperations<T> inbound)
    {
        ArgumentNullException.ThrowIfNull(inbound);
        _inbound = inbound;
    }

    public ICacheApi<T> ForCache(string cacheName) => _inbound.ForCache(cacheName);
}
