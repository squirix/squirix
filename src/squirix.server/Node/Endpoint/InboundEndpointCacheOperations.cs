using System;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>
/// Routes inbound endpoint calls to the logical cache surface.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class InboundEndpointCacheOperations<T> : IInboundEndpointCacheOperations<T>
{
    private readonly ILogicalNamespacedCache<T> _namespaced;

    public InboundEndpointCacheOperations(ILogicalNamespacedCache<T> namespaced)
    {
        _namespaced = namespaced ?? throw new ArgumentNullException(nameof(namespaced));
    }

    public ICacheApi<T> ForCache(string cacheName)
    {
        var canonical = CacheName.ParsePublic(cacheName).Canonical;
        return new RoutedCacheApi<T>(new NamespacedCacheAdapter<T>(_namespaced), canonical);
    }
}
