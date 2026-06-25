using System;
using System.Collections.Concurrent;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Routes inbound endpoint calls to the logical cache surface.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class InboundEndpointCacheOperations<T> : IInboundEndpointCacheOperations<T>
{
    private readonly NamespacedCacheAdapter<T> _adapter;
    private readonly ConcurrentDictionary<string, RoutedCacheApi<T>> _apiByCache = new(StringComparer.Ordinal);

    public InboundEndpointCacheOperations(ILogicalNamespacedCache<T> namespaced)
    {
        ArgumentNullException.ThrowIfNull(namespaced);
        _adapter = new NamespacedCacheAdapter<T>(namespaced);
    }

    public ICacheApi<T> ForCache(string cacheName)
    {
        var canonical = CacheName.ParsePublic(cacheName).Canonical;
        return _apiByCache.GetOrAdd(canonical, static (name, adapter) => new RoutedCacheApi<T>(adapter, name), _adapter);
    }
}
