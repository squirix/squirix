using System;
using System.Collections.Concurrent;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Routes inbound endpoint calls to the logical cache surface.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class InboundEndpointCacheOperations<T> : IInboundEndpointCacheOperations<T>
{
    private readonly ILogicalNamespacedCache<T> _namespaced;
    private readonly ConcurrentDictionary<string, RoutedCacheApi<T>> _apiByCache = new(StringComparer.Ordinal);

    public InboundEndpointCacheOperations(ILogicalNamespacedCache<T> namespaced)
    {
        ArgumentNullException.ThrowIfNull(namespaced);
        _namespaced = namespaced;
    }

    public ICacheApi<T> ForCache(string cacheName)
    {
        // A name that is already cached was validated when it was added, and valid names are their own canonical form.
        if (cacheName != null && _apiByCache.TryGetValue(cacheName, out var cached))
            return cached;

        var canonical = ServerCacheName.ParsePublic(cacheName).Canonical;
        return _apiByCache.GetOrAdd(canonical, static (name, namespaced) => new RoutedCacheApi<T>(namespaced, name), _namespaced);
    }
}
