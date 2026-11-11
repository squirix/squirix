using System;
using System.Collections.Concurrent;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Routes inbound endpoint calls to the logical cache surface.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[ThreadSafe]
internal sealed class InboundEndpointCacheOperations<T> : IInboundEndpointCacheOperations<T>
{
    private readonly ILogicalNamespacedCache<T> _namespaced;
    private readonly ConcurrentDictionary<string, RoutedCacheApi<T>> _apiByCache = new(StringComparer.Ordinal);
    private readonly Lock _creationLock = new();
    private int _cachedCount;

    public InboundEndpointCacheOperations(ILogicalNamespacedCache<T> namespaced)
    {
        ArgumentNullException.ThrowIfNull(namespaced);
        _namespaced = namespaced;
    }

    public ICacheApi<T> ForCache(string cacheName)
    {
        if (cacheName != null && _apiByCache.TryGetValue(cacheName, out var cached))
            return cached;

        var canonical = ServerCacheName.ParsePublic(cacheName).Canonical;
        if (Volatile.Read(ref _cachedCount) >= InboundEndpointCacheLimits.MaxCachedFacades)
            return new RoutedCacheApi<T>(_namespaced, canonical);

        lock (_creationLock)
        {
            if (_apiByCache.TryGetValue(canonical, out cached))
                return cached;

            var created = new RoutedCacheApi<T>(_namespaced, canonical);
            if (_cachedCount < InboundEndpointCacheLimits.MaxCachedFacades)
            {
                _apiByCache[canonical] = created;
                Volatile.Write(ref _cachedCount, _cachedCount + 1);
            }

            return created;
        }
    }
}
