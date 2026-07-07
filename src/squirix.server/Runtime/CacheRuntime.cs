using System;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

internal sealed class CacheRuntime : ICacheRuntime
{
    private readonly ILogicalNamespacedCache<object?> _defaultCache;

    public CacheRuntime(ILogicalNamespacedCache<object?> defaultCache)
    {
        _defaultCache = defaultCache ?? throw new ArgumentNullException(nameof(defaultCache));
    }

    public ILogicalNamespacedCache<T> GetCache<T>(string cacheName)
    {
        _ = ServerCacheName.ParsePublic(cacheName);
        if (_defaultCache is not ILogicalNamespacedCache<T> typedCache)
            throw new InvalidOperationException($"Default cache does not support value type '{typeof(T).Name}'.");

        return new NamespacedCacheAdapter<T>(typedCache);
    }
}
