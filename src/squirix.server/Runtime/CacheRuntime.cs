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
        _ = CacheName.ParsePublic(cacheName);
        return new NamespacedCacheAdapter<T>((ILogicalNamespacedCache<T>)_defaultCache);
    }
}
