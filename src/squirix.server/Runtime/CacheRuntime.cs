using System;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

[Immutable]
internal sealed class CacheRuntime : ICacheRuntime
{
    private readonly ILogicalNamespacedCache<object?> _cache;

    public CacheRuntime(ILogicalNamespacedCache<object?> cache)
    {
        ArgumentNullException.ThrowIfNull(cache);
        _cache = cache;
    }

    public ILogicalNamespacedCache<T> GetCache<T>(string cacheName)
    {
        _ = ServerCacheName.ParsePublic(cacheName);
        return _cache is not ILogicalNamespacedCache<T> inner
            ? throw new InvalidOperationException("Default _cache does not support the requested value type.")
            : new NamespacedCacheAdapter<T>(inner);
    }
}
