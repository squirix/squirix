using System;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

[Immutable]
internal sealed class CacheRuntime : ICacheRuntime
{
    private readonly ILogicalNamespacedCache<object?> _defaultCache;

    public CacheRuntime(ILogicalNamespacedCache<object?> defaultCache)
    {
        ArgumentNullException.ThrowIfNull(defaultCache);
        _defaultCache = defaultCache;
    }

    public ILogicalNamespacedCache<T> GetCache<T>(string cacheName)
    {
        _ = ServerCacheName.ParsePublic(cacheName);
        if (_defaultCache is not ILogicalNamespacedCache<T> typedCache)
            throw new InvalidOperationException("Default cache does not support the requested value type.");

        return new NamespacedCacheAdapter<T>(typedCache);
    }
}
