using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Ensures owner-local physical mutations execute only on the owning node.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class OwnershipGuardCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly INodeLocator _locator;
    private readonly string _self;

    public OwnershipGuardCacheDecorator(string self, INodeLocator locator, ILogicalNamespacedCache<T> inner)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
        _locator = locator ?? throw new ArgumentNullException(nameof(locator));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.AddAsync(cacheName, key, value, cancellationToken);
    }

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.AddAsync(cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.ContainsAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.SetAsync(cacheName, key, value, cancellationToken);
    }

    public ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.SetAsync(cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.RemoveExpirationAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.RemoveAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.TouchAsync(cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.TryAddAsync(cacheName, key, value, cancellationToken);
    }

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.TryAddAsync(cacheName, key, entry, cancellationToken);
    }

    public ValueTask<CacheValueResult<T>> GetOrAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.GetOrAddAsync(cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.UpdateAsync(cacheName, key, value, cancellationToken);
    }

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.TryGetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.TryRemoveAsync(cacheName, key, cancellationToken);
    }

    private void EnsureLocalOwner(string cacheName, string key)
    {
        var owner = _locator.GetOwner(cacheName, key);
        if (!string.Equals(owner, _self, StringComparison.Ordinal))
            throw new OwnershipMismatchException("logical", cacheName, key, owner, _self);
    }
}
