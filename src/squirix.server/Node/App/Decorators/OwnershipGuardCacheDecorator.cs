using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Ensures owner-local physical mutations execute only on the owning node.</summary>
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

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key);
        return _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }

    private void EnsureLocalOwner(string cacheName, string key)
    {
        var owner = _locator.GetOwner(cacheName, key);
        if (!string.Equals(owner, _self, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Ownership mismatch for local physical cache operation 'logical' on cache '{cacheName}' and key '{key}'. " +
                $"Expected owner '{owner}', current node '{_self}'.");
        }
    }
}
