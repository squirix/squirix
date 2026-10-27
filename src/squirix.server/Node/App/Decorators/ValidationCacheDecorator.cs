using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies public/runtime cache operation validation before admission, metrics, journal, and mutation.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class ValidationCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    internal ValidationCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetEntryAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        expiration.ThrowIfNegativeOrZero(nameof(expiration), "expiration must be greater than zero.");
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();

        // Local-owner update sizing runs in the ownership inner chain (journal prepare or local guard).
        return _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }
}
