using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies public/runtime cache operation validation before admission, metrics, journal, and mutation.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class ValidationCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly INodeLocator _ring;
    private readonly string _self;

    internal ValidationCacheDecorator(ILogicalNamespacedCache<T> inner, INodeLocator ring, string self)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(ring);
        ArgumentNullException.ThrowIfNull(self);
        _inner = inner;
        _ring = ring;
        _self = self;
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

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        await EnsureRemotePutWithinLimitAsync(cacheName, key, entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        expiration.ThrowIfNegativeOrZero(nameof(expiration), "expiration must be greater than zero.");
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        await EnsureRemotePutWithinLimitAsync(cacheName, key, entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();

        // Local-owner update sizing runs in the ownership inner chain (journal prepare or local guard).
        return _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }

    private Task EnsureRemotePutWithinLimitAsync(string cacheName, string key, NodeCacheEntry<T> entry)
    {
        if (IsLocalOwner(cacheName, key))
            return Task.CompletedTask;

        JournalEntryPayload.EnsureEncodedLengthWithinLimit(entry);
        return Task.CompletedTask;
    }

    private bool IsLocalOwner(string cacheName, string key) => string.Equals(_ring.GetOwner(cacheName, key), _self, StringComparison.Ordinal);
}
