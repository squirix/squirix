using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies public/runtime cache operation validation before admission, metrics, journal, and mutation.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ValidationCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly INodeLocator _ring;
    private readonly string _self;

    internal ValidationCacheDecorator(ILogicalNamespacedCache<T> inner, INodeLocator ring, string self)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
        _self = self ?? throw new ArgumentNullException(nameof(self));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetEntryAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);
    }

    public async ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        ServerExpirationValidator.ValidateRequiredPositive(expiration, nameof(expiration));
        cancellationToken.ThrowIfCancellationRequested();
        await _inner.SetEntryAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        ServerOpInputValidator.ValidateEntry(entry);
        await EnsureRemotePutWithinLimitAsync(cacheName, key, entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        await EnsureEntryWithinLimitAsync(entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return await _inner.TryAddEntryAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveAsync(cacheName, key, cancellationToken);
    }

    /// <summary>Validates single-operation payloads such as cache entries and non-null factory delegates.</summary>
    private static class ServerOpInputValidator
    {
        /// <summary>Validates a cache entry reference and its tags when present.</summary>
        /// <param name="entry">The entry to validate.</param>
        /// <exception cref="SquirixException">Thrown when entry tags exceed limits.</exception>
        internal static void ValidateEntry(NodeCacheEntry<T> entry)
        {
            ArgumentNullException.ThrowIfNull(entry);
            EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        }
    }
}
