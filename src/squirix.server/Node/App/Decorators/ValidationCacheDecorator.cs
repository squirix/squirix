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

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetEntryAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        ServerOpInputValidator.ValidateEntry(entry);
        await EnsureRemotePutWithinLimitAsync(cacheName, key, entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        ServerExpirationValidator.ValidateRequiredPositive(expiration, nameof(expiration));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        ServerOpInputValidator.ValidateEntry(entry);
        await EnsureRemotePutWithinLimitAsync(cacheName, key, entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
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

    /// <summary>Validates expiration arguments where a strictly positive duration is required (for example touch operations).</summary>
    private static class ServerExpirationValidator
    {
        /// <summary>
        /// Ensures <paramref name="expiration" /> is greater than zero.
        /// </summary>
        /// <param name="expiration">The expiration to validate.</param>
        /// <param name="parameterName">The caller parameter name for exceptions.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="expiration" /> is zero or negative.</exception>
        internal static void ValidateRequiredPositive(TimeSpan expiration, string parameterName)
        {
            if (expiration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(parameterName, expiration, "expiration must be greater than zero.");
        }
    }

    /// <summary>Validates logical cache key strings before operations reach the inner pipeline.</summary>
    private static class ServerKeyValidator
    {
        /// <summary>
        /// Validates a key string and throws <see cref="ArgumentException" /> when invalid.
        /// </summary>
        /// <param name="key">The key to validate.</param>
        /// <param name="parameterName">The caller parameter name for exceptions.</param>
        internal static void Validate(string key, string parameterName) => _ = CacheKeyValidator.Validate(key, parameterName);
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
