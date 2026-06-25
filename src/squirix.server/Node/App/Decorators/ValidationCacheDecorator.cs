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

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        ServerExpirationValidator.ValidateRequiredPositive(expiration, nameof(expiration));
        cancellationToken.ThrowIfCancellationRequested();
        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        ServerKeyValidator.Validate(key, nameof(key));
        ServerOpInputValidator.ValidateEntry(entry);
        await EnsureRemotePutWithinLimitAsync(cacheName, key, entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);
    }

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        await EnsureEntryWithinLimitAsync(entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();

        // The encoded-size guard for updates needs the existing entry metadata, so it is enforced by the
        // memory-admission layer (the owner-local stage that already reads the existing entry) to avoid a
        // duplicate read here and a redundant network read when the key is owned by a remote node.
        return _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }

    private static Task EnsureEntryWithinLimitAsync(CacheEntry<T> entry)
    {
        EntryPayloadSizeGuard.EnsureEncodedLengthWithinLimit(entry);
        return Task.CompletedTask;
    }
}
