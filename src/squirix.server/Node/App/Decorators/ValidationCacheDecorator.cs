using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Limits;
using Squirix.Server.Node.App.Decorators.Validation;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies public/runtime cache operation validation before admission, metrics, journal, and mutation.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ValidationCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    public ValidationCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
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
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        await EnsureEntryWithinLimitAsync(entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
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

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();

        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing is null)
            return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

        var updateEntry = new CacheEntry<T>
        {
            Value = value,
            ExpiresUtc = existing.ExpiresUtc,
            Expiration = existing.Expiration,
            Version = existing.Version,
        };
        EntryPayloadSizeGuard.EnsureEncodedLengthWithinLimit(updateEntry);
        return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);
    }

    private static Task EnsureEntryWithinLimitAsync(CacheEntry<T> entry)
    {
        EntryPayloadSizeGuard.EnsureEncodedLengthWithinLimit(entry);
        return Task.CompletedTask;
    }
}
