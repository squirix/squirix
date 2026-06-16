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

    public async ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureValueWithinLimitAsync(value, 1).ConfigureAwait(false);
        await _inner.AddAsync(cacheName, key, value, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        await EnsureEntryWithinLimitAsync(entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        await _inner.AddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.ContainsAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetEntryAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetExpirationAsync(cacheName, key, cancellationToken);
    }

    public async ValueTask<CacheValueResult<T>> GetOrAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        await EnsureEntryWithinLimitAsync(entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return await _inner.GetOrAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationAsync(cacheName, key, cancellationToken);
    }

    public async ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureValueWithinLimitAsync(value, 1).ConfigureAwait(false);
        await _inner.SetAsync(cacheName, key, value, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        await EnsureEntryWithinLimitAsync(entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        await _inner.SetAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchAsync(cacheName, key, expiration, cancellationToken);
    }

    public async ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureValueWithinLimitAsync(value, 1).ConfigureAwait(false);
        return await _inner.TryAddAsync(cacheName, key, value, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        await EnsureEntryWithinLimitAsync(entry).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return await _inner.TryAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TryGetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TryRemoveAsync(cacheName, key, cancellationToken);
    }

    public async ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureValueWithinLimitAsync(value, 1).ConfigureAwait(false);
        return await _inner.UpdateAsync(cacheName, key, value, cancellationToken).ConfigureAwait(false);
    }

    private static Task EnsureEntryWithinLimitAsync(CacheEntry<T> entry) => EntryPayloadSizeGuard.EnsureWithinLimitAsync(entry);

    private static Task EnsureValueWithinLimitAsync(T? value, long version) =>
        EntryPayloadSizeGuard.EnsureWithinLimitAsync(new CacheEntry<T> { Value = value, Version = version });
}
