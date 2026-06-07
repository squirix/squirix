using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Limits;
using Squirix.Server.Node.App.Decorators.Validation;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Applies public/runtime cache operation validation before admission, metrics, journal, and mutation.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ValidationCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    public ValidationCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        EnsureValueWithinLimit(value, 1);
        return _inner.AddAsync(cacheName, key, value, cancellationToken);
    }

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        EnsureEntryWithinLimit(entry);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.AddAsync(cacheName, key, entry, cancellationToken);
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

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask InsertAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        EnsureValueWithinLimit(value, 1);
        return _inner.InsertAsync(cacheName, key, value, cancellationToken);
    }

    public ValueTask InsertAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        EnsureEntryWithinLimit(entry);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.InsertAsync(cacheName, key, entry, cancellationToken);
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

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchAsync(cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();
        EnsureValueWithinLimit(value, 1);
        return _inner.TryAddAsync(cacheName, key, value, cancellationToken);
    }

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        KeyInputValidator.Validate(key, nameof(key));
        OperationInputValidator<T>.ValidateEntry(entry);
        EnsureEntryWithinLimit(entry);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TryAddAsync(cacheName, key, entry, cancellationToken);
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

    private static void EnsureEntryWithinLimit(CacheEntry<T> entry) => EntryPayloadSizeGuard.EnsureWithinLimit(entry);

    private static void EnsureValueWithinLimit(T? value, long version) => EntryPayloadSizeGuard.EnsureWithinLimit(new CacheEntry<T> { Value = value, Version = version });
}
