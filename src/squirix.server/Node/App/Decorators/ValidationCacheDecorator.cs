using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies public/runtime cache operation validation before admission, metrics, journal, and mutation.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class ValidationCacheDecorator<T> : IRequestCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly int _valueLimit;
    private readonly IRoutedKeyContext? _keyValidation;

    internal ValidationCacheDecorator(ILogicalNamespacedCache<T> inner, int valueLimit = int.MaxValue, IRoutedKeyContext? keyValidation = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _valueLimit = valueLimit;
        _keyValidation = keyValidation;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetEntryForRequestAsync(cacheName, key, cancellationToken, request);
    }

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueWithDeadlineForRequestAsync(cacheName, key, cancellationToken, request);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueForRequestAsync(cacheName, key, cancellationToken, request);
    }

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveForRequestAsync(operationId, cacheName, key, cancellationToken, request);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationForRequestAsync(operationId, cacheName, key, cancellationToken, request);
    }

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        ValidateLocalValue(entry.Value, request?.Forwarded == true, entry.PendingWrite);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.SetEntryForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        expiration.ThrowIfNegativeOrZero(nameof(expiration), "expiration must be greater than zero.");
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchForRequestAsync(operationId, cacheName, key, expiration, cancellationToken, request);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        ValidateLocalValue(entry.Value, request?.Forwarded == true, entry.PendingWrite);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TryAddEntryForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<bool> TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        ValidateLocalValue(entry.Value, request?.Forwarded == true, entry.PendingWrite);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TryReplaceEntryForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        ValidateLocalValue(entry.Value, request?.Forwarded == true, entry.PendingWrite);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.AddOrGetForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), request ?? _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateLocalValue(value, request?.Forwarded == true);
        return _inner.UpdateForRequestAsync(operationId, cacheName, key, value, cancellationToken, request);
    }

    private void ValidateLocalValue(T? value, bool forwarded, DistributedCacheWrite? pending = null)
    {
        if (_valueLimit == int.MaxValue || forwarded)
            return;

        var length = pending?.PayloadLength ?? (value is StoredValue stored ? stored.Bytes.Length : MeasureValue(value));
        if (length > _valueLimit)
            throw ServerOpContract.PayloadTooLarge(_valueLimit);
    }

    private int MeasureValue(T? value) => NodeCacheEntry<T>.NormalizeValue(value) switch
    {
        byte[] bytes => bytes.Length,
        JsonUtf8Value json => json.Length,
        string text when text.Length > _valueLimit / 3 => Encoding.UTF8.GetByteCount(text),
        _ => 0,
    };
}
