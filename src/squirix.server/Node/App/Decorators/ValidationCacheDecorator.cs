using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies public/runtime cache operation validation before admission, metrics, journal, and mutation.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class ValidationCacheDecorator<T> : ILogicalNamespacedCache<T>
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

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetEntryAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueWithDeadlineAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.GetValueAsync(cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);
    }

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        ValidateLocalValue(entry.Value, entry.PendingWrite);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        expiration.ThrowIfNegativeOrZero(nameof(expiration), "expiration must be greater than zero.");
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        ValidateLocalValue(entry.Value, entry.PendingWrite);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        ArgumentNullException.ThrowIfNull(entry);
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        ValidateLocalValue(entry.Value, entry.PendingWrite);
        cancellationToken.ThrowIfCancellationRequested();
        return _inner.AddOrGetAsync(operationId, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        _ = CacheKeyValidator.Validate(key, nameof(key), _keyValidation);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateLocalValue(value);
        return _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }

    private void ValidateLocalValue(T? value, DistributedCacheWrite? pending = null)
    {
        if (_valueLimit == int.MaxValue || RemoteInvocationContext.IsInternalOwnerInvocation)
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
