using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Appends journal records for local-owner core mutations.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class JournalLoggingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly IJournalCoordinator _journal;
    private readonly INodeLocator _ring;
    private readonly string _self;

    public JournalLoggingCacheDecorator(string self, INodeLocator ring, ILogicalNamespacedCache<T> inner, IJournalCoordinator journal)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        InsertAsync(cacheName, key, new CacheEntry<T> { Value = value }, cancellationToken);

    public async ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!await _inner.TryAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException($"Key already exists: {key}");
    }

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.ContainsAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask InsertAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        InsertAsync(cacheName, key, new CacheEntry<T> { Value = value }, cancellationToken);

    public async ValueTask InsertAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
        {
            await _inner.InsertAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var payload = DiscriminatedEntryJsonWriter.BuildEntryJson(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, null);
        await _journal.AppendPutAsync(new CacheKey(cacheName, key), payload, null, cancellationToken).ConfigureAwait(false);
        await _inner.InsertAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.RemoveExpirationAsync(cacheName, key, cancellationToken).ConfigureAwait(false);

        await _journal.AppendRemoveExpirationAsync(new CacheKey(cacheName, key), cancellationToken).ConfigureAwait(false);
        return await _inner.RemoveExpirationAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        if (IsLocalOwner(cacheName, key))
            await _journal.AppendRemoveAsync(new CacheKey(cacheName, key), cancellationToken).ConfigureAwait(false);

        return await _inner.RemoveAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.TouchAsync(cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var expiresUtc = DateTime.UtcNow.Add(expiration);
        await _journal.AppendTouchExpirationAsync(new CacheKey(cacheName, key), expiresUtc, cancellationToken).ConfigureAwait(false);
        return await _inner.TouchAsync(cacheName, key, expiration, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _inner.TryAddAsync(cacheName, key, value, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        TryAddCore(cacheName, key, entry, cancellationToken);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.TryGetValueAsync(cacheName, key, cancellationToken);

    public async ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.TryRemoveAsync(cacheName, key, cancellationToken).ConfigureAwait(false);

        await _journal.AppendRemoveAsync(new CacheKey(cacheName, key), cancellationToken).ConfigureAwait(false);
        return await _inner.TryRemoveAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
    }

    private bool IsLocalOwner(string cacheName, string key) => string.Equals(_ring.GetOwner(cacheName, key), _self, StringComparison.Ordinal);

    private async ValueTask<bool> TryAddCore(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!await _inner.TryAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            return false;

        if (!IsLocalOwner(cacheName, key))
            return true;

        var payload = DiscriminatedEntryJsonWriter.BuildEntryJson(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, null);
        await _journal.AppendPutAsync(new CacheKey(cacheName, key), payload, null, cancellationToken).ConfigureAwait(false);
        return true;
    }
}
