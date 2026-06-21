using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Appends journal records for local-owner core mutations.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class JournalLoggingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly DurableMutationExecutor _durableMutations;
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly IJournalCoordinator _journal;
    private readonly INodeLocator _ring;
    private readonly string _self;

    public JournalLoggingCacheDecorator(string self, INodeLocator ring, ILogicalNamespacedCache<T> inner, IJournalCoordinator journal, DurableMutationExecutor durableMutations)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _durableMutations = durableMutations ?? throw new ArgumentNullException(nameof(durableMutations));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public async ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var cacheKey = new CacheKey(cacheName, key);
        return await _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<CacheRemoveResult<T>>.Apply()),
            ct => _journal.AppendRemoveAsync(cacheKey, ct),
            ct => _inner.RemoveAsync(operationId, cacheName, key, ct),
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);

        var cacheKey = new CacheKey(cacheName, key);
        return await _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            ct => _journal.AppendRemoveExpirationAsync(cacheKey, ct),
            ct => _inner.RemoveExpirationAsync(operationId, cacheName, key, ct),
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
        {
            await _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        var payload = await GetOrBuildJournalPayloadAsync(entry).ConfigureAwait(false);
        var cacheKey = new CacheKey(cacheName, key);
        _ = await _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            ct => _journal.AppendPutAsync(cacheKey, payload, null, ct),
            async ct =>
            {
                await _inner.SetEntryAsync(operationId, cacheName, key, entry, ct).ConfigureAwait(false);
                return true;
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken).ConfigureAwait(false);

        var cacheKey = new CacheKey(cacheName, key);
        var expiresUtc = DateTime.UtcNow.Add(expiration);
        return await _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            ct => _journal.AppendTouchExpirationAsync(cacheKey, expiresUtc, ct),
            ct => _inner.TouchAsync(operationId, cacheName, key, expiration, ct),
            cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        TryAddCoreAsync(operationId, cacheName, key, entry, cancellationToken);

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

        var existing = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing is null)
            return false;

        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(value, existing.ExpiresUtc, existing.Expiration, existing.Version, null).ConfigureAwait(false);
        var cacheKey = new CacheKey(cacheName, key);
        return await _durableMutations.ExecuteAsync(
            cacheKey,
            static _ => ValueTask.FromResult(DurableMutationCondition<bool>.Apply()),
            ct => _journal.AppendPutAsync(cacheKey, payload, null, ct),
            ct => _inner.UpdateAsync(operationId, cacheName, key, value, ct),
            cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<byte[]> GetOrBuildJournalPayloadAsync(CacheEntry<T> entry)
    {
        if (entry.PreparedJournalDiscriminatedJson is { } prepared)
            return prepared;

        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration);
        return await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(entry.Value, expiresUtc, expiration, entry.Version, null).ConfigureAwait(false);
    }

    private bool IsLocalOwner(string cacheName, string key) => string.Equals(_ring.GetOwner(cacheName, key), _self, StringComparison.Ordinal);

    private async ValueTask<bool> TryAddCoreAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);

        var payload = await GetOrBuildJournalPayloadAsync(entry).ConfigureAwait(false);
        var cacheKey = new CacheKey(cacheName, key);
        return await _durableMutations.ExecuteAsync(
            cacheKey,
            async ct =>
            {
                var existing = await _inner.GetValueAsync(cacheName, key, ct).ConfigureAwait(false);
                return existing.Found ? DurableMutationCondition<bool>.Skip(false) : DurableMutationCondition<bool>.Apply();
            },
            ct => _journal.AppendPutAsync(cacheKey, payload, null, ct),
            ct => _inner.TryAddEntryAsync(operationId, cacheName, key, entry, ct),
            cancellationToken).ConfigureAwait(false);
    }
}
