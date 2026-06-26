using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Limits;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Entries;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Prepares and size-checks journal put payloads for local-owner writes before durable logging.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class JournalPayloadPrepareCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly JournalLoggingCacheDecorator<T> _journal;
    private readonly INodeLocator _ring;
    private readonly string _self;

    public JournalPayloadPrepareCacheDecorator(string self, INodeLocator ring, JournalLoggingCacheDecorator<T> journal)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.RemoveAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _journal.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        var prepared = JournalEntryPayload.PrepareEncode(entry);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return _journal.SetEntryWithPreparedPayloadAsync(operationId, cacheName, key, entry, prepared, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _journal.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _journal.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        var prepared = JournalEntryPayload.PrepareEncode(entry);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return _journal.TryAddEntryWithPreparedPayloadAsync(operationId, cacheName, key, entry, prepared, cancellationToken);
    }

    public async ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return await _journal.UpdateAsync(operationId, cacheName, key, value, cancellationToken).ConfigureAwait(false);

        var existing = await _journal.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing is null)
            return false;

        var replacement = CreateUpdateReplacement(existing, value);
        var prepared = JournalEntryPayload.PrepareEncode(replacement);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return await _journal.UpdateWithPreparedPayloadAsync(operationId, cacheName, key, value, prepared, cancellationToken).ConfigureAwait(false);
    }

    private static CacheEntry<T> CreateUpdateReplacement(CacheEntry<T> existing, T? value) => new()
    {
        Value = value,
        ExpiresUtc = existing.ExpiresUtc,
        Expiration = existing.Expiration,
        Version = existing.Version,
    };

    private bool IsLocalOwner(string cacheName, string key) => string.Equals(_ring.GetOwner(cacheName, key), _self, StringComparison.Ordinal);
}
