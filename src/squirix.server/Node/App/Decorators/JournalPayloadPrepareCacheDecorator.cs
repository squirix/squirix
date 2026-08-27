using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Prepares and size-checks journal put payloads for local-owner writes before durable logging.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class JournalPayloadPrepareCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly JournalLoggingCacheDecorator<T> _journal;
    private readonly INodeLocator _ring;
    private readonly string _self;

    internal JournalPayloadPrepareCacheDecorator(string self, INodeLocator ring, JournalLoggingCacheDecorator<T> journal)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.RemoveAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _journal.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _journal.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        var prepared = JournalEntryPayload.PrepareEncode(entry);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return _journal.SetEntryWithPreparedPayloadAsync(operationId, cacheName, key, entry, prepared, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _journal.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        if (!IsLocalOwner(cacheName, key))
            return _journal.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        var prepared = JournalEntryPayload.PrepareEncode(entry);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        return _journal.TryAddEntryWithPreparedPayloadAsync(operationId, cacheName, key, entry, prepared, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _journal.UpdateAsync(operationId, cacheName, key, value, cancellationToken);

    private bool IsLocalOwner(string cacheName, string key) => string.Equals(_ring.GetOwner(cacheName, key), _self, StringComparison.Ordinal);
}
