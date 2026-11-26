using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Local cache double that journals every replicated write through the real journal coordinator, as the local write chain does.</summary>
[ThreadSafe]
internal sealed class JournalingCache : ILogicalNamespacedCache<StoredValue>
{
    private readonly IJournalCoordinator _journal;

    internal JournalingCache(IJournalCoordinator journal)
    {
        _journal = journal;
    }

    public ValueTask<NodeCacheEntry<StoredValue>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult<NodeCacheEntry<StoredValue>?>(null);

    public ValueTask<NodeCacheValueResult<StoredValue>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        ValueTask.FromResult(new NodeCacheValueResult<StoredValue>(false, default));

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        ValueTask.FromResult(false);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<StoredValue> entry, CancellationToken cancellationToken) =>
        _journal.AppendPutUnderGateAsync(new CacheKey(cacheName, key), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

    public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<StoredValue> entry, CancellationToken cancellationToken)
    {
        await _journal.AppendPutUnderGateAsync(new CacheKey(cacheName, key), JournalEntryPayloadKit.EncodePut("v"), cancellationToken).ConfigureAwait(false);
        return true;
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, StoredValue value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
}
