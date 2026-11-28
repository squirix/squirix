using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Prepares and size-checks journal put payloads for owner-local writes before durable logging.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class JournalPayloadPrepareCacheDecorator<T> : IRequestCache<T>, IReplicaCacheMutations<T>
{
    private readonly JournalLoggingCacheDecorator<T> _journal;

    internal JournalPayloadPrepareCacheDecorator(JournalLoggingCacheDecorator<T> journal)
    {
        ArgumentNullException.ThrowIfNull(journal);
        _journal = journal;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.GetEntryAsync(cacheName, key, request, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.GetValueAsync(cacheName, key, request, cancellationToken);

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.GetValueWithDeadlineAsync(cacheName, key, request, cancellationToken);

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.RemoveAsync(operationId, cacheName, key, request, cancellationToken);

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, bool replicaApply, CancellationToken cancellationToken, object? scope = null) =>
        _journal.RemoveAsync(operationId, cacheName, key, replicaApply, cancellationToken, scope);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.RemoveExpirationAsync(operationId, cacheName, key, request, cancellationToken);

    ValueTask IRequestCache<T>.SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.SetEntryAsync(operationId, cacheName, key, entry, request, cancellationToken);

    /// <inheritdoc />
    /// <remarks>An entry with a relative expiration is measured like any other: resolving it adds a deadline the known length may not cover.</remarks>
    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, int? encodedLength, CancellationToken cancellationToken) =>
        _journal.SetEntryAsync(operationId, cacheName, key, entry, encodedLength, default, cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, int? encodedLength, (bool ReplicaApply, object? Scope) mutation, CancellationToken cancellationToken) =>
        _journal.SetEntryAsync(operationId, cacheName, key, entry, encodedLength, mutation, cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.TouchAsync(operationId, cacheName, key, expiration, request, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.TryAddEntryAsync(operationId, cacheName, key, entry, request, cancellationToken);

    public ValueTask<bool> TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.TryReplaceEntryAsync(operationId, cacheName, key, entry, request, cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.UpdateAsync(operationId, cacheName, key, value, request, cancellationToken);

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        _journal.AddOrGetAsync(operationId, cacheName, key, entry, request, cancellationToken);
}
