using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Services;

/// <summary>Replicated owner-local cache: reads stay local, mutations commit through the owned group.</summary>
/// <remarks>
/// This layer sits between ownership routing and the local pipeline on activated hosts only.
/// Remote-owned keys never reach it (the router forwards them to the owner), and RF=1 hosts
/// never register it, preserving the single-copy path byte for byte.
/// </remarks>
[Immutable]
internal sealed class ReplicatedCache : ILogicalNamespacedCache<object?>
{
    private readonly ReplicaGroupCommitter _committer;
    private readonly ILogicalNamespacedCache<object?> _inner;

    /// <summary>Initializes a new instance of the <see cref="ReplicatedCache" /> class.</summary>
    /// <param name="inner">Local cache pipeline used for reads and ordered applies.</param>
    /// <param name="committer">Serialized replicated committer for the owned group.</param>
    internal ReplicatedCache(ILogicalNamespacedCache<object?> inner, ReplicaGroupCommitter committer)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(committer);
        _inner = inner;
        _committer = committer;
    }

    /// <inheritdoc />
    public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetEntryAsync(cacheName, key, cancellationToken);

    /// <inheritdoc />
    public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    /// <inheritdoc />
    public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        new(_committer.CommitRemoveAsync(operationId, cacheName, key, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        new(_committer.CommitRemoveExpirationAsync(operationId, cacheName, key, cancellationToken));

    /// <inheritdoc />
    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
        new(_committer.CommitSetAsync(operationId, cacheName, key, entry, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        new(_committer.CommitTouchAsync(operationId, cacheName, key, expiration, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
        new(_committer.CommitTryAddAsync(operationId, cacheName, key, entry, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) =>
        new(_committer.CommitUpdateAsync(operationId, cacheName, key, value, cancellationToken));
}
