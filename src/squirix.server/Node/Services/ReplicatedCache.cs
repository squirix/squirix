using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Services;

/// <summary>Replicated owner-local cache: reads stay local, mutations commit through the owned group.</summary>
/// <remarks>
/// Storage keeps an entry past its deadline until a committed record removes it, and only the leader decides expiry. A read that finds
/// its entry past the deadline on the leader clock commits the tombstone of the entry before it reports the miss, so no replica reports
/// the key absent before the group agrees it is; when the tombstone cannot commit, the read is refused retryably.
/// This layer sits between memory admission and the local chain on activated hosts only.
/// Remote-owned keys never reach it (the ownership guard above refuses them, and the gRPC adapter forwards
/// them to their owner), and RF=1 hosts never register it, preserving the single-copy path byte for byte.
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
    /// <exception cref="Grpc.Core.RpcException">The entry expired and its tombstone could not commit: Unavailable, retryable.</exception>
    public async ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var entry = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var expired = entry?.ExpiresUtc is { } deadline && deadline.Ticks <= _committer.Clock.GetUtcNow().UtcDateTime.Ticks;
        return expired ? await ExpireAsync(cacheName, key, cancellationToken).ConfigureAwait(false) : entry;
    }

    /// <inheritdoc />
    /// <exception cref="Grpc.Core.RpcException">The entry expired and its tombstone could not commit: Unavailable, retryable.</exception>
    public async ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var entry = await GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return entry == null ? new NodeCacheValueResult<object?>(false, null) : new NodeCacheValueResult<object?>(true, entry.Value);
    }

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

    /// <summary>Expires the key on the leader and reports what the committed decision leaves.</summary>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="cancellationToken">Cancellation token for this read.</param>
    /// <returns><see langword="null" /> once the tombstone is committed; the entry when the leader finds it live.</returns>
    /// <exception cref="Grpc.Core.RpcException">The tombstone could not commit: Unavailable with the expiration-pending detail.</exception>
    private async Task<NodeCacheEntry<object?>?> ExpireAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        try
        {
            return await _committer.ExpireAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error)
        {
            // Whatever stopped the tombstone, the key is not absent until the group commits it, and it is not live either.
            throw ServerOpContract.ExpirationPending(error);
        }
    }
}
