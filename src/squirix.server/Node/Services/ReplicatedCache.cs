using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Services;

/// <summary>Replicated owner-local cache: reads stay local, mutations commit through the led group that owns their key.</summary>
/// <remarks>
/// Under quorum reads, a read first fences on the leader of the group that owns its key: a read index confirmed by a majority in the led
/// term and applied to memory, so the local read observes every write committed before it arrived. Without them reads are not fenced.
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
    private readonly ReplicaGroupCommitters _committers;
    private readonly ILogicalNamespacedCache<object?> _inner;
    private readonly bool _quorumReads;

    /// <summary>Initializes a new instance of the <see cref="ReplicatedCache" /> class.</summary>
    /// <param name="inner">Local cache pipeline used for reads and ordered applies.</param>
    /// <param name="committers">The serialized replicated committers of the led groups.</param>
    /// <param name="quorumReads">Whether reads fence on the elected leader of their group; only for groups led by election.</param>
    internal ReplicatedCache(ILogicalNamespacedCache<object?> inner, ReplicaGroupCommitters committers, bool quorumReads = false)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(committers);
        _inner = inner;
        _committers = committers;
        _quorumReads = quorumReads;
    }

    /// <inheritdoc />
    /// <exception cref="Grpc.Core.RpcException">
    /// The entry expired and its tombstone could not commit: Unavailable, retryable. Under quorum reads, also the refusal of a read this
    /// node may not serve as the leader of the group, or whose read index no majority confirmed in time; nothing was read.
    /// </exception>
    public async ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        if (_quorumReads)
            await _committers.ForKey(cacheName, key).ConfirmReadAsync(cancellationToken).ConfigureAwait(false);

        var entry = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return IsExpired(entry) ? await ExpireAsync(cacheName, key, cancellationToken).ConfigureAwait(false) : entry;
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
        new(_committers.ForKey(cacheName, key).CommitRemoveAsync(operationId, cacheName, key, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        new(_committers.ForKey(cacheName, key).CommitRemoveExpirationAsync(operationId, cacheName, key, cancellationToken));

    /// <inheritdoc />
    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
        new(_committers.ForKey(cacheName, key).CommitSetAsync(operationId, cacheName, key, entry, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        new(_committers.ForKey(cacheName, key).CommitTouchAsync(operationId, cacheName, key, expiration, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
        new(_committers.ForKey(cacheName, key).CommitTryAddAsync(operationId, cacheName, key, entry, cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) =>
        new(_committers.ForKey(cacheName, key).CommitUpdateAsync(operationId, cacheName, key, value, cancellationToken));

    /// <summary>Reads the stored entry of a key without deciding its expiry: an entry past its deadline on the leader clock reads as absent.</summary>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The live entry, or <see langword="null" /> when the key is absent or expired.</returns>
    /// <remarks>
    /// Never commits a tombstone, so a caller around a write (memory admission and its accounting) cannot turn a committed write into a
    /// refused read; the write itself folds the expiry of the key into its decision.
    /// </remarks>
    internal async ValueTask<NodeCacheEntry<object?>?> PeekEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var entry = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return IsExpired(entry) ? null : entry;
    }

    /// <summary>Tells whether the leader clock has passed the deadline of a stored entry.</summary>
    /// <param name="entry">The stored entry, or <see langword="null" />.</param>
    /// <returns><see langword="true" /> when the entry has a deadline at or before the leader clock.</returns>
    private bool IsExpired(NodeCacheEntry<object?>? entry) => entry?.ExpiresUtc is { } deadline && deadline.Ticks <= _committers.Clock.GetUtcNow().UtcDateTime.Ticks;

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
            return await _committers.ForKey(cacheName, key).ExpireAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
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
