using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Cluster.Transport;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Cluster.Routing;

/// <summary>
/// Routes cache operations to the static owner using gRPC on remote peers.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ClusteredCache<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _local;
    private readonly INodeLocator _locator;
    private readonly ClusterRemote<T> _remote;
    private readonly string _selfId;

    public ClusteredCache(string selfId, ILogicalNamespacedCache<T> local, INodeLocator locator, IClientPool clients)
    {
        _selfId = selfId ?? throw new ArgumentNullException(nameof(selfId));
        _local = local ?? throw new ArgumentNullException(nameof(local));
        _locator = locator ?? throw new ArgumentNullException(nameof(locator));
        _remote = new ClusterRemote<T>(clients ?? throw new ArgumentNullException(nameof(clients)));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _local.AddAsync(cacheName, key, value, cancellationToken);

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => _local.AddAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.ContainsAsync(cacheName, key, cancellationToken) : _remote.ContainsAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.GetEntryAsync(cacheName, key, cancellationToken) : _remote.GetEntryAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _local.GetExpirationAsync(cacheName, key, cancellationToken);

    public async ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var entry = await GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return entry is null ? default : entry.Value;
    }

    public ValueTask InsertAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        InsertAsync(cacheName, key, new CacheEntry<T> { Value = value }, cancellationToken);

    public async ValueTask InsertAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        if (owner == _selfId)
            await _local.InsertAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        else
            await _remote.InsertAsync(owner, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.RemoveExpirationAsync(cacheName, key, cancellationToken) : _remote.RemoveExpirationAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.RemoveAsync(cacheName, key, cancellationToken) : _remote.RemoveAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.TouchAsync(cacheName, key, expiration, cancellationToken) : _remote.TouchAsync(owner, cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        TryAddAsync(cacheName, key, new CacheEntry<T> { Value = value }, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.TryAddAsync(cacheName, key, entry, cancellationToken) : _remote.TryAddAsync(owner, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.TryGetValueAsync(cacheName, key, cancellationToken) : _remote.TryGetValueAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return owner == _selfId ? _local.TryRemoveAsync(cacheName, key, cancellationToken) : _remote.TryRemoveAsync(owner, cacheName, key, cancellationToken);
    }

    private string OwnerFor(string cacheName, string key) => _locator.GetOwner(cacheName, key);
}
