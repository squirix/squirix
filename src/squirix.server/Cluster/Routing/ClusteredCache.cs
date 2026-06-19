using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Cluster.Routing;

/// <summary>Routes cache operations to the static owner using gRPC on remote peers.</summary>
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

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.GetEntryAsync(cacheName, key, cancellationToken)
            : _remote.GetEntryAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.RemoveExpirationAsync(cacheName, key, cancellationToken)
            : _remote.RemoveExpirationAsync(owner, cacheName, key, cancellationToken);
    }

    public async ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        if (string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase))
            await _local.SetEntryAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false);
        else
            await _remote.SetEntryAsync(owner, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.TouchAsync(cacheName, key, expiration, cancellationToken)
            : _remote.TouchAsync(owner, cacheName, key, expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.TryAddEntryAsync(cacheName, key, entry, cancellationToken)
            : _remote.TryAddEntryAsync(owner, cacheName, key, entry, cancellationToken);
    }

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.GetValueAsync(cacheName, key, cancellationToken)
            : _remote.GetValueAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.RemoveAsync(cacheName, key, cancellationToken)
            : _remote.RemoveAsync(owner, cacheName, key, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var owner = OwnerFor(cacheName, key);
        return string.Equals(owner, _selfId, StringComparison.OrdinalIgnoreCase) ? _local.UpdateAsync(cacheName, key, value, cancellationToken)
            : _remote.UpdateAsync(owner, cacheName, key, value, cancellationToken);
    }

    private string OwnerFor(string cacheName, string key) => _locator.GetOwner(cacheName, key);
}
