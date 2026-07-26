using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;

namespace Squirix.E2ETests.Cluster;

/// <summary>Connected two-node named caches for multi-node public API tests.</summary>
/// <typeparam name="T">Cached value type.</typeparam>
public sealed class TwoNodeNamedCaches<T> : IAsyncDisposable
{
    private readonly HostedCluster _host;
    private readonly bool _ownsLifetime;

    private TwoNodeNamedCaches(HostedCluster host, Clients clients, Caches caches, bool ownsLifetime)
    {
        _host = host;
        ClientA = clients.ClientA;
        ClientB = clients.ClientB;
        CacheA = caches.CacheA;
        CacheB = caches.CacheB;
        CustomerCacheA = caches.CustomerCacheA;
        CustomerCacheB = caches.CustomerCacheB;
        _ownsLifetime = ownsLifetime;
    }

    /// <summary>
    /// Gets the node A <c>orders</c> cache facade.
    /// </summary>
    public ICache<T> CacheA { get; }

    /// <summary>
    /// Gets the node B <c>orders</c> cache facade.
    /// </summary>
    public ICache<T> CacheB { get; }

    /// <summary>
    /// Gets the node A <c>customers</c> cache facade.
    /// </summary>
    public ICache<T> CustomerCacheA { get; }

    /// <summary>
    /// Gets the node B <c>customers</c> cache facade.
    /// </summary>
    public ICache<T> CustomerCacheB { get; }

    /// <summary>Gets the node A listen address.</summary>
    public Uri NodeAAddress => _host.GetUri("nodeA");

    private ISquirixClient ClientA { get; }

    private ISquirixClient ClientB { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (!_ownsLifetime)
            return;

        await ClientB.DisposeAsync();
        await ClientA.DisposeAsync();
        await _host.DisposeAsync();
    }

    internal static async ValueTask<TwoNodeNamedCaches<T>> CreateAsync(
        HostedCluster host,
        ISquirixClient clientA,
        ISquirixClient clientB,
        CancellationToken cancellationToken,
        bool ownsLifetime = true)
    {
        var caches = new Caches
        {
            CacheA = await clientA.GetCacheAsync<T>("orders", cancellationToken),
            CacheB = await clientB.GetCacheAsync<T>("orders", cancellationToken),
            CustomerCacheA = await clientA.GetCacheAsync<T>("customers", cancellationToken),
            CustomerCacheB = await clientB.GetCacheAsync<T>("customers", cancellationToken),
        };
        return new TwoNodeNamedCaches<T>(host, new Clients { ClientA = clientA, ClientB = clientB }, caches, ownsLifetime);
    }

    private sealed class Caches
    {
        internal required ICache<T> CacheA { get; init; }

        internal required ICache<T> CacheB { get; init; }

        internal required ICache<T> CustomerCacheA { get; init; }

        internal required ICache<T> CustomerCacheB { get; init; }
    }

    private sealed class Clients
    {
        internal required ISquirixClient ClientA { get; init; }

        internal required ISquirixClient ClientB { get; init; }
    }
}
