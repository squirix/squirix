using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;

namespace Squirix.E2ETests.Support.Cluster;

/// <summary>Connected two-node named caches for multi-node public API tests.</summary>
/// <typeparam name="T">Cached value type.</typeparam>
public sealed class TwoNodeNamedCaches<T> : IAsyncDisposable
{
    private readonly bool _ownsLifetime;
    private readonly HostedCluster _host;

    private TwoNodeNamedCaches(
        HostedCluster host,
        ISquirixClient clientA,
        ISquirixClient clientB,
        ICache<T> cacheA,
        ICache<T> cacheB,
        ICache<T> customerCacheA,
        ICache<T> customerCacheB,
        bool ownsLifetime)
    {
        _host = host;
        ClientA = clientA;
        ClientB = clientB;
        CacheA = cacheA;
        CacheB = cacheB;
        CustomerCacheA = customerCacheA;
        CustomerCacheB = customerCacheB;
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
        var cacheA = await clientA.GetCacheAsync<T>("orders", cancellationToken);
        var cacheB = await clientB.GetCacheAsync<T>("orders", cancellationToken);
        var customerCacheA = await clientA.GetCacheAsync<T>("customers", cancellationToken);
        var customerCacheB = await clientB.GetCacheAsync<T>("customers", cancellationToken);
        return new TwoNodeNamedCaches<T>(host, clientA, clientB, cacheA, cacheB, customerCacheA, customerCacheB, ownsLifetime);
    }
}
