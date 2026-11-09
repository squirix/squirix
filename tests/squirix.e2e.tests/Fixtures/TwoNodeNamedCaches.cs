using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Connected two-node named caches for multi-node public API tests.</summary>
/// <typeparam name="T">Cached value type.</typeparam>
[Immutable]
public sealed class TwoNodeNamedCaches<T> : IAsyncDisposable
{
    private readonly HostedCluster _host;
    private readonly bool _ownsLifetime;

    private TwoNodeNamedCaches(HostedCluster host, Caches caches, bool ownsLifetime)
    {
        _host = host;
        CacheA = caches.CacheA;
        CacheB = caches.CacheB;
        CustomerCacheA = caches.CustomerCacheA;
        CustomerCacheB = caches.CustomerCacheB;
        _ownsLifetime = ownsLifetime;
    }

    /// <summary>Gets the node A <c language="csharp">orders</c> cache facade.</summary>
    public ICache<T> CacheA { get; }

    /// <summary>Gets the node B <c language="csharp">orders</c> cache facade.</summary>
    public ICache<T> CacheB { get; }

    /// <summary>Gets the node A <c language="csharp">customers</c> cache facade.</summary>
    public ICache<T> CustomerCacheA { get; }

    /// <summary>Gets the node B <c language="csharp">customers</c> cache facade.</summary>
    public ICache<T> CustomerCacheB { get; }

    /// <summary>Gets the node A listen address.</summary>
    public Uri NodeAAddress => _host.GetUri("nodeA");

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _ownsLifetime ? _host.DisposeAsync() : ValueTask.CompletedTask;

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
        return new TwoNodeNamedCaches<T>(host, caches, ownsLifetime);
    }

    [Immutable]
    private sealed class Caches
    {
        internal required ICache<T> CacheA { get; init; }

        internal required ICache<T> CacheB { get; init; }

        internal required ICache<T> CustomerCacheA { get; init; }

        internal required ICache<T> CustomerCacheB { get; init; }
    }
}
