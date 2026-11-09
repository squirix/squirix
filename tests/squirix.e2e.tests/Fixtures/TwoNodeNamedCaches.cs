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

    private TwoNodeNamedCaches(HostedCluster host, ICache<T> cacheA, ICache<T> cacheB, bool ownsLifetime)
    {
        _host = host;
        CacheA = cacheA;
        CacheB = cacheB;
        _ownsLifetime = ownsLifetime;
    }

    /// <summary>Gets the node A named cache facade.</summary>
    public ICache<T> CacheA { get; }

    /// <summary>Gets the node B named cache facade.</summary>
    public ICache<T> CacheB { get; }

    /// <summary>Gets the node A listen address.</summary>
    public Uri NodeAAddress => _host.GetUri("nodeA");

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _ownsLifetime ? _host.DisposeAsync() : ValueTask.CompletedTask;

    internal static async ValueTask<TwoNodeNamedCaches<T>> CreateAsync(
        HostedCluster host,
        ISquirixClient clientA,
        ISquirixClient clientB,
        CancellationToken cancellationToken,
        bool ownsLifetime = true,
        string cacheName = "orders")
    {
        var cacheA = await clientA.GetCacheAsync<T>(cacheName, cancellationToken);
        var cacheB = await clientB.GetCacheAsync<T>(cacheName, cancellationToken);
        return new TwoNodeNamedCaches<T>(host, cacheA, cacheB, ownsLifetime);
    }
}
