using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Infrastructure;

namespace Squirix.E2ETests.PublicApi.MultiNode;

/// <summary>
/// Shared fixtures for multi-node v0.1 public <see cref="ICache{T}" /> integration tests.
/// </summary>
public abstract class PublicApiMultiNodeTestBase : E2ETestBase
{
    internal static async Task<Exception?> CaptureAddAsync(ICache<object?> cache, string key, object? value)
    {
        try
        {
            await cache.AddAsync(key, value, cancellationToken: DefaultCancellationToken);
            return null;
        }
        catch (RpcException ex)
        {
            return ex;
        }
        catch (InvalidOperationException ex)
        {
            return ex;
        }
        catch (IOException ex)
        {
            return ex;
        }
    }

    internal static CacheEntryOptions? Options(TimeSpan? expiration = null) =>
        expiration is null ? null : new CacheEntryOptions { Expiration = expiration };

    internal static string FindKeyOwnedBy(string cacheName, string ownerId, string prefix) => FindKeysOwnedBy(cacheName, ownerId, 1, prefix)[0];

    internal static async Task<TwoNodeNamedCaches<T>> StartTwoNodeNamedCachesAsync<T>([CallerMemberName] string testName = "")
    {
        var cluster = await E2ECluster.StartTwoNodeAsync(testName, DefaultCancellationToken);
        try
        {
            var clientA = await cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
            var clientB = await cluster.ConnectClientAsync("nodeB", DefaultCancellationToken);
            var cacheA = await clientA.GetCacheAsync<T>("orders", DefaultCancellationToken);
            var cacheB = await clientB.GetCacheAsync<T>("orders", DefaultCancellationToken);
            var customerCacheA = await clientA.GetCacheAsync<T>("customers", DefaultCancellationToken);
            var customerCacheB = await clientB.GetCacheAsync<T>("customers", DefaultCancellationToken);
            return new TwoNodeNamedCaches<T>(cluster, clientA, clientB, cacheA, cacheB, customerCacheA, customerCacheB);
        }
        catch (InvalidOperationException)
        {
            await cluster.DisposeAsync();
            throw;
        }
        catch (IOException)
        {
            await cluster.DisposeAsync();
            throw;
        }
        catch (RpcException)
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    private static string[] FindKeysOwnedBy(string cacheName, string ownerId, int count, string prefix) =>
        new E2EKeyOwnerHelper(["nodeA", "nodeB"]).FindKeysOwnedBy(cacheName, ownerId, count, prefix);

    internal sealed class TwoNodeNamedCaches<T> : IAsyncDisposable
    {
        public TwoNodeNamedCaches(
            E2ECluster cluster,
            E2EClientHandle clientA,
            E2EClientHandle clientB,
            ICache<T> cacheA,
            ICache<T> cacheB,
            ICache<T> customerCacheA,
            ICache<T> customerCacheB)
        {
            Cluster = cluster;
            ClientA = clientA;
            ClientB = clientB;
            CacheA = cacheA;
            CacheB = cacheB;
            CustomerCacheA = customerCacheA;
            CustomerCacheB = customerCacheB;
        }

        public ICache<T> CacheA { get; }

        public ICache<T> CacheB { get; }

        public ICache<T> CustomerCacheA { get; }

        public ICache<T> CustomerCacheB { get; }

        public string NodeAAddress => Cluster.GetAddress("nodeA");

        private E2EClientHandle ClientA { get; }

        private E2EClientHandle ClientB { get; }

        private E2ECluster Cluster { get; }

        public async ValueTask DisposeAsync()
        {
            await ClientB.DisposeAsync();
            await ClientA.DisposeAsync();
            await Cluster.DisposeAsync();
        }
    }
}
