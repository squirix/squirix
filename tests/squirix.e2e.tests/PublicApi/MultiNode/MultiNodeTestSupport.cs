using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Infrastructure;
using Xunit;

namespace Squirix.E2ETests.PublicApi.MultiNode;

/// <summary>
/// Per-test cluster startup and routing helpers for multi-node public API tests.
/// </summary>
internal static class MultiNodeTestSupport
{
    internal static async Task<Exception?> CaptureAddAsync(ICache<object?> cache, string key, object? value)
    {
        try
        {
            await cache.AddAsync(key, value, cancellationToken: TestContext.Current.CancellationToken);
            return null;
        }
        catch (RpcException ex)
        {
            return ex;
        }
        catch (CacheConflictException ex)
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

    internal static string FindKeyOwnedBy(string cacheName, string ownerId, string prefix) => FindKeysOwnedBy(cacheName, ownerId, 1, prefix)[0];

    internal static CacheEntryOptions? Options(TimeSpan? expiration = null) => expiration is null ? null : new CacheEntryOptions { Expiration = expiration };

    internal static async Task<TwoNodeNamedCaches<T>> StartTwoNodeNamedCachesAsync<T>([CallerMemberName] string testName = "")
    {
        var cluster = await HostedCluster.StartTwoNodeAsync(testName, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            var clientA = await cluster.ConnectClientAsync("nodeA", TestContext.Current.CancellationToken);
            var clientB = await cluster.ConnectClientAsync("nodeB", TestContext.Current.CancellationToken);
            return await TwoNodeNamedCaches<T>.CreateAsync(cluster, clientA, clientB, TestContext.Current.CancellationToken);
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
        new KeyOwnerHelper(["nodeA", "nodeB"]).FindKeysOwnedBy(cacheName, ownerId, count, prefix);
}
