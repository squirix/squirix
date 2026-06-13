using System.Linq;
using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Xunit;

namespace Squirix.E2ETests.PublicApi.MultiNode;

/// <summary>
/// Integration tests for multi-node public CRUD and cross-node visibility.
/// </summary>
public sealed class MultiNodeCrudTests : PublicApiMultiNodeTestBase
{
    /// <summary>
    /// Verifies AddAsync(string, T) observes existing named-cache entries across nodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task AddValueOnNodeBThrowsWhenKeyInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        _ = await Assert.ThrowsAsync<CacheConflictException>(async () => await cluster.CacheB.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies only one concurrent AddAsync succeeds for the same key across nodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task ConcurrentAddFromBothNodesOnlyOneSucceeds()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        var key = FindKeyOwnedBy("orders", "nodeB", "concurrent-add");

        var a = CaptureAddAsync(cluster.CacheA, key, "a");
        var b = CaptureAddAsync(cluster.CacheB, key, "b");

        var errors = await Task.WhenAll(a, b);

        _ = Assert.Single(errors, static e => e is null);
        _ = Assert.Single(errors, static e => e is CacheConflictException);
        Assert.True((await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies only one concurrent TryAddAsync returns true for the same key across nodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task ConcurrentTryAddFromBothNodesOnlyOneReturnsTrue()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        var key = FindKeyOwnedBy("orders", "nodeB", "concurrent-try-add");

        var a = cluster.CacheA.TryAddAsync(key, "a", cancellationToken: DefaultCancellationToken);
        var b = cluster.CacheB.TryAddAsync(key, "b", cancellationToken: DefaultCancellationToken);

        var results = await Task.WhenAll(a, b);

        _ = Assert.Single(results, static r => r);
        _ = Assert.Single(results, static r => !r);
        Assert.True((await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies concurrent upserts from different nodes converge to one visible value without corrupting reads.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task ConcurrentUpsertsFromBothNodesLeaveReadableValue()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        var key = FindKeyOwnedBy("orders", "nodeB", "concurrent-upsert");

        var tasks = Enumerable.Range(0, 50).Select(i =>
            i % 2 == 0 ? cluster.CacheA.SetAsync(key, $"a-{i}", cancellationToken: DefaultCancellationToken)
                : cluster.CacheB.SetAsync(key, $"b-{i}", cancellationToken: DefaultCancellationToken)).ToArray();

        await Task.WhenAll(tasks);

        var valueA = await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken);
        var valueB = await cluster.CacheB.GetValueAsync(key, DefaultCancellationToken);

        Assert.True(valueA.Found);
        Assert.Equal(valueA.Value, valueB.Value);
    }

    /// <summary>
    /// Verifies an external gRPC client connected to a non-owner node is routed through the server-side cluster pipeline.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task ExternalClientConnectedToNodeARoutesMutationToOwnerNodeB()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        var key = FindKeyOwnedBy("orders", "nodeB", "external-client-route");
        await using var client = await E2ETestConnect.ConnectAsync(cluster.NodeAAddress, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<object?>("orders", DefaultCancellationToken);

        await cache.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v1", (await cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.Equal("v1", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies GetEntryAsync sees a named-cache entry written by another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task GetEntryOnNodeBReturnsEntryInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        var entry = await cluster.CacheB.GetEntryAsync("k1", DefaultCancellationToken);

        Assert.True(entry.Found);
    }

    /// <summary>
    /// Verifies GetValueAsync sees a named-cache entry written by another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task GetValueOnNodeBReturnsTrueWhenKeyInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        Assert.True((await cluster.CacheB.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies an update through one node is immediately visible when reading through another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task InsertOnNodeAUpdateOnNodeBGetOnNodeAReturnsLatestValue()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();

        var key = FindKeyOwnedBy("orders", "nodeB", "cross-node-update");

        await cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);
        await cluster.CacheB.SetAsync(key, "v2", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v2", (await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies SetAsync(string, T) writes are visible from another node for the same named cache.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task InsertValueOnNodeAThenGetOnNodeBReturnsInsertedValue()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v1", (await cluster.CacheB.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies RemoveAsync on one node removes a named-cache entry written on another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RemoveNodeBDeletesEntryInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        Assert.True(await cluster.CacheB.RemoveAsync("k1", DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies a remove through one node makes the key missing through another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RemoveNodeBThenGetOnNodeAReturnsNull()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();

        var key = FindKeyOwnedBy("orders", "nodeB", "cross-node-remove");

        await cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.True(await cluster.CacheB.RemoveAsync(key, DefaultCancellationToken));
        Assert.False((await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies the same key in different named caches remains isolated across cluster nodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task SameKeyInDifferentNamedCachesRemainsIsolatedAcrossNodes()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();

        await cluster.CacheA.SetAsync("same-key", "order-value", cancellationToken: DefaultCancellationToken);
        await cluster.CustomerCacheA.SetAsync("same-key", "customer-value", cancellationToken: DefaultCancellationToken);

        Assert.Equal("order-value", (await cluster.CacheB.GetValueAsync("same-key", DefaultCancellationToken)).Value);
        Assert.Equal("customer-value", (await cluster.CustomerCacheB.GetValueAsync("same-key", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TryAddAsync(string, T) observes existing named-cache values across nodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryAddValueOnNodeBReturnsFalseWhenKeyInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        Assert.False(await cluster.CacheB.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies a stored null value remains distinguishable from a missing key across nodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryGetValueOnNodeBReturnsFoundForNullValueInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();

        await cluster.CacheA.SetAsync("null-key", null, cancellationToken: DefaultCancellationToken);

        var result = await cluster.CacheB.GetValueAsync("null-key", DefaultCancellationToken);

        Assert.True(result.Found);
        Assert.False((await cluster.CacheB.GetValueAsync("missing-null-key", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies GetValueAsync sees a named-cache value written by another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryGetValueOnNodeBReturnsValueInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        var result = await cluster.CacheB.GetValueAsync("k1", DefaultCancellationToken);

        Assert.True(result.Found);
    }

    /// <summary>
    /// Verifies RemoveAsync can remove a named-cache entry written by another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryRemoveOnNodeBRemovesEntryInsertedOnNodeA()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        await cluster.CacheA.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);

        var result = await cluster.CacheB.RemoveAsync("k1", DefaultCancellationToken);

        Assert.True(result);
    }

    /// <summary>
    /// Verifies remote RemoveAsync removes an entry after it was read.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryRemoveOnNodeBReturnsRemoteRemovedEntryMetadata()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        var key = FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-entry-metadata");

        await cluster.CacheA.SetAsync(key, "v", cancellationToken: DefaultCancellationToken);

        var before = await cluster.CacheA.GetEntryAsync(key, DefaultCancellationToken);
        Assert.True(before.Found);

        var removed = await cluster.CacheB.RemoveAsync(key, DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies remote RemoveAsync removes a stored null value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryRemoveOnNodeBStoredNullReportsRemoved()
    {
        await using var cluster = await StartTwoNodeNamedCachesAsync<object?>();
        var key = FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-null");

        await cluster.CacheA.SetAsync(key, null, cancellationToken: DefaultCancellationToken);

        var removed = await cluster.CacheB.RemoveAsync(key, DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }
}
