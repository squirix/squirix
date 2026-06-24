using System.Globalization;
using System.Threading.Tasks;
using Squirix.E2ETests.Support.Client;
using Squirix.E2ETests.Support.Cluster.Fixtures;
using Xunit;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>Integration tests for multi-node public CRUD and cross-node visibility.</summary>
/// <param name="fixture">Shared two-node cluster fixture.</param>
public sealed class CrudTests(TwoNodeFixture fixture) : MultiNodeTestBase(fixture)
{
    /// <summary>Verifies AddAsync(string, T) observes existing named-cache entries across nodes.</summary>
    [Fact]
    public async Task AddValueOnNodeBThrowsWhenKeyInsertedOnNodeA()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-add-conflict");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        _ = await Assert.ThrowsAsync<CacheConflictException>(() => Cluster.CacheB.AddAsync(key, "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>Verifies only one concurrent AddAsync succeeds for the same key across nodes.</summary>
    [Fact]
    public async Task ConcurrentAddFromBothNodesOnlyOneSucceeds()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "concurrent-add");

        var a = MultiNodeSupport.CaptureAddAsync(Cluster.CacheA, key, "a", DefaultCancellationToken);
        var b = MultiNodeSupport.CaptureAddAsync(Cluster.CacheB, key, "b", DefaultCancellationToken);

        var errors = await Task.WhenAll(a, b);

        _ = Assert.Single(errors, static e => e is null);
        _ = Assert.Single(errors, static e => e is CacheConflictException);
        Assert.True((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies only one concurrent TryAddAsync returns true for the same key across nodes.</summary>
    [Fact]
    public async Task ConcurrentTryAddFromBothNodesOnlyOneReturnsTrue()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "concurrent-try-add");

        var a = Cluster.CacheA.TryAddAsync(key, "a", cancellationToken: DefaultCancellationToken);
        var b = Cluster.CacheB.TryAddAsync(key, "b", cancellationToken: DefaultCancellationToken);

        var results = await Task.WhenAll(a, b);

        _ = Assert.Single(results, static r => r);
        _ = Assert.Single(results, static r => !r);
        Assert.True((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies concurrent upserts from different nodes converge to one visible value without corrupting reads.</summary>
    [Fact]
    public async Task ConcurrentUpsertsFromBothNodesLeaveReadableValue()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "concurrent-upsert");

        var tasks = new Task[50];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = i % 2 is 0 ? Cluster.CacheA.SetAsync(key, $"a-{i.ToString(CultureInfo.InvariantCulture)}", cancellationToken: DefaultCancellationToken)
                : Cluster.CacheB.SetAsync(key, $"b-{i.ToString(CultureInfo.InvariantCulture)}", cancellationToken: DefaultCancellationToken);
        }

        await Task.WhenAll(tasks);

        var valueA = await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken);
        var valueB = await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken);

        Assert.True(valueA.Found);
        Assert.Equal(valueA.Value, valueB.Value);
    }

    /// <summary>Verifies an external gRPC client connected to a non-owner node is routed through the server-side cluster pipeline.</summary>
    [Fact]
    public async Task ExternalClientConnectedToNodeARoutesMutationToOwnerNodeB()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "external-client-route");
        await using var client = await LoopbackConnect.ConnectAsync(Cluster.NodeAAddress, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<object?>("orders", DefaultCancellationToken);

        await cache.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v1", (await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.Equal("v1", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies GetEntryAsync sees a named-cache entry written by another node.</summary>
    [Fact]
    public async Task GetEntryOnNodeBReturnsEntryInsertedOnNodeA()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-get-entry");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        var entry = await Cluster.CacheB.GetEntryAsync(key, DefaultCancellationToken);

        Assert.True(entry.Found);
    }

    /// <summary>Verifies GetValueAsync sees a named-cache entry written by another node.</summary>
    [Fact]
    public async Task GetValueOnNodeBReturnsTrueWhenKeyInsertedOnNodeA()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-get-value");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.True((await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies an update through one node is immediately visible when reading through another node.</summary>
    [Fact]
    public async Task InsertOnNodeAUpdateOnNodeBGetOnNodeAReturnsLatestValue()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "cross-node-update");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);
        await Cluster.CacheB.SetAsync(key, "v2", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v2", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies SetAsync(string, T) writes are visible from another node for the same named cache.</summary>
    [Fact]
    public async Task InsertValueOnNodeAThenGetOnNodeBReturnsInsertedValue()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-insert-get");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v1", (await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies RemoveAsync on one node removes a named-cache entry written on another node.</summary>
    [Fact]
    public async Task RemoveNodeBDeletesEntryInsertedOnNodeA()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-remove-entry");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken));
    }

    /// <summary>Verifies a remove through one node makes the key missing through another node.</summary>
    [Fact]
    public async Task RemoveNodeBThenGetOnNodeAReturnsNull()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "cross-node-remove");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken));
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies the same key in different named caches remains isolated across cluster nodes.</summary>
    [Fact]
    public async Task SameKeyInDifferentNamedCachesRemainsIsolatedAcrossNodes()
    {
        await Cluster.CacheA.SetAsync("same-key", "order-value", cancellationToken: DefaultCancellationToken);
        await Cluster.CustomerCacheA.SetAsync("same-key", "customer-value", cancellationToken: DefaultCancellationToken);

        Assert.Equal("order-value", (await Cluster.CacheB.GetValueAsync("same-key", DefaultCancellationToken)).Value);
        Assert.Equal("customer-value", (await Cluster.CustomerCacheB.GetValueAsync("same-key", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TryAddAsync(string, T) observes existing named-cache values across nodes.</summary>
    [Fact]
    public async Task TryAddValueOnNodeBReturnsFalseWhenKeyInsertedOnNodeA()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-try-add");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        Assert.False(await Cluster.CacheB.TryAddAsync(key, "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>Verifies a stored null value remains distinguishable from a missing key across nodes.</summary>
    [Fact]
    public async Task TryGetValueOnNodeBReturnsFoundForNullValueInsertedOnNodeA()
    {
        await Cluster.CacheA.SetAsync("null-key", null, cancellationToken: DefaultCancellationToken);

        var result = await Cluster.CacheB.GetValueAsync("null-key", DefaultCancellationToken);

        Assert.True(result.Found);
        Assert.False((await Cluster.CacheB.GetValueAsync("missing-null-key", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetValueAsync sees a named-cache value written by another node.</summary>
    [Fact]
    public async Task TryGetValueOnNodeBReturnsValueInsertedOnNodeA()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-try-get-value");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        var result = await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken);

        Assert.True(result.Found);
    }

    /// <summary>Verifies RemoveAsync can remove a named-cache entry written by another node.</summary>
    [Fact]
    public async Task TryRemoveOnNodeBRemovesEntryInsertedOnNodeA()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-try-remove");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);

        var result = await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken);

        Assert.True(result);
    }

    /// <summary>Verifies remote RemoveAsync removes an entry after it was read.</summary>
    [Fact]
    public async Task TryRemoveOnNodeBReturnsRemoteRemovedEntryMetadata()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-entry-metadata");

        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: DefaultCancellationToken);

        var before = await Cluster.CacheA.GetEntryAsync(key, DefaultCancellationToken);
        Assert.True(before.Found);

        var removed = await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies remote RemoveAsync removes a stored null value.</summary>
    [Fact]
    public async Task TryRemoveOnNodeBStoredNullReportsRemoved()
    {
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-null");

        await Cluster.CacheA.SetAsync(key, null, cancellationToken: DefaultCancellationToken);

        var removed = await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }
}
