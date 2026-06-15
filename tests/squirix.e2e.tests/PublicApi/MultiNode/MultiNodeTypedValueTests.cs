using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Squirix.E2ETests.PublicApi.TypedValues;
using Xunit;

namespace Squirix.E2ETests.PublicApi.MultiNode;

/// <summary>
/// Integration tests for typed custom values routed through a two-node public cache API cluster.
/// </summary>
public sealed class MultiNodeTypedValueTests(TwoNodeFixture fixture) : PublicApiMultiNodeTestBase(fixture)
{
    /// <summary>
    /// Verifies GetOrAddShouldStoreCustomRecordForRemoteOwnerAcrossTwoNodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetOrAddShouldStoreCustomRecordForRemoteOwnerAcrossTwoNodes()
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>();
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-get-or-add");
        var expected = TypedValueFactory.CreateProfile(key);

        var added = await cluster.CacheA.GetOrAddAsync(
            key,
            static (factoryKey, _) => Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateProfile(factoryKey)),
            cancellationToken: DefaultCancellationToken);
        var reread = await cluster.CacheA.GetOrAddAsync(
            key,
            static (_, _) => Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateUpdatedProfile("unused")),
            cancellationToken: DefaultCancellationToken);

        Assert.True(added.Found);
        TypedValueAssertions.AssertProfileEquals(expected, added.Value!);
        Assert.True(reread.Found);
        TypedValueAssertions.AssertProfileEquals(expected, reread.Value!);
    }

    /// <summary>
    /// Verifies LocalOwnerKeyShouldRoundTripCustomRecordAcrossTwoNodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task LocalOwnerKeyShouldRoundTripCustomRecordAcrossTwoNodes()
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>();
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "typed-local-record");
        var expected = TypedValueFactory.CreateProfile(key);

        await cluster.CacheA.SetAsync(key, expected, cancellationToken: DefaultCancellationToken);

        var result = await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>
    /// Verifies RemoteOwnerKeyShouldRoundTripCustomRecordAcrossTwoNodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoteOwnerKeyShouldRoundTripCustomRecordAcrossTwoNodes()
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>();
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-record");
        var expected = TypedValueFactory.CreateProfile(key);

        await cluster.CacheA.SetAsync(key, expected, cancellationToken: DefaultCancellationToken);

        var result = await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>
    /// Verifies RemoveShouldDeleteRemoteOwnerCustomRecordAcrossTwoNodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveShouldDeleteRemoteOwnerCustomRecordAcrossTwoNodes()
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>();
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-remove");

        await cluster.CacheA.SetAsync(key, TypedValueFactory.CreateProfile(key), cancellationToken: DefaultCancellationToken);

        Assert.True(await cluster.CacheA.RemoveAsync(key, DefaultCancellationToken));
        Assert.False((await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies SetAndGetShouldRoundTripCustomRecordAcrossTwoNodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task SetAndGetShouldRoundTripCustomRecordAcrossTwoNodes()
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>();
        var nodeAKey = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "typed-mixed-record-a");
        var nodeBKey = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeB", "typed-mixed-record-b");
        var nodeAValue = TypedValueFactory.CreateProfile(nodeAKey);
        var nodeBValue = TypedValueFactory.CreateUpdatedProfile(nodeBKey);

        await cluster.CacheA.SetAsync(nodeAKey, nodeAValue, cancellationToken: DefaultCancellationToken);
        await cluster.CacheA.SetAsync(nodeBKey, nodeBValue, cancellationToken: DefaultCancellationToken);

        var nodeAResult = await cluster.CacheA.GetValueAsync(nodeAKey, DefaultCancellationToken);
        var nodeBResult = await cluster.CacheA.GetValueAsync(nodeBKey, DefaultCancellationToken);
        Assert.True(nodeAResult.Found);
        Assert.True(nodeBResult.Found);
        TypedValueAssertions.AssertProfileEquals(nodeAValue, nodeAResult.Value!);
        TypedValueAssertions.AssertProfileEquals(nodeBValue, nodeBResult.Value!);
    }

    /// <summary>
    /// Verifies SetAndGetShouldRoundTripMutableClassAcrossTwoNodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task SetAndGetShouldRoundTripMutableClassAcrossTwoNodes()
    {
        var cluster = await GetNamedCachesAsync<TypedMutableCart>();
        var nodeAKey = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "typed-mixed-cart-a");
        var nodeBKey = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeB", "typed-mixed-cart-b");
        var nodeAValue = TypedValueFactory.CreateCart(nodeAKey);
        var nodeBValue = TypedValueFactory.CreateUpdatedCart(nodeBKey);

        await cluster.CacheA.SetAsync(nodeAKey, nodeAValue, cancellationToken: DefaultCancellationToken);
        await cluster.CacheA.SetAsync(nodeBKey, nodeBValue, cancellationToken: DefaultCancellationToken);

        var nodeAResult = await cluster.CacheA.GetValueAsync(nodeAKey, DefaultCancellationToken);
        var nodeBResult = await cluster.CacheA.GetValueAsync(nodeBKey, DefaultCancellationToken);
        Assert.True(nodeAResult.Found);
        Assert.True(nodeBResult.Found);
        TypedValueAssertions.AssertCartEquals(nodeAValue, nodeAResult.Value!);
        TypedValueAssertions.AssertCartEquals(nodeBValue, nodeBResult.Value!);
    }

    /// <summary>
    /// Verifies UpdateShouldPreserveExpirationForRemoteOwnerCustomRecordAcrossTwoNodes.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task UpdateShouldPreserveExpirationForRemoteOwnerCustomRecordAcrossTwoNodes()
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>();
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-update");
        var updated = TypedValueFactory.CreateUpdatedProfile(key);

        await cluster.CacheA.SetAsync(key, TypedValueFactory.CreateProfile(key), new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(5) }, DefaultCancellationToken);

        Assert.True(await cluster.CacheA.UpdateAsync(key, updated, DefaultCancellationToken));
        var result = await cluster.CacheA.GetValueAsync(key, DefaultCancellationToken);
        var expiration = await cluster.CacheA.GetExpirationAsync(key, DefaultCancellationToken);

        Assert.True(result.Found);
        Assert.True(expiration.HasExpiration);
        TypedValueAssertions.AssertProfileEquals(updated, result.Value!);
    }
}
