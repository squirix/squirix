using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures.TypedValues;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>Integration tests for typed custom values routed through a two-node public cache API cluster.</summary>
[Immutable]
public sealed class CrossNodeTypedValueTests : CrossNodeTestBase
{
    /// <summary>Verifies CustomRecordRoundTripsAcrossTwoNodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CustomRecordRoundTripsAcrossTwoNodes(CancellationToken cancellationToken)
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>(cancellationToken);
        var nodeAKey = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "typed-mixed-record-a");
        var nodeBKey = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "typed-mixed-record-b");
        var nodeAValue = TypedValueFactory.CreateProfile(nodeAKey);
        var nodeBValue = TypedValueFactory.CreateUpdatedProfile(nodeBKey);
        await cluster.CacheA.SetAsync(nodeAKey, nodeAValue, cancellationToken: cancellationToken);
        await cluster.CacheA.SetAsync(nodeBKey, nodeBValue, cancellationToken: cancellationToken);
        var nodeAResult = await cluster.CacheA.GetValueAsync(nodeAKey, cancellationToken);
        var nodeBResult = await cluster.CacheA.GetValueAsync(nodeBKey, cancellationToken);
        _ = await Assert.That(nodeAResult.Found).IsTrue();
        _ = await Assert.That(nodeBResult.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(nodeAValue, nodeAResult.Value!);
        await TypedValueAssertions.AssertProfileEquals(nodeBValue, nodeBResult.Value!);
    }

    /// <summary>Verifies GetOrAddStoresCustomRecordForRemoteOwner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddStoresCustomRecordForRemoteOwner(CancellationToken cancellationToken)
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>(cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-get-or-add");
        var expected = TypedValueFactory.CreateProfile(key);
        var added = await cluster.CacheA.GetOrAddAsync(
            key,
            static (factoryKey, _) => Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateProfile(factoryKey)),
            cancellationToken: cancellationToken);
        var reread = await cluster.CacheA.GetOrAddAsync(
            key,
            static (_, _) => Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateUpdatedProfile("unused")),
            cancellationToken: cancellationToken);
        _ = await Assert.That(added.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, added.Value!);
        _ = await Assert.That(reread.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, reread.Value!);
    }

    /// <summary>Verifies LocalOwnerCustomRecordRoundTripsTwoNodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LocalOwnerCustomRecordRoundTripsTwoNodes(CancellationToken cancellationToken)
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>(cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "typed-local-record");
        var expected = TypedValueFactory.CreateProfile(key);
        await cluster.CacheA.SetAsync(key, expected, cancellationToken: cancellationToken);
        var result = await cluster.CacheA.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies MutableClassRoundTripsAcrossTwoNodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MutableClassRoundTripsAcrossTwoNodes(CancellationToken cancellationToken)
    {
        var cluster = await GetNamedCachesAsync<TypedMutableCart>(cancellationToken);
        var nodeAKey = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "typed-mixed-cart-a");
        var nodeBKey = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "typed-mixed-cart-b");
        var nodeAValue = TypedValueFactory.CreateCart(nodeAKey);
        var nodeBValue = TypedValueFactory.CreateUpdatedCart(nodeBKey);
        await cluster.CacheA.SetAsync(nodeAKey, nodeAValue, cancellationToken: cancellationToken);
        await cluster.CacheA.SetAsync(nodeBKey, nodeBValue, cancellationToken: cancellationToken);
        var nodeAResult = await cluster.CacheA.GetValueAsync(nodeAKey, cancellationToken);
        var nodeBResult = await cluster.CacheA.GetValueAsync(nodeBKey, cancellationToken);
        _ = await Assert.That(nodeAResult.Found).IsTrue();
        _ = await Assert.That(nodeBResult.Found).IsTrue();
        await TypedValueAssertions.AssertCartEquals(nodeAValue, nodeAResult.Value!);
        await TypedValueAssertions.AssertCartEquals(nodeBValue, nodeBResult.Value!);
    }

    /// <summary>Verifies RemoteOwnerCustomRecordRoundTripsNodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteOwnerCustomRecordRoundTripsNodes(CancellationToken cancellationToken)
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>(cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-record");
        var expected = TypedValueFactory.CreateProfile(key);
        await cluster.CacheA.SetAsync(key, expected, cancellationToken: cancellationToken);
        var result = await cluster.CacheA.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RemoveDeletesRemoteOwnerCustomRecord.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveDeletesRemoteOwnerCustomRecord(CancellationToken cancellationToken)
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>(cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-remove");
        await cluster.CacheA.SetAsync(key, TypedValueFactory.CreateProfile(key), cancellationToken: cancellationToken);
        _ = await Assert.That(await cluster.CacheA.RemoveAsync(key, cancellationToken)).IsTrue();
        _ = await Assert.That((await cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies UpdateKeepsExpiryOfRemoteCustomRecord.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateKeepsExpiryOfRemoteCustomRecord(CancellationToken cancellationToken)
    {
        var cluster = await GetNamedCachesAsync<TypedCustomerProfile>(cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "typed-remote-update");
        var updated = TypedValueFactory.CreateUpdatedProfile(key);
        await cluster.CacheA.SetAsync(key, TypedValueFactory.CreateProfile(key), Expiry.In(TimeSpan.FromMinutes(5)), cancellationToken);
        _ = await Assert.That(await cluster.CacheA.UpdateAsync(key, updated, cancellationToken)).IsTrue();
        var result = await cluster.CacheA.GetValueAsync(key, cancellationToken);
        var expiration = await cluster.CacheA.GetExpirationAsync(key, cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(updated, result.Value!);
    }
}
