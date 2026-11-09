using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using Squirix.E2ETests.Fixtures.TypedValues;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Integration tests for typed custom values restored through durable restart recovery.</summary>
[Immutable]
public sealed class DurableTypedValueRestartTests
{
    /// <summary>Verifies both custom value types are restored from the journal after a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresTypedValuesFromJournal(CancellationToken cancellationToken)
    {
        const string profileCacheName = "typed-durable-record";
        const string cartCacheName = "typed-durable-cart";
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(RestartRestoresTypedValuesFromJournal),
            persistence: true,
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        var profileCache = await cluster.GetCacheAsync<TypedCustomerProfile>(profileCacheName, cancellationToken: cancellationToken);
        var cartCache = await cluster.GetCacheAsync<TypedMutableCart>(cartCacheName, cancellationToken: cancellationToken);
        var expectedProfile = TypedValueFactory.CreateProfile("journal-record");
        var expectedCart = TypedValueFactory.CreateCart("journal-cart");
        await profileCache.SetAsync("k", expectedProfile, cancellationToken: cancellationToken);
        await cartCache.SetAsync("k", expectedCart, cancellationToken: cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);

        var restartedProfileCache = await cluster.GetCacheAsync<TypedCustomerProfile>(profileCacheName, cancellationToken: cancellationToken);
        var profile = await restartedProfileCache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(profile.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEqualsAsync(expectedProfile, profile.Value!);

        var restartedCartCache = await cluster.GetCacheAsync<TypedMutableCart>(cartCacheName, cancellationToken: cancellationToken);
        var cart = await restartedCartCache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(cart.Found).IsTrue();
        await TypedValueAssertions.AssertCartEqualsAsync(expectedCart, cart.Value!);
    }

    /// <summary>Verifies RestartSkipsExpiredCustomRecord.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartSkipsExpiredCustomRecord(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(RestartSkipsExpiredCustomRecord),
            persistence: true,
            timeProvider: clock,
            cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", cancellationToken: cancellationToken);
        await cache.SetAsync("k", TypedValueFactory.CreateProfile("expired"), Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);

        // Advance the fake node clock past the TTL so the in-memory entry is deterministically expired before restart.
        clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restartedCache = await cluster.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", cancellationToken: cancellationToken);
        _ = await Assert.That((await restartedCache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }
}
