using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using Squirix.E2ETests.Fixtures.TypedValues;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Integration tests for typed custom values restored through durable restart recovery.</summary>
[Immutable]
public sealed class DurableTypedValueRestartTests : EndToEndTestBase
{
    /// <summary>Verifies RestartRestoresCustomRecordFromJournal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresCustomRecordFromJournal(CancellationToken cancellationToken)
    {
        const string name = nameof(RestartRestoresCustomRecordFromJournal);
        await using var cluster = await HostedCluster.StartSingleNodeAsync(name, persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", cancellationToken: cancellationToken);
        var expected = TypedValueFactory.CreateProfile("journal-record");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restartedCache = await cluster.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", cancellationToken: cancellationToken);
        var result = await restartedCache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEqualsAsync(expected, result.Value!);
    }

    /// <summary>Verifies RestartRestoresMutableClassFromJournal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresMutableClassFromJournal(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(RestartRestoresMutableClassFromJournal),
            persistence: true,
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<TypedMutableCart>("typed-durable-cart", cancellationToken: cancellationToken);
        var expected = TypedValueFactory.CreateCart("journal-cart");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restartedCache = await cluster.GetCacheAsync<TypedMutableCart>("typed-durable-cart", cancellationToken: cancellationToken);
        var result = await restartedCache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertCartEqualsAsync(expected, result.Value!);
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
