using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Squirix.E2ETests.PublicApi.TypedValues;
using Xunit;

namespace Squirix.E2ETests.PublicApi.Durability;

/// <summary>
/// Integration tests for typed custom values restored through durable restart recovery.
/// </summary>
public sealed class DurabilityTypedValueTests : E2ETestBase
{
    /// <summary>
    /// Verifies RestartShouldNotRestoreExpiredCustomRecord.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RestartShouldNotRestoreExpiredCustomRecord()
    {
        await using var node = await E2ERestartableSingleNode.StartAsync(nameof(RestartShouldNotRestoreExpiredCustomRecord), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            TypedValueFactory.CreateProfile("expired"),
            new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(100) },
            DefaultCancellationToken);

        // Expiration is time-based; wait past the TTL before restart so recovery observes a deterministically expired entry.
        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);

        Assert.False((await restartedCache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RestartShouldRestoreCustomRecordFromWal.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RestartShouldRestoreCustomRecordFromWal()
    {
        await using var node = await E2ERestartableSingleNode.StartAsync(nameof(RestartShouldRestoreCustomRecordFromWal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("wal-record");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>
    /// Verifies RestartShouldRestoreMutableClassFromWal.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RestartShouldRestoreMutableClassFromWal()
    {
        await using var node = await E2ERestartableSingleNode.StartAsync(nameof(RestartShouldRestoreMutableClassFromWal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateCart("wal-cart");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        TypedValueAssertions.AssertCartEquals(expected, result.Value!);
    }
}
