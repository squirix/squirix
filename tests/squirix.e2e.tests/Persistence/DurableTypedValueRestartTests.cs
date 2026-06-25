using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures.TypedValues;
using Squirix.E2ETests.Support;
using Squirix.E2ETests.Support.Restart;
using Xunit;

namespace Squirix.E2ETests.Persistence;

/// <summary>Integration tests for typed custom values restored through durable restart recovery.</summary>
public sealed class DurableTypedValueRestartTests : EndToEndTestBase
{
    /// <summary>Verifies RestartShouldNotRestoreExpiredCustomRecord.</summary>
    [Fact]
    public async Task RestartShouldNotRestoreExpiredCustomRecord()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartShouldNotRestoreExpiredCustomRecord), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);

        await cache.SetAsync("k", TypedValueFactory.CreateProfile("expired"), new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(100) }, DefaultCancellationToken);

        // Expiration is time-based; wait past the TTL before restart so recovery observes a deterministically expired entry.
        await Task.Delay(TimeSpan.FromMilliseconds(300), TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-expired", DefaultCancellationToken);

        Assert.False((await restartedCache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RestartShouldRestoreCustomRecordFromJournal.</summary>
    [Fact]
    public async Task RestartShouldRestoreCustomRecordFromJournal()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartShouldRestoreCustomRecordFromJournal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("journal-record");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedCustomerProfile>("typed-durable-record", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RestartShouldRestoreMutableClassFromJournal.</summary>
    [Fact]
    public async Task RestartShouldRestoreMutableClassFromJournal()
    {
        await using var node = await RestartableSingleNode.StartAsync(nameof(RestartShouldRestoreMutableClassFromJournal), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateCart("journal-cart");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restartedCache = await node.GetCacheAsync<TypedMutableCart>("typed-durable-cart", DefaultCancellationToken);
        var result = await restartedCache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        TypedValueAssertions.AssertCartEquals(expected, result.Value!);
    }
}
