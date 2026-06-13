using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Unit tests for derived cache mutations on the server local cache surface.
/// </summary>
public sealed class CacheDerivedMutationTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures ClientCache GetOrAddAsync invokes the factory once under concurrency.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ClientCacheGetOrAddAsyncInvokesFactoryOnceUnderConcurrency()
    {
        await using var physical = new PhysicalCache<string>();
        var clientCache = new ClientCache<string>(physical, physical);
        var factoryCalls = 0;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = clientCache.GetOrAddWithFactoryAsync(
            "orders",
            "k",
            async (key, cancellationToken) =>
            {
                _ = key;
                _ = cancellationToken;
                _ = Interlocked.Increment(ref factoryCalls);
                await gate.Task.ConfigureAwait(false);
                return "created";
            },
            null,
            DefaultCancellationToken);
        var second = clientCache.GetOrAddWithFactoryAsync(
            "orders",
            "k",
            async (key, cancellationToken) =>
            {
                _ = key;
                _ = cancellationToken;
                _ = Interlocked.Increment(ref factoryCalls);
                await gate.Task.ConfigureAwait(false);
                return "created";
            },
            null,
            DefaultCancellationToken);
        await Task.Delay(50, DefaultCancellationToken);
        gate.SetResult();
        var results = await Task.WhenAll(first.AsTask(), second.AsTask());

        Assert.Equal(1, factoryCalls);
        foreach (var result in results)
        {
            Assert.True(result.Found);
            Assert.Equal("created", result.Value);
        }
    }

    /// <summary>
    /// Ensures ClientCache UpdateAsync preserves expiration through the adapter.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ClientCacheUpdateAsyncPreservesExpiration()
    {
        var clock = new FakeClock(DateTime.UtcNow);
        await using var physical = new PhysicalCache<string>(clock);
        var clientCache = new ClientCache<string>(physical, physical);
        var expires = clock.UtcNow.AddMinutes(10);
        await clientCache.SetAsync("orders", "k", new CacheEntry<string> { Value = "old", ExpiresUtc = expires }, DefaultCancellationToken);

        var updated = await clientCache.UpdateAsync("orders", "k", "new", DefaultCancellationToken);

        Assert.True(updated);
        var entry = await clientCache.GetEntryAsync("orders", "k", DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("new", entry.Value);
        Assert.Equal(expires, entry.ExpiresUtc);
    }

    /// <summary>
    /// Ensures UpdateAsync changes the value while preserving expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task UpdateAsyncPreservesExpirationOnPhysicalCache()
    {
        var clock = new FakeClock(DateTime.UtcNow);
        await using var cache = new PhysicalCache<string>(clock);
        var expires = clock.UtcNow.AddMinutes(5);
        await cache.InsertAsync("k", new CacheEntry<string> { Value = "old", ExpiresUtc = expires }, DefaultCancellationToken);

        var updated = await cache.UpdateAsync("k", "new", DefaultCancellationToken);

        Assert.True(updated);
        var entry = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("new", entry.Value);
        Assert.Equal(expires, entry.ExpiresUtc);
    }

    /// <summary>
    /// Ensures UpdateAsync returns false for missing keys.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task UpdateAsyncReturnsFalseForMissingKey()
    {
        await using var cache = new PhysicalCache<string>();
        var updated = await cache.UpdateAsync("missing", "new", DefaultCancellationToken);
        Assert.False(updated);
    }
}
