using System;
using System.Threading.Tasks;
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Unit tests covering expiration operations: TouchAsync, GetExpirationAsync, and RemoveExpirationAsync.
/// </summary>
public sealed class ExpirationOperationsTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies RemoveExpirationAsync removes expiration for an existing expiring key and the value remains after the old expiration window.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncClearsExpirationAndKeepsKey()
    {
        await using var cache = new PhysicalCache<string>();
        await cache.InsertAsync("k1", new CacheEntry<string> { Value = "v", ExpiresUtc = DateTime.UtcNow.AddMilliseconds(150), Version = 1 }, DefaultCancellationToken);

        // Ensure expiration is present initially
        var expiration1 = Assert.NotNull(await cache.GetExpirationAsync("k1", DefaultCancellationToken));
        Assert.True(expiration1 > TimeSpan.Zero);

        // RemoveExpiration should clear expiration and return true.
        var ok = await cache.RemoveExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(ok);

        // expiration should now be null and the key should outlive old expiration
        Assert.Null(await cache.GetExpirationAsync("k1", DefaultCancellationToken));
        await Task.Delay(200, DefaultCancellationToken);
        var found = await cache.TryGetValueAsync("k1", DefaultCancellationToken);
        Assert.True(found.Found);
        Assert.Equal("v", found.Value);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync does not resurrect an already expired entry.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncDoesNotResurrectExpiredEntry()
    {
        var clock = new FakeClock(DateTime.UtcNow);
        await using var cache = new PhysicalCache<string>(clock);

        await cache.InsertAsync(
            "k",
            new CacheEntry<string>
            {
                Value = "v",
                Expiration = TimeSpan.FromMilliseconds(10),
            },
            DefaultCancellationToken);

        clock.Advance(TimeSpan.FromMilliseconds(30));

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        var result = await cache.TryGetValueAsync("k", DefaultCancellationToken);
        Assert.False(result.Found);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync removes expiration once and returns false when the key is already persistent.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncReturnsFalseWhenAlreadyPersistent()
    {
        await using var cache = new PhysicalCache<string>();
        await cache.InsertAsync(
            "k",
            new CacheEntry<string>
            {
                Value = "v",
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken);

        Assert.True(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        var entry = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("v", entry.Value);
        Assert.Null(await cache.GetExpirationAsync("k", DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync on a non-expiring key returns false and leaves the value and absence of expiration unchanged.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncOnNonExpiringKeyReturnsFalseAndKeepsKeyLive()
    {
        await using var cache = new PhysicalCache<string>();
        await cache.InsertAsync("k", new CacheEntry<string> { Value = "v", Version = 1 }, DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        var entry = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("v", entry.Value);
        Assert.Null(await cache.GetExpirationAsync("k", DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync returns false for a missing key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncReturnsFalseForMissingKey()
    {
        await using var cache = new PhysicalCache<int>();
        Assert.False(await cache.RemoveExpirationAsync("missing", DefaultCancellationToken));
    }
}
