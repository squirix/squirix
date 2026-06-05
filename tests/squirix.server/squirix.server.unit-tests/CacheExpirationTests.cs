using System;
using System.Threading.Tasks;
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Unit tests for <see cref="PhysicalCache{T}" /> expiration and expiration handling.
/// Verifies both relative expiration (<see cref="CacheEntry{T}.Expiration" />) and absolute
/// expiration (<see cref="CacheEntry{T}.ExpiresUtc" />).
/// </summary>
public sealed class CacheExpirationTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures entries expire correctly when inserted with either relative expiration or absolute expiration.
    /// </summary>
    /// <param name="expirationMs">expiration in milliseconds when using relative expiration.</param>
    /// <param name="sleepMs">Delay before checking presence in milliseconds.</param>
    /// <param name="useAbsoluteExpires">If <c>true</c>, uses <see cref="CacheEntry{T}.ExpiresUtc" />; otherwise <see cref="CacheEntry{T}.Expiration" />.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Theory]
    [InlineData(10, 25, true)]
    [InlineData(10, 25, false)]
    [InlineData(25, 60, true)]
    [InlineData(25, 60, false)]
    public async Task ExpirationSyncTheoryTest(int expirationMs, int sleepMs, bool useAbsoluteExpires)
    {
        var clock = new FakeClock(DateTime.UtcNow);
        await using var cache = new PhysicalCache<string>(clock);

        var entry = useAbsoluteExpires
            ? new CacheEntry<string> { Value = "v", ExpiresUtc = clock.UtcNow.AddMilliseconds(expirationMs) }
            : new CacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMilliseconds(expirationMs) };

        await cache.InsertAsync("k", entry, DefaultCancellationToken);
        Assert.True(await cache.ContainsAsync("k", DefaultCancellationToken));
        Assert.NotNull(await cache.GetValueAsync("k", DefaultCancellationToken));

        clock.Advance(TimeSpan.FromMilliseconds(sleepMs));
        Assert.False(await cache.ContainsAsync("k", DefaultCancellationToken));
        Assert.Null(await cache.GetValueAsync("k", DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies whether an entry should exist or be expired after a fixed delay,
    /// based on expiration and/or absolute expiration configuration.
    /// </summary>
    /// <param name="expirationMs">expiration in milliseconds (nullable).</param>
    /// <param name="expiresMs">Absolute expiration in milliseconds relative to now (nullable).</param>
    /// <param name="shouldStillExist">Expected presence of the entry after the delay.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Theory]
    [InlineData(null, null, true)]
    [InlineData(50, null, false)]
    [InlineData(null, 50, false)]
    public async Task PresenceAfterDelaySyncTheoryTest(int? expirationMs, int? expiresMs, bool shouldStillExist)
    {
        var clock = new FakeClock(DateTime.UtcNow);
        await using var cache = new PhysicalCache<string>(clock);

        var entry = new CacheEntry<string>
        {
            Value = "v",
            Expiration = expirationMs.HasValue ? TimeSpan.FromMilliseconds(expirationMs.Value) : null,
            ExpiresUtc = expiresMs.HasValue ? clock.UtcNow.AddMilliseconds(expiresMs.Value) : null,
        };

        await cache.InsertAsync("k", entry, DefaultCancellationToken);
        clock.Advance(TimeSpan.FromMilliseconds(60));

        var exists = await cache.ContainsAsync("k", DefaultCancellationToken);
        Assert.Equal(shouldStillExist, exists);
    }

    /// <summary>
    /// Verifies remove operations treat expired keys as missing.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RemoveOnExpiredKeyReturnsFalse()
    {
        var clock = new FakeClock(DateTime.UtcNow);
        await using var cache = new PhysicalCache<string>(clock);

        await cache.InsertAsync("k", new CacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMilliseconds(10) }, DefaultCancellationToken);

        clock.Advance(TimeSpan.FromMilliseconds(25));

        Assert.False(await cache.RemoveAsync("k", DefaultCancellationToken));
        Assert.False((await cache.TryRemoveAsync("k", DefaultCancellationToken)).Removed);
    }
}
