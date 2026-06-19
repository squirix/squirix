using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>
/// Unit tests for <see cref="PhysicalCache{T}" /> expiration and expiration handling.
/// Verifies both relative expiration (<see cref="CacheEntry{T}.Expiration" />) and absolute
/// expiration (<see cref="CacheEntry{T}.ExpiresUtc" />).
/// </summary>
public sealed class CacheExpirationTests : UnitTestBase
{
    /// <summary>Verifies TryAddAsync treats an expired existing entry as absent and inserts a new value.</summary>
    [Fact]
    public async Task TryAddShouldSucceedWhenExistingEntryExpired()
    {
        var timeProvider = new FakeTimeProvider();
        await using var cache = new PhysicalCache<string>(timeProvider);

        Assert.True(await cache.TryAddAsync("k", new CacheEntry<string> { Value = "expired", Expiration = TimeSpan.FromMilliseconds(10) }, DefaultCancellationToken));

        timeProvider.Advance(TimeSpan.FromMilliseconds(25));

        Assert.True(await cache.TryAddAsync("k", new CacheEntry<string> { Value = "new" }, DefaultCancellationToken));
        Assert.Equal("new", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Ensures entries expire correctly when inserted with either relative expiration or absolute expiration.</summary>
    /// <param name="expirationMs">expiration in milliseconds when using relative expiration.</param>
    /// <param name="sleepMs">Delay before checking presence in milliseconds.</param>
    /// <param name="useAbsoluteExpires">If <see langword="true"/>, uses <see cref="CacheEntry{T}.ExpiresUtc" />; otherwise <see cref="CacheEntry{T}.Expiration" />.</param>
    [Theory]
    [InlineData(10, 25, true)]
    [InlineData(10, 25, false)]
    [InlineData(25, 60, true)]
    [InlineData(25, 60, false)]
    public async Task ExpirationSyncTheoryTest(int expirationMs, int sleepMs, bool useAbsoluteExpires)
    {
        var timeProvider = new FakeTimeProvider();
        await using var cache = new PhysicalCache<string>(timeProvider);

        var entry = useAbsoluteExpires ? new CacheEntry<string> { Value = "v", ExpiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddMilliseconds(expirationMs) }
            : new CacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMilliseconds(expirationMs) };

        await cache.SetAsync("k", entry, DefaultCancellationToken);
        Assert.True((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);

        timeProvider.Advance(TimeSpan.FromMilliseconds(sleepMs));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies whether an entry should exist or be expired after a fixed delay,
    /// based on expiration and/or absolute expiration configuration.
    /// </summary>
    /// <param name="expirationMs">expiration in milliseconds (nullable).</param>
    /// <param name="expiresMs">Absolute expiration in milliseconds relative to now (nullable).</param>
    /// <param name="shouldStillExist">Expected presence of the entry after the delay.</param>
    [Theory]
    [InlineData(null, null, true)]
    [InlineData(50, null, false)]
    [InlineData(null, 50, false)]
    public async Task PresenceAfterDelaySyncTheoryTest(int? expirationMs, int? expiresMs, bool shouldStillExist)
    {
        var timeProvider = new FakeTimeProvider();
        await using var cache = new PhysicalCache<string>(timeProvider);

        var entry = new CacheEntry<string>
        {
            Value = "v",
            Expiration = expirationMs is not null ? TimeSpan.FromMilliseconds(expirationMs.Value) : null,
            ExpiresUtc = expiresMs is not null ? timeProvider.GetUtcNow().UtcDateTime.AddMilliseconds(expiresMs.Value) : null,
        };

        await cache.SetAsync("k", entry, DefaultCancellationToken);
        timeProvider.Advance(TimeSpan.FromMilliseconds(60));

        var exists = (await cache.GetValueAsync("k", DefaultCancellationToken)).Found;
        Assert.Equal(shouldStillExist, exists);
    }

    /// <summary>Verifies remove operations treat expired keys as missing.</summary>
    [Fact]
    public async Task RemoveExpiredKeyReturnsFalse()
    {
        var timeProvider = new FakeTimeProvider();
        await using var cache = new PhysicalCache<string>(timeProvider);

        await cache.SetAsync("k", new CacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMilliseconds(10) }, DefaultCancellationToken);

        timeProvider.Advance(TimeSpan.FromMilliseconds(25));

        Assert.False((await cache.RemoveAsync("k", DefaultCancellationToken)).Removed);
        Assert.False((await cache.RemoveAsync("k", DefaultCancellationToken)).Removed);
    }

    /// <summary>Verifies TryAddAsync stores absolute expiration metadata that GetEntryAsync can read back.</summary>
    [Fact]
    public async Task TryAddAsyncPreservesAbsoluteExpiration()
    {
        var timeProvider = new FakeTimeProvider();
        await using var cache = new PhysicalCache<string>(timeProvider);

        var expiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddSeconds(5);
        var added = await cache.TryAddAsync("k", new CacheEntry<string> { Value = "v", ExpiresUtc = expiresUtc }, DefaultCancellationToken);

        Assert.True(added);

        var stored = await cache.GetEntryAsync("k", DefaultCancellationToken);
        Assert.NotNull(stored);
        Assert.Equal(expiresUtc, stored.ExpiresUtc);
        var remaining = stored.ExpiresUtc!.Value - timeProvider.GetUtcNow().UtcDateTime;
        Assert.True(remaining > TimeSpan.Zero);
        Assert.True(remaining <= TimeSpan.FromSeconds(5));
    }
}
