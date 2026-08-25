using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Touch expiration semantics.</summary>
[Immutable]
public sealed class ExpirationTouchTests : ClockTestBase
{
    /// <summary>Verifies TouchAsync on a non-expiring key adds expiration and keeps the value.</summary>
    [Fact]
    public async Task TouchAsyncAddsExpiryToNonExpiringKey()
    {
        var cache = await Client.GetCacheAsync<string>("touch-non-expiring-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);
        Assert.True(await cache.TouchAsync("k", TimeSpan.FromMinutes(1), DefaultCancellationToken));
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Value > TimeSpan.Zero);
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TouchAsync treats an expired key as missing and does not resurrect it.</summary>
    [Fact]
    public async Task TouchAsyncDoesNotResurrectExpiredKeys()
    {
        var cache = await Client.GetCacheAsync<string>("touch-expired-resurrect-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.False(await cache.TouchAsync("k", TimeSpan.FromMinutes(1), DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies TouchAsync returns false and removes an already expired entry.</summary>
    [Fact]
    public async Task TouchAsyncExpiredEntryStaysMissing()
    {
        var cache = await Client.GetCacheAsync<string>("touch-expired-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.False(await cache.TouchAsync("k", TimeSpan.FromSeconds(1), DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies TouchAsync extends the expiration window when the key exists.</summary>
    [Fact]
    public async Task TouchAsyncExtendsExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-async", DefaultCancellationToken);
        await cache.SetAsync("k1", "v", Expiry.In(TimeSpan.FromMinutes(1)), DefaultCancellationToken);
        var expirationBefore = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(expirationBefore.Found);
        Assert.True(expirationBefore.HasExpiration);
        Assert.True(await cache.TouchAsync("k1", TimeSpan.FromMilliseconds(200), DefaultCancellationToken));
        var expirationAfter = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(expirationAfter.Found);
        Assert.True(expirationAfter.HasExpiration);
        Assert.True(expirationAfter <= TimeSpan.FromSeconds(2) && expirationAfter > TimeSpan.Zero);
    }

    /// <summary>
    /// Verifies TouchAsync refreshes the expiration of an entry inserted with an absolute ExpiresAt through the public API.
    /// The absolute deadline is anchored to the injected fake clock so the SetAsync, Advance, and Touch operations
    /// share one time source and the key is still live when the touch runs.
    /// </summary>
    [Fact]
    public async Task TouchAsyncExtendsExpiryThroughPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-public-extra", DefaultCancellationToken);

        // Absolute deadline well beyond the SetAsync round-trip so the key cannot expire before the touch.
        var cacheEntryOptions = new CacheEntryOptions { ExpiresAt = Clock.GetUtcNow().AddSeconds(30) };
        await cache.SetAsync("k", "v", cacheEntryOptions, DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(50));
        Assert.True(await cache.TouchAsync("k", TimeSpan.FromSeconds(2), DefaultCancellationToken));
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration > TimeSpan.Zero);
        Assert.True(expiration <= TimeSpan.FromSeconds(3));
        var value = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(value.Found);
        Assert.Equal("v", value.Value);
    }

    /// <summary>
    /// Verifies TouchAsync extends expiration for an entry inserted with expiration through the public API.
    /// Ensures the key remains available past the original expiration after a successful touch.
    /// </summary>
    [Fact]
    public async Task TouchAsyncExtendsInsertedEntryExpiry()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-public-extra-expiration", DefaultCancellationToken);

        // Deterministic crossing of the original deadline: the original TTL (2s) elapses while the
        // touch-installed fresh 60-second window keeps the entry alive; that window is far beyond any
        // scheduling delay, so neither the touch nor the final read can race an expiry (#412).
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromSeconds(2)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(250));
        Assert.True(await cache.TouchAsync("k", TimeSpan.FromSeconds(60), DefaultCancellationToken));
        Clock.Advance(TimeSpan.FromMilliseconds(2100));
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TouchAsync rejects non-positive expiration without mutating the existing expiration.</summary>
    [Fact]
    public async Task TouchAsyncRejectsUnchangedExistingExpiry()
    {
        var cache = await Client.GetCacheAsync<string>("touch-invalid-expiration-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMinutes(1)), DefaultCancellationToken);
        var before = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(before.Found);
        Assert.True(before.HasExpiration);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<ArgumentOutOfRangeException>(cache.TouchAsync("k", TimeSpan.Zero, DefaultCancellationToken));
        var after = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(after.Found);
        Assert.True(after.HasExpiration);
        Assert.True(after.Value > TimeSpan.Zero);
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TouchAsync returns false for a missing key through the public API.</summary>
    [Fact]
    public async Task TouchAsyncReturnsFalseForMissingKey()
    {
        var cache = await Client.GetCacheAsync<string>("missing-touch-missing", DefaultCancellationToken);
        Assert.False(await cache.TouchAsync("missing", TimeSpan.FromSeconds(1), DefaultCancellationToken));
    }

    /// <summary>Verifies Touch (sync) extends the expiration window when the key exists.</summary>
    [Fact]
    public async Task TouchExtendsExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-sync", DefaultCancellationToken);
        await cache.SetAsync("k1", "v", Expiry.In(TimeSpan.FromMinutes(1)), DefaultCancellationToken);
        Assert.True(await cache.TouchAsync("k1", TimeSpan.FromMilliseconds(200), DefaultCancellationToken));
        var expirationAfter = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(expirationAfter.Found);
        Assert.True(expirationAfter.HasExpiration);
        Assert.True(expirationAfter.Value > TimeSpan.Zero);
        Assert.Equal("v", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }
}
