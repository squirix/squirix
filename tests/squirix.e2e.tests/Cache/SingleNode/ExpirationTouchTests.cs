using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Touch expiration semantics.</summary>
[Immutable]
public sealed class ExpirationTouchTests : TestBase
{
    /// <summary>Initializes a new instance of the <see cref="ExpirationTouchTests" /> class.</summary>
    /// <param name="fixture">Shared single-node cluster fixture.</param>
    public ExpirationTouchTests(SingleNodeFixture fixture)
        : base(fixture)
    {
    }

    /// <summary>Verifies TouchAsync returns false and removes an already expired entry.</summary>
    [Fact]
    public async Task TouchAsyncExpiredEntryReturnsFalseMakesKeyMissing()
    {
        var cache = await Client.GetCacheAsync<string>("touch-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(40),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(90), TimeProvider.System, DefaultCancellationToken);

        Assert.False(await cache.TouchAsync("k", TimeSpan.FromSeconds(1), DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies TouchAsync extends the expiration window when the key exists.</summary>
    [Fact]
    public async Task TouchAsyncExtendsExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-async", DefaultCancellationToken);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
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
    /// Verifies TouchAsync extends expiration for an entry inserted with expiration through the public API.
    /// Ensures the key remains available past the original expiration after a successful touch.
    /// </summary>
    [Fact]
    public async Task TouchAsyncExtendsExpirationInsertedEntryPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-public-extra-expiration", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(300),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(60), TimeProvider.System, DefaultCancellationToken);

        Assert.True(await cache.TouchAsync("k", TimeSpan.FromMilliseconds(500), DefaultCancellationToken));

        await Task.Delay(TimeSpan.FromMilliseconds(320), TimeProvider.System, DefaultCancellationToken);

        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TouchAsync refreshes the expiration of an entry inserted with an absolute ExpiresAt through the public API.
    /// The absolute deadline is set safely in the future so the key is still live when the touch runs, even under full-suite load
    /// where the SetAsync round-trip can be slow.
    /// </summary>
    [Fact]
    public async Task TouchAsyncExtendsExpirationThroughPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-public-extra", DefaultCancellationToken);

        // Absolute deadline well beyond the SetAsync round-trip so the key cannot expire before the touch,
        // even when the full e2e suite runs concurrently and RPCs are slow.
        var originalExpiresUtc = DateTime.UtcNow.AddSeconds(30);
        var cacheEntryOptions = new CacheEntryOptions
        {
            ExpiresAt = new DateTimeOffset(originalExpiresUtc, TimeSpan.Zero),
        };
        await cache.SetAsync("k", "v", cacheEntryOptions, DefaultCancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, DefaultCancellationToken);
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

    /// <summary>Verifies TouchAsync on a non-expiring key adds expiration and keeps the value.</summary>
    [Fact]
    public async Task TouchAsyncOnNonExpiringKeyAddsExpirationKeepsValue()
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

    /// <summary>Verifies TouchAsync rejects non-positive expiration without mutating the existing expiration.</summary>
    [Fact]
    public async Task TouchAsyncRejectsNonChangingExistingExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("touch-invalid-expiration-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken);

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
    public async Task TouchAsyncReturnsFalseForMissingKeyPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("missing-touch-missing", DefaultCancellationToken);

        Assert.False(await cache.TouchAsync("missing", TimeSpan.FromSeconds(1), DefaultCancellationToken));
    }

    /// <summary>Verifies TouchAsync treats an expired key as missing and does not resurrect it.</summary>
    [Fact]
    public async Task TouchAsyncTreatsExpiredKeyAsMissingAndNotResurrect()
    {
        var cache = await Client.GetCacheAsync<string>("touch-expired-resurrect-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), TimeProvider.System, DefaultCancellationToken);

        Assert.False(await cache.TouchAsync("k", TimeSpan.FromMinutes(1), DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies Touch (sync) extends the expiration window when the key exists.</summary>
    [Fact]
    public async Task TouchExtendsExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-sync", DefaultCancellationToken);

        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
        Assert.True(await cache.TouchAsync("k1", TimeSpan.FromMilliseconds(200), DefaultCancellationToken));
        var expirationAfter = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(expirationAfter.Found);
        Assert.True(expirationAfter.HasExpiration);
        Assert.True(expirationAfter.Value > TimeSpan.Zero);
        Assert.Equal("v", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }
}
