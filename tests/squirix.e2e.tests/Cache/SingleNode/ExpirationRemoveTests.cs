using System;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Remove and RemoveExpiration semantics.</summary>
public sealed class ExpirationRemoveTests : TestBase
{
    /// <summary>Initializes a new instance of the <see cref="ExpirationRemoveTests"/> class.</summary>
    /// <param name="fixture">Shared single-node cluster fixture.</param>
    public ExpirationRemoveTests(SingleNodeFixture fixture)
        : base(fixture)
    {
    }

    /// <summary>Verifies expired entries are treated as missing by RemoveAsync.</summary>
    [Fact]
    public async Task RemoveAsyncTreatsExpiredEntryAsMissing()
    {
        var cache = await Client.GetCacheAsync<string>("try-remove-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(50),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(150), TimeProvider.System, DefaultCancellationToken);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.False(removed);
    }

    /// <summary>Verifies RemoveAsync on an expired key returns false and does not resurrect or expose the expired value.</summary>
    [Fact]
    public async Task RemoveAsyncTreatsExpiredKeyAsMissing()
    {
        var cache = await Client.GetCacheAsync<string>("remove-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), TimeProvider.System, DefaultCancellationToken);

        Assert.False(await cache.RemoveAsync("k", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveExpirationAsync returns false and removes an already expired entry.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncOnExpiredEntryReturnsFalseAndMakesKeyMissing()
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(40),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(90), TimeProvider.System, DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveExpirationAsync on a non-expiring key returns false and keeps the key live.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncOnNonExpiringKeyReturnsFalseAndKeepsKeyLive()
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-non-expiring-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
        Assert.False((await cache.GetExpirationAsync("k", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration and keeps the entry beyond the original expiration.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncRemovesExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-remove-expiration-async", DefaultCancellationToken);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
        var expirationBefore = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(expirationBefore.Found);
        Assert.True(expirationBefore.HasExpiration);

        Assert.True(await cache.RemoveExpirationAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies RemoveExpirationAsync returns false for missing and already persistent entries and true when expiration is removed.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncReportsStatusForMissingPersistentAndExpiringEntries()
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-result-status-public-extra", DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("missing", DefaultCancellationToken));

        await cache.SetAsync("persistent", "v1", cancellationToken: DefaultCancellationToken);
        Assert.False(await cache.RemoveExpirationAsync("persistent", DefaultCancellationToken));

        await cache.SetAsync(
            "expiring",
            "v2",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken);

        Assert.True(await cache.RemoveExpirationAsync("expiring", DefaultCancellationToken));
        Assert.False((await cache.GetExpirationAsync("expiring", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies RemoveExpirationAsync returns false for a missing key and an already non-expiring live key through the public API.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncReturnsFalseForMissingKeyAndPersistentKeyThroughPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("missing-remove-expiration-false", DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("missing", DefaultCancellationToken));

        await cache.SetAsync("persistent", "v", cancellationToken: DefaultCancellationToken);
        Assert.False(await cache.RemoveExpirationAsync("persistent", DefaultCancellationToken));
        Assert.Equal("v", (await cache.GetValueAsync("persistent", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration once and returns false on subsequent calls for an already persistent key.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncReturnsFalseWhenAlreadyPersistent()
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-idempotent-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken);

        Assert.True(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
        Assert.False((await cache.GetExpirationAsync("k", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies RemoveExpirationAsync treats an expired key as missing.</summary>
    [Fact]
    public async Task RemoveExpirationAsyncTreatsExpiredKeyAsMissing()
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-expired-public-extra-2", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), TimeProvider.System, DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration and keeps the entry beyond the original expiration.</summary>
    [Fact]
    public async Task RemoveExpirationRemovesExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-remove-expiration-sync", DefaultCancellationToken);

        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
        var before = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(before.Found);
        Assert.True(before.HasExpiration);
        Assert.True(await cache.RemoveExpirationAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);
    }
}
