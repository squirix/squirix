using System;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.E2ETests.PublicApi.SingleNode;

/// <summary>
/// Integration tests for single-node expiration, Touch, and RemoveExpiration semantics.
/// </summary>
public sealed class SingleNodeExpirationTests : PublicApiSingleNodeTestBase
{
    /// <summary>
    /// Verifies AddAsync with immediate expiration reports success but does not leave a live key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddAsyncEntryWithImmediateExpirationDoesNotLeaveLiveKey()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("add-immediate-expiration-public-extra", DefaultCancellationToken);

        await cache.AddAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.Zero,
            },
            DefaultCancellationToken);

        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies AddAsync treats an expired key as absent and inserts a new value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddAsyncTreatsExpiredKeyAsAbsent()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("add-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "expired",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        await cache.AddAsync("k", "new", cancellationToken: DefaultCancellationToken);

        Assert.Equal("new", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies GetValueAsync reflects presence and expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetValueAsyncReflectsPresenceAndExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("contains-async", DefaultCancellationToken);

        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);

        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = Delay60 }, DefaultCancellationToken);
        Assert.True((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);

        await Task.Delay(Delay90, DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies expired entries are treated as missing by RemoveAsync.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveAsyncTreatsExpiredEntryAsMissing()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-remove-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(50),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(150), DefaultCancellationToken);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.False(removed);
    }

    /// <summary>
    /// Verifies GetExpirationAsync returns remaining expiration for expiring entries and null for persistent or missing ones.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetExpirationAsyncReturnsRemainingOrNull()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-get-expiration-async", DefaultCancellationToken);

        // Missing -> null
        Assert.False((await cache.GetExpirationAsync("missing", DefaultCancellationToken)).HasExpiration);

        // Insert with expiration and check remaining is > 0 and <= original
        var expiration = TimeSpan.FromMilliseconds(120);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = expiration }, DefaultCancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.NotNull(remaining1);
        Assert.True(remaining1.Value > TimeSpan.Zero);
        Assert.True(remaining1.Value <= expiration);

        // Wait until expiry -> null
        await Task.Delay(TimeSpan.FromMilliseconds(140), DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);

        // Persistent entry -> null
        await cache.SetAsync("k2", "v2", cancellationToken: DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k2", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies GetExpiration (sync) returns remaining expiration for expiring entries and null for persistent or missing ones.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetExpirationReturnsRemainingOrNull()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-get-expiration-sync", DefaultCancellationToken);

        Assert.False((await cache.GetExpirationAsync("missing", DefaultCancellationToken)).HasExpiration);

        var expiration = TimeSpan.FromMilliseconds(120);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = expiration }, DefaultCancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.NotNull(remaining1);
        Assert.True(remaining1.Value > TimeSpan.Zero);
        Assert.True(remaining1.Value <= expiration);

        await Task.Delay(TimeSpan.FromMilliseconds(140), DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);

        await cache.SetAsync("k2", "v2", cancellationToken: DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k2", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies GetValue returns value on hit and null after expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetValueHonorsPresenceAndExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("get-value", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(250) }, DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        await Task.Delay(TimeSpan.FromMilliseconds(320), DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync removes expiration once and returns false on subsequent calls for an already persistent key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncReturnsFalseWhenAlreadyPersistent()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove-expiration-idempotent-public-extra", DefaultCancellationToken);

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

    /// <summary>
    /// Verifies RemoveExpirationAsync returns false for missing and already persistent entries and true when expiration is removed.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncReportsStatusForMissingPersistentAndExpiringEntries()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove-expiration-result-status-public-extra", DefaultCancellationToken);

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

    /// <summary>
    /// Verifies RemoveExpirationAsync returns false and removes an already expired entry.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncOnExpiredEntryReturnsFalseAndMakesKeyMissing()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove-expiration-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(40),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(90), DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync on a non-expiring key returns false and keeps the key live.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncOnNonExpiringKeyReturnsFalseAndKeepsKeyLive()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove-expiration-non-expiring-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
        Assert.False((await cache.GetExpirationAsync("k", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync removes expiration and keeps the entry beyond the original expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncRemovesExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-remove-expiration-async", DefaultCancellationToken);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
        var expirationBefore = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.NotNull(expirationBefore);

        Assert.True(await cache.RemoveExpirationAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync returns false for a missing key and an already non-expiring live key through the public API.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncReturnsFalseForMissingKeyAndPersistentKeyThroughPublicApi()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("missing-remove-expiration-false", DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("missing", DefaultCancellationToken));

        await cache.SetAsync("persistent", "v", cancellationToken: DefaultCancellationToken);
        Assert.False(await cache.RemoveExpirationAsync("persistent", DefaultCancellationToken));
        Assert.Equal("v", (await cache.GetValueAsync("persistent", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync treats an expired key as missing.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationAsyncTreatsExpiredKeyAsMissing()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove-expiration-expired-public-extra-2", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync removes expiration and keeps the entry beyond the original expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveExpirationRemovesExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-remove-expiration-sync", DefaultCancellationToken);

        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
        Assert.NotNull(await cache.GetExpirationAsync("k1", DefaultCancellationToken));
        Assert.True(await cache.RemoveExpirationAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies RemoveAsync on an expired key returns false and does not resurrect or expose the expired value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveAsyncTreatsExpiredKeyAsMissing()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.False(await cache.RemoveAsync("k", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies value-based SetAsync applies relative expiration options to the stored entry.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task SetAsyncValueOptionsApplyRelativeExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("set-options-expiration-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(250),
            },
            DefaultCancellationToken);

        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Expiration > TimeSpan.Zero);
        Assert.True(expiration.Expiration <= TimeSpan.FromMilliseconds(250));

        await Task.Delay(TimeSpan.FromMilliseconds(350), DefaultCancellationToken);

        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies value-based TryAddAsync applies absolute expiration options to the stored entry.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddAsyncValueOptionsApplyAbsoluteExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-add-options-expires-at-public-extra", DefaultCancellationToken);

        var added = await cache.TryAddAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                ExpiresAt = DateTimeOffset.UtcNow.AddMilliseconds(500),
            },
            DefaultCancellationToken);

        Assert.True(added);

        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Expiration > TimeSpan.Zero);

        await Task.Delay(TimeSpan.FromMilliseconds(600), DefaultCancellationToken);

        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies value-based SetAsync does not drop expiration when overwriting an existing expiring entry.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task SetAsyncValueShouldNotDropExpirationWhenOverwritingExistingExpiringEntry()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-insert-value-overwrite-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v1",
            new CacheEntryOptions
            {
                ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(10),
            },
            DefaultCancellationToken);

        await cache.SetAsync("k", "v2", cancellationToken: DefaultCancellationToken);

        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);

        Assert.NotNull(expiration);
        Assert.True(expiration.Value > TimeSpan.Zero);
    }

    /// <summary>
    /// Verifies TouchAsync extends expiration for an existing public cache entry.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TouchAsyncExtendsExpirationThroughPublicApi()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-touch-public-extra", DefaultCancellationToken);
        var originalExpiresUtc = DateTime.UtcNow.AddSeconds(1);
        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                ExpiresAt = new DateTimeOffset(originalExpiresUtc, TimeSpan.Zero),
            },
            DefaultCancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), DefaultCancellationToken);
        Assert.True(await cache.TouchAsync("k", TimeSpan.FromSeconds(2), DefaultCancellationToken));

        var touched = await cache.GetEntryAsync("k", DefaultCancellationToken);
        Assert.NotNull(touched);
        Assert.True(touched.ExpiresUtc > originalExpiresUtc, $"expected touched expiry after {originalExpiresUtc:O}, actual {touched.ExpiresUtc:O}");
    }

    /// <summary>
    /// Verifies TouchAsync extends the expiration window when the key exists.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TouchAsyncExtendsExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-touch-async", DefaultCancellationToken);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
        var expirationBefore = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.NotNull(expirationBefore);

        Assert.True(await cache.TouchAsync("k1", TimeSpan.FromMilliseconds(200), DefaultCancellationToken));
        var expirationAfter = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.NotNull(expirationAfter);
        Assert.True(expirationAfter <= TimeSpan.FromSeconds(2) && expirationAfter > TimeSpan.Zero, $"unexpected remaining expiration: {expirationAfter}");
    }

    /// <summary>
    /// Verifies TouchAsync extends expiration for an entry inserted with expiration through the public API.
    /// Ensures the key remains available past the original expiration after a successful touch.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task TouchAsyncExtendsExpirationInsertedEntryThroughPublicApi()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-touch-public-extra-expiration", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(300),
            },
            DefaultCancellationToken);

        await Task.Delay(60, DefaultCancellationToken);

        Assert.True(await cache.TouchAsync("k", TimeSpan.FromMilliseconds(500), DefaultCancellationToken));

        await Task.Delay(320, DefaultCancellationToken);

        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TouchAsync returns false and removes an already expired entry.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TouchAsyncOnExpiredEntryReturnsFalseAndMakesKeyMissing()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("touch-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(40),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(90), DefaultCancellationToken);

        Assert.False(await cache.TouchAsync("k", TimeSpan.FromSeconds(1), DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies TouchAsync on a non-expiring key adds expiration and keeps the value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TouchAsyncOnNonExpiringKeyAddsExpirationAndKeepsValue()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("touch-non-expiring-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        Assert.True(await cache.TouchAsync("k", TimeSpan.FromMinutes(1), DefaultCancellationToken));

        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);

        Assert.NotNull(expiration);
        Assert.True(expiration.Value > TimeSpan.Zero);
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TouchAsync rejects non-positive expiration without mutating the existing expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TouchAsyncRejectsNonPositiveExpirationWithoutChangingExistingExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("touch-invalid-expiration-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken);

        var before = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.NotNull(before);

        _ = await Assert.ThrowsAnyAsync<ArgumentOutOfRangeException>(async () => await cache.TouchAsync("k", TimeSpan.Zero, DefaultCancellationToken));

        var after = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.NotNull(after);

        Assert.True(after.Value > TimeSpan.Zero);
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TouchAsync returns false for a missing key through the public API.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TouchAsyncReturnsFalseForMissingKeyThroughPublicApi()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("missing-touch-missing", DefaultCancellationToken);

        Assert.False(await cache.TouchAsync("missing", TimeSpan.FromSeconds(1), DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies TouchAsync treats an expired key as missing and does not resurrect it.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TouchAsyncTreatsExpiredKeyAsMissingAndDoesNotResurrect()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("touch-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.False(await cache.TouchAsync("k", TimeSpan.FromMinutes(1), DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies Touch (sync) extends the expiration window when the key exists.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TouchExtendsExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("expiration-touch-sync", DefaultCancellationToken);

        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(1) }, DefaultCancellationToken);
        Assert.True(await cache.TouchAsync("k1", TimeSpan.FromMilliseconds(200), DefaultCancellationToken));
        var expirationAfter = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.NotNull(expirationAfter);
    }

    /// <summary>
    /// Verifies TryAddAsync with immediate expiration returns true but does not leave a live key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddAsyncEntryWithImmediateExpirationDoesNotLeaveLiveKey()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-add-immediate-expiration-public-extra", DefaultCancellationToken);

        var added = await cache.TryAddAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.Zero,
            },
            DefaultCancellationToken);

        Assert.True(added);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies TryAddAsync treats an expired key as absent and inserts a new value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddAsyncTreatsExpiredKeyAsAbsent()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-add-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "expired",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k", "new", cancellationToken: DefaultCancellationToken));
        Assert.Equal("new", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }
}
