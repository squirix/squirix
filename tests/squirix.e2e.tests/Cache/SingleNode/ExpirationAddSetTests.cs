using System;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Add/Set/Get expiration semantics.</summary>
public sealed class ExpirationAddSetTests : TestBase
{
    /// <summary>Initializes a new instance of the <see cref="ExpirationAddSetTests" /> class.</summary>
    /// <param name="fixture">Shared single-node cluster fixture.</param>
    public ExpirationAddSetTests(SingleNodeFixture fixture)
        : base(fixture)
    {
    }

    /// <summary>Verifies AddAsync with immediate expiration reports success but does not leave a live key.</summary>
    [Fact]
    public async Task AddAsyncEntryImmediateExpirationNotLeaveLiveKey()
    {
        var cache = await Client.GetCacheAsync<string>("add-immediate-expiration-public-extra", DefaultCancellationToken);

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

    /// <summary>Verifies AddAsync treats an expired key as absent and inserts a new value.</summary>
    [Fact]
    public async Task AddAsyncTreatsExpiredKeyAsAbsent()
    {
        var cache = await Client.GetCacheAsync<string>("add-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "expired",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), TimeProvider.System, DefaultCancellationToken);

        await cache.AddAsync("k", "new", cancellationToken: DefaultCancellationToken);

        Assert.Equal("new", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies GetExpirationAsync returns remaining expiration for expiring entries and null for persistent or missing ones.</summary>
    [Fact]
    public async Task GetExpirationAsyncReturnsRemainingOrNull()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-get-expiration-async", DefaultCancellationToken);

        // Missing -> null
        Assert.False((await cache.GetExpirationAsync("missing", DefaultCancellationToken)).HasExpiration);

        // Insert with expiration and check remaining is > 0 and <= original
        var expiration = TimeSpan.FromMilliseconds(120);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = expiration }, DefaultCancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(remaining1.Found);
        Assert.True(remaining1.HasExpiration);
        Assert.True(remaining1.Value > TimeSpan.Zero);
        Assert.True(remaining1.Value <= expiration);

        // Wait until expiry -> null
        await Task.Delay(TimeSpan.FromMilliseconds(140), TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);

        // Persistent entry -> null
        await cache.SetAsync("k2", "v2", cancellationToken: DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k2", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies GetExpiration (sync) returns remaining expiration for expiring entries and null for persistent or missing ones.</summary>
    [Fact]
    public async Task GetExpirationReturnsRemainingOrNull()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-get-expiration-sync", DefaultCancellationToken);

        Assert.False((await cache.GetExpirationAsync("missing", DefaultCancellationToken)).HasExpiration);

        var expiration = TimeSpan.FromMilliseconds(120);
        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = expiration }, DefaultCancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(remaining1.Found);
        Assert.True(remaining1.HasExpiration);
        Assert.True(remaining1.Value > TimeSpan.Zero);
        Assert.True(remaining1.Value <= expiration);

        await Task.Delay(TimeSpan.FromMilliseconds(140), TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);

        await cache.SetAsync("k2", "v2", cancellationToken: DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k2", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies GetValueAsync reflects presence and expiration.</summary>
    [Fact]
    public async Task GetValueAsyncReflectsPresenceAndExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("contains-async", DefaultCancellationToken);

        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);

        await cache.SetAsync("k1", "v", new CacheEntryOptions { Expiration = Delay60 }, DefaultCancellationToken);
        Assert.True((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);

        await Task.Delay(Delay90, TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetValue returns value on hit and null after expiration.</summary>
    [Fact]
    public async Task GetValueHonorsPresenceAndExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("get-value", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(250) }, DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        await Task.Delay(TimeSpan.FromMilliseconds(320), TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies value-based SetAsync does not drop expiration when overwriting an existing expiring entry.</summary>
    [Fact]
    public async Task SetAsyncValueDropOverwritingExistingExpiringEntry()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-insert-value-overwrite-public-extra", DefaultCancellationToken);

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

        Assert.True(expiration.Value > TimeSpan.Zero);
    }

    /// <summary>Verifies value-based SetAsync applies relative expiration options to the stored entry.</summary>
    [Fact]
    public async Task SetAsyncValueOptionsApplyRelativeExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("set-options-expiration-public-extra", DefaultCancellationToken);

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

        await Task.Delay(TimeSpan.FromMilliseconds(350), TimeProvider.System, DefaultCancellationToken);

        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies TryAddAsync with immediate expiration returns true but does not leave a live key.</summary>
    [Fact]
    public async Task TryAddAsyncEntryImmediateExpirationNotLeaveLiveKey()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-immediate-expiration-public-extra", DefaultCancellationToken);

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

    /// <summary>Verifies TryAddAsync treats an expired key as absent and inserts a new value.</summary>
    [Fact]
    public async Task TryAddAsyncTreatsExpiredKeyAsAbsent()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-expired-public-extra", DefaultCancellationToken);

        await cache.SetAsync(
            "k",
            "expired",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), TimeProvider.System, DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k", "new", cancellationToken: DefaultCancellationToken));
        Assert.Equal("new", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies value-based TryAddAsync applies absolute expiration options to the stored entry.</summary>
    [Fact]
    public async Task TryAddAsyncValueOptionsApplyAbsoluteExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-options-expires-at-public-extra", DefaultCancellationToken);

        // Absolute deadline is captured on the client before the round-trip; keep it far enough out that
        // full-suite scheduling delay cannot make the entry look already expired on GetExpiration.
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(2);
        var added = await cache.TryAddAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                ExpiresAt = expiresAt,
            },
            DefaultCancellationToken);

        Assert.True(added);

        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Expiration > TimeSpan.Zero);
        Assert.True(expiration.Expiration <= expiresAt - DateTimeOffset.UtcNow + TimeSpan.FromSeconds(5));

        // Prove absolute expiry is honored without sleeping for the long ExpiresAt window used above.
        await cache.SetAsync("k-short", "v", new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(250) }, DefaultCancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(400), TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k-short", DefaultCancellationToken)).Found);
    }
}
