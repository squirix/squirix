using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Add/Set/Get expiration semantics.</summary>
[Immutable]
public sealed class ExpirationAddSetTests : ClockTestBase
{
    /// <summary>Verifies AddAsync treats an expired key as absent and inserts a new value.</summary>
    [Fact]
    public async Task AddAsyncTreatsExpiredKeyAsAbsent()
    {
        var cache = await Client.GetCacheAsync<string>("add-expired-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        await cache.AddAsync("k", "new", cancellationToken: DefaultCancellationToken);
        Assert.Equal("new", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies AddAsync with immediate expiration reports success but does not leave a live key.</summary>
    [Fact]
    public async Task AddImmediateExpiryNeverLeavesLiveKey()
    {
        var cache = await Client.GetCacheAsync<string>("add-immediate-expiration-public-extra", DefaultCancellationToken);
        await cache.AddAsync("k", "v", Expiry.In(TimeSpan.Zero), DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetExpirationAsync returns remaining expiration for expiring entries and null for persistent or missing ones.</summary>
    [Fact]
    public async Task GetExpirationAsyncReturnsRemainingOrNull()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-get-expiration-async", DefaultCancellationToken);

        // Missing -> null
        Assert.False((await cache.GetExpirationAsync("missing", DefaultCancellationToken)).HasExpiration);

        // Insert with expiration and check remaining is > 0 and <= original
        var expiration = TimeSpan.FromMilliseconds(1500);
        await cache.SetAsync("k1", "v", Expiry.In(expiration), DefaultCancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(remaining1.Found, $"DIAG clock={Clock.GetUtcNow():o}");
        Assert.True(remaining1.HasExpiration);
        Assert.True(remaining1.Value > TimeSpan.Zero);
        Assert.True(remaining1.Value <= expiration);

        // Wait until expiry -> null
        Clock.Advance(TimeSpan.FromMilliseconds(2500));
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
        var expiration = TimeSpan.FromMilliseconds(1500);
        await cache.SetAsync("k1", "v", Expiry.In(expiration), DefaultCancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", DefaultCancellationToken);
        Assert.True(remaining1.Found);
        Assert.True(remaining1.HasExpiration);
        Assert.True(remaining1.Value > TimeSpan.Zero);
        Assert.True(remaining1.Value <= expiration);
        Clock.Advance(TimeSpan.FromMilliseconds(2500));
        Assert.False((await cache.GetExpirationAsync("k1", DefaultCancellationToken)).HasExpiration);
        await cache.SetAsync("k2", "v2", cancellationToken: DefaultCancellationToken);
        Assert.False((await cache.GetExpirationAsync("k2", DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies GetValueAsync reflects presence and expiration.</summary>
    [Fact]
    public async Task GetValueAsyncReflectsPresenceAndExpiry()
    {
        var cache = await Client.GetCacheAsync<string>("contains-async", DefaultCancellationToken);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);

        // Generous expiration so expiry does not overtake the immediate read on a loaded CI runner.
        var expiration = TimeSpan.FromSeconds(2);
        await cache.SetAsync("k1", "v", Expiry.In(expiration), DefaultCancellationToken);
        Assert.True((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetValue returns value on hit and null after expiration.</summary>
    [Fact]
    public async Task GetValueHonorsPresenceAndExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("get-value", DefaultCancellationToken);
        await cache.SetAsync("k1", "v1", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
        Clock.Advance(TimeSpan.FromMilliseconds(2000));
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found, $"DIAG clock={Clock.GetUtcNow():o}");
    }

    /// <summary>Verifies value-based SetAsync applies relative expiration options to the stored entry.</summary>
    [Fact]
    public async Task SetAsyncOptionsApplyRelativeExpiry()
    {
        var cache = await Client.GetCacheAsync<string>("set-options-expiration-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Found, $"DIAG clock={Clock.GetUtcNow():o}");
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Expiration > TimeSpan.Zero);
        Assert.True(expiration.Expiration <= TimeSpan.FromMilliseconds(500));
        Clock.Advance(TimeSpan.FromMilliseconds(2000));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies value-based SetAsync does not drop expiration when overwriting an existing expiring entry.</summary>
    [Fact]
    public async Task SetAsyncValueDropReplacesExpiringEntry()
    {
        var cache = await Client.GetCacheAsync<string>("expiration-insert-value-overwrite-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "v1", Expiry.In(TimeSpan.FromSeconds(10)), DefaultCancellationToken);
        await cache.SetAsync("k", "v2", cancellationToken: DefaultCancellationToken);
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Value > TimeSpan.Zero);
    }

    /// <summary>Verifies TryAddAsync treats an expired key as absent and inserts a new value.</summary>
    [Fact]
    public async Task TryAddAsyncTreatsExpiredKeyAsAbsent()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-expired-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.True(await cache.TryAddAsync("k", "new", cancellationToken: DefaultCancellationToken));
        Assert.Equal("new", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TryAddAsync with immediate expiration returns true but does not leave a live key.</summary>
    [Fact]
    public async Task TryAddImmediateExpiryNeverLeavesLiveKey()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-immediate-expiration-public-extra", DefaultCancellationToken);
        var added = await cache.TryAddAsync("k", "v", Expiry.In(TimeSpan.Zero), DefaultCancellationToken);
        Assert.True(added);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies value-based TryAddAsync applies absolute expiration options to the stored entry.</summary>
    [Fact]
    public async Task TryAddOptionsApplyAbsoluteExpiry()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-options-expires-at-public-extra", DefaultCancellationToken);

        // Absolute deadline is anchored to the controllable fake clock (not real time), so the
        // server-side expiry check stays deterministic regardless of when the test runs.
        var expiresAt = Clock.GetUtcNow().AddMinutes(2);
        var added = await cache.TryAddAsync("k", "v", Expiry.At(expiresAt), DefaultCancellationToken);
        Assert.True(added);
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Expiration > TimeSpan.Zero);
        Assert.True(expiration.Expiration <= expiresAt - Clock.GetUtcNow() + TimeSpan.FromSeconds(5));

        // Prove relative expiry is honored without sleeping for the long ExpiresAt window used above,
        // then cross k's absolute deadline to prove absolute expiry is honored too.
        await cache.SetAsync("k-short", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(2000));
        Assert.False((await cache.GetValueAsync("k-short", DefaultCancellationToken)).Found);
        Clock.Advance(expiresAt - Clock.GetUtcNow() + TimeSpan.FromSeconds(1));
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }
}
