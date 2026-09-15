using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Add/Set/Get expiration semantics.</summary>
[Immutable]
public sealed class ExpirationAddSetTests : ClockTestBase
{
    /// <summary>Verifies value-based TryAddAsync applies absolute expiration options to the stored entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddAppliesAbsoluteExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-add-options-expires-at-public-extra", cancellationToken);

        // Absolute deadline is anchored to the controllable fake clock (not real time), so the
        // server-side expiry check stays deterministic regardless of when the test runs.
        var expiresAt = Clock.GetUtcNow().AddMinutes(2);
        var added = await cache.TryAddAsync("k", "v", Expiry.At(expiresAt), cancellationToken);
        _ = await Assert.That(added).IsTrue();
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        _ = await Assert.That(expiration.Expiration > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(expiration.Expiration <= expiresAt - Clock.GetUtcNow() + TimeSpan.FromSeconds(5)).IsTrue();

        // Prove relative expiry is honored without sleeping for the long ExpiresAt window used above,
        // then cross k's absolute deadline to prove absolute expiry is honored too.
        await cache.SetAsync("k-short", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(2000));
        _ = await Assert.That((await cache.GetValueAsync("k-short", cancellationToken)).Found).IsFalse();
        Clock.Advance(expiresAt - Clock.GetUtcNow() + TimeSpan.FromSeconds(1));
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies AddAsync treats an expired key as absent and inserts a new value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddAsyncTreatsExpiredKeyAsAbsent(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("add-expired-public-extra", cancellationToken);
        await cache.SetAsync("k", "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        await cache.AddAsync("k", "new", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("new");
    }

    /// <summary>Verifies TryAddAsync with immediate expiration returns true but does not leave a live key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddImmediateExpiryLeavesNoKey(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-add-immediate-expiration-public-extra", cancellationToken);
        var added = await cache.TryAddAsync("k", "v", Expiry.In(TimeSpan.Zero), cancellationToken);
        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies AddAsync with immediate expiration reports success but does not leave a live key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddImmediateExpiryNeverLeavesLiveKey(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("add-immediate-expiration-public-extra", cancellationToken);
        await cache.AddAsync("k", "v", Expiry.In(TimeSpan.Zero), cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies TryAddAsync treats an expired key as absent and inserts a new value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddReplacesExpired(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-add-expired-public-extra", cancellationToken);
        await cache.SetAsync("k", "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await cache.TryAddAsync("k", "new", cancellationToken: cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("new");
    }

    /// <summary>Verifies GetExpirationAsync returns remaining expiration for expiring entries and null for persistent or missing ones.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetExpirationAsyncReturnsRemainingOrNull(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-get-expiration-async", cancellationToken);

        // Missing -> null
        _ = await Assert.That((await cache.GetExpirationAsync("missing", cancellationToken)).HasExpiration).IsFalse();

        // Insert with expiration and check remaining is > 0 and <= original
        var expiration = TimeSpan.FromMilliseconds(1500);
        await cache.SetAsync("k1", "v", Expiry.In(expiration), cancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", cancellationToken);
        _ = await Assert.That(remaining1.Found).IsTrue().Because($"DIAG clock={Clock.GetUtcNow():o}");
        _ = await Assert.That(remaining1.HasExpiration).IsTrue();
        _ = await Assert.That(remaining1.Value > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(remaining1.Value <= expiration).IsTrue();

        // Wait until expiry -> null
        Clock.Advance(TimeSpan.FromMilliseconds(2500));
        _ = await Assert.That((await cache.GetExpirationAsync("k1", cancellationToken)).HasExpiration).IsFalse();

        // Persistent entry -> null
        await cache.SetAsync("k2", "v2", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetExpirationAsync("k2", cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies GetExpiration (sync) returns remaining expiration for expiring entries and null for persistent or missing ones.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetExpirationReturnsRemainingOrNull(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-get-expiration-sync", cancellationToken);
        _ = await Assert.That((await cache.GetExpirationAsync("missing", cancellationToken)).HasExpiration).IsFalse();
        var expiration = TimeSpan.FromMilliseconds(1500);
        await cache.SetAsync("k1", "v", Expiry.In(expiration), cancellationToken);
        var remaining1 = await cache.GetExpirationAsync("k1", cancellationToken);
        _ = await Assert.That(remaining1.Found).IsTrue();
        _ = await Assert.That(remaining1.HasExpiration).IsTrue();
        _ = await Assert.That(remaining1.Value > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(remaining1.Value <= expiration).IsTrue();
        Clock.Advance(TimeSpan.FromMilliseconds(2500));
        _ = await Assert.That((await cache.GetExpirationAsync("k1", cancellationToken)).HasExpiration).IsFalse();
        await cache.SetAsync("k2", "v2", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetExpirationAsync("k2", cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies GetValueAsync reflects presence and expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetValueAsyncReflectsPresenceAndExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("contains-async", cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsFalse();

        // Generous expiration so expiry does not overtake the immediate read on a loaded CI runner.
        var expiration = TimeSpan.FromSeconds(2);
        await cache.SetAsync("k1", "v", Expiry.In(expiration), cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsTrue();
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies GetValue returns value on hit and null after expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetValueHonorsPresenceAndExpiration(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("get-value", cancellationToken);
        await cache.SetAsync("k1", "v1", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
        Clock.Advance(TimeSpan.FromMilliseconds(2000));
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsFalse().Because($"DIAG clock={Clock.GetUtcNow():o}");
    }

    /// <summary>Verifies value-based SetAsync applies relative expiration options to the stored entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAsyncOptionsApplyRelativeExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("set-options-expiration-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Found).IsTrue().Because($"DIAG clock={Clock.GetUtcNow():o}");
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        _ = await Assert.That(expiration.Expiration > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(expiration.Expiration <= TimeSpan.FromMilliseconds(500)).IsTrue();
        Clock.Advance(TimeSpan.FromMilliseconds(2000));
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies value-based SetAsync does not drop expiration when overwriting an existing expiring entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAsyncValueDropReplacesExpiringEntry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-insert-value-overwrite-public-extra", cancellationToken);
        await cache.SetAsync("k", "v1", Expiry.In(TimeSpan.FromSeconds(10)), cancellationToken);
        await cache.SetAsync("k", "v2", cancellationToken: cancellationToken);
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Value > TimeSpan.Zero).IsTrue();
    }
}
