using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Touch expiration semantics.</summary>
[Immutable]
public sealed class ExpirationTouchTests : ClockTestBase
{
    /// <summary>Verifies TouchAsync on a non-expiring key adds expiration and keeps the value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncAddsExpiryToNonExpiringKey(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("touch-non-expiring-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.TouchAsync("k", TimeSpan.FromMinutes(1), cancellationToken)).IsTrue();
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        _ = await Assert.That(expiration.Value > TimeSpan.Zero).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
    }

    /// <summary>Verifies TouchAsync treats an expired key as missing and does not resurrect it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncDoesNotResurrectExpiredKeys(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("touch-expired-resurrect-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await cache.TouchAsync("k", TimeSpan.FromMinutes(1), cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies TouchAsync returns false and removes an already expired entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncExpiredEntryStaysMissing(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("touch-expired-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await cache.TouchAsync("k", TimeSpan.FromSeconds(1), cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies TouchAsync extends the expiration window when the key exists.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncExtendsExpiration(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-async", cancellationToken);
        await cache.SetAsync("k1", "v", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        var expirationBefore = await cache.GetExpirationAsync("k1", cancellationToken);
        _ = await Assert.That(expirationBefore.Found).IsTrue();
        _ = await Assert.That(expirationBefore.HasExpiration).IsTrue();
        _ = await Assert.That(await cache.TouchAsync("k1", TimeSpan.FromMilliseconds(200), cancellationToken)).IsTrue();
        var expirationAfter = await cache.GetExpirationAsync("k1", cancellationToken);
        _ = await Assert.That(expirationAfter.Found).IsTrue();
        _ = await Assert.That(expirationAfter.HasExpiration).IsTrue();
        _ = await Assert.That(expirationAfter <= TimeSpan.FromSeconds(2) && expirationAfter > TimeSpan.Zero).IsTrue();
    }

    /// <summary>
    /// Verifies TouchAsync refreshes the expiration of an entry inserted with an absolute ExpiresAt through the public API.
    /// The absolute deadline is anchored to the injected fake clock so the SetAsync, Advance, and Touch operations
    /// share one time source and the key is still live when the touch runs.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncExtendsExpiryThroughPublicApi(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-public-extra", cancellationToken);

        // Absolute deadline well beyond the SetAsync round-trip so the key cannot expire before the touch.
        var cacheEntryOptions = new CacheEntryOptions { ExpiresAt = Clock.GetUtcNow().AddSeconds(30) };
        await cache.SetAsync("k", "v", cacheEntryOptions, cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(50));
        _ = await Assert.That(await cache.TouchAsync("k", TimeSpan.FromSeconds(2), cancellationToken)).IsTrue();
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        _ = await Assert.That(expiration > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(expiration <= TimeSpan.FromSeconds(3)).IsTrue();
        var value = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value).IsEqualTo("v");
    }

    /// <summary>
    /// Verifies TouchAsync extends expiration for an entry inserted with expiration through the public API.
    /// Ensures the key remains available past the original expiration after a successful touch.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncExtendsInsertedEntryExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-public-extra-expiration", cancellationToken);

        // Deterministic crossing of the original deadline: the original TTL (2s) elapses while the
        // touch-installed fresh 60-second window keeps the entry alive; that window is far beyond any
        // scheduling delay, so neither the touch nor the final read can race an expiry (#412).
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromSeconds(2)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(250));
        _ = await Assert.That(await cache.TouchAsync("k", TimeSpan.FromSeconds(60), cancellationToken)).IsTrue();
        Clock.Advance(TimeSpan.FromMilliseconds(2100));
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
    }

    /// <summary>Verifies TouchAsync rejects non-positive expiration without mutating the existing expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncRejectsUnchangedExistingExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("touch-invalid-expiration-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        var before = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(before.Found).IsTrue();
        _ = await Assert.That(before.HasExpiration).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<ArgumentOutOfRangeException>(cache.TouchAsync("k", TimeSpan.Zero, cancellationToken));
        var after = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(after.Found).IsTrue();
        _ = await Assert.That(after.HasExpiration).IsTrue();
        _ = await Assert.That(after.Value > TimeSpan.Zero).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
    }

    /// <summary>Verifies TouchAsync returns false for a missing key through the public API.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncReturnsFalseForMissingKey(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("missing-touch-missing", cancellationToken);
        _ = await Assert.That(await cache.TouchAsync("missing", TimeSpan.FromSeconds(1), cancellationToken)).IsFalse();
    }

    /// <summary>Verifies Touch (sync) extends the expiration window when the key exists.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchExtendsExpiration(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-touch-sync", cancellationToken);
        await cache.SetAsync("k1", "v", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        _ = await Assert.That(await cache.TouchAsync("k1", TimeSpan.FromMilliseconds(200), cancellationToken)).IsTrue();
        var expirationAfter = await cache.GetExpirationAsync("k1", cancellationToken);
        _ = await Assert.That(expirationAfter.Found).IsTrue();
        _ = await Assert.That(expirationAfter.HasExpiration).IsTrue();
        _ = await Assert.That(expirationAfter.Value > TimeSpan.Zero).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v");
    }
}
