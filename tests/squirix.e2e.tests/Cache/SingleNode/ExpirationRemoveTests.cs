using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Remove and RemoveExpiration semantics.</summary>
[Immutable]
public sealed class ExpirationRemoveTests : ClockTestBase
{
    /// <summary>Verifies expired entries are treated as missing by RemoveAsync.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAsyncTreatsExpiredEntryAsMissing(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-remove-expired-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        var removed = await cache.RemoveAsync("k", cancellationToken);
        _ = await Assert.That(removed).IsFalse();
    }

    /// <summary>Verifies RemoveAsync on an expired key returns false and does not resurrect or expose the expired value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAsyncTreatsExpiredKeyAsMissing(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-expired-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await cache.RemoveAsync("k", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration and keeps the entry beyond the original expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationAsyncRemovesExpiration(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-remove-expiration-async", cancellationToken);
        await cache.SetAsync("k1", "v", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        var expirationBefore = await cache.GetExpirationAsync("k1", cancellationToken);
        _ = await Assert.That(expirationBefore.Found).IsTrue();
        _ = await Assert.That(expirationBefore.HasExpiration).IsTrue();
        _ = await Assert.That(await cache.RemoveExpirationAsync("k1", cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetExpirationAsync("k1", cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration and keeps the entry beyond the original expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationRemovesExpiration(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("expiration-remove-expiration-sync", cancellationToken);
        await cache.SetAsync("k1", "v", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        var before = await cache.GetExpirationAsync("k1", cancellationToken);
        _ = await Assert.That(before.Found).IsTrue();
        _ = await Assert.That(before.HasExpiration).IsTrue();
        _ = await Assert.That(await cache.RemoveExpirationAsync("k1", cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetExpirationAsync("k1", cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync returns false and removes an already expired entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryExpiredFalseMakesKeyMissing(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-expired-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await cache.RemoveExpirationAsync("k", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync on a non-expiring key returns false and keeps the key live.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryNonExpiringFalseKeepsLive(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-non-expiring-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("k", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
        _ = await Assert.That((await cache.GetExpirationAsync("k", cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync returns false for missing and already persistent entries and true when expiration is removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryOnPersistentExpiringEntries(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-result-status-public-extra", cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("missing", cancellationToken)).IsFalse();
        await cache.SetAsync("persistent", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("persistent", cancellationToken)).IsFalse();
        await cache.SetAsync("expiring", "v2", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("expiring", cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetExpirationAsync("expiring", cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration once and returns false on subsequent calls for an already persistent key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryReturnsFalseForPersistent(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-idempotent-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("k", cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.RemoveExpirationAsync("k", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
        _ = await Assert.That((await cache.GetExpirationAsync("k", cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync returns false for a missing key and an already non-expiring live key through the public API.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryReturnsPersistentKeyViaApi(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("missing-remove-expiration-false", cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("missing", cancellationToken)).IsFalse();
        await cache.SetAsync("persistent", "v", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("persistent", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("persistent", cancellationToken)).Value).IsEqualTo("v");
    }

    /// <summary>Verifies RemoveExpirationAsync treats an expired key as missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryTreatsExpiredKeyAsMissing(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration-expired-public-extra-2", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await cache.RemoveExpirationAsync("k", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }
}
