using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node Remove and RemoveExpiration semantics.</summary>
[Immutable]
public sealed class ExpirationRemoveTests : ClockTestBase
{
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

    /// <summary>Verifies removing expiration changes only a live key that has a TTL.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryOnlyChangesExpiringKey(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-expiration", cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("missing", cancellationToken)).IsFalse();

        await cache.SetAsync("persistent", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("persistent", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("persistent", cancellationToken)).Value).IsEqualTo("v1");
        _ = await Assert.That((await cache.GetExpirationAsync("persistent", cancellationToken)).HasExpiration).IsFalse();

        await cache.SetAsync("expiring", "v2", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        var expirationBefore = await cache.GetExpirationAsync("expiring", cancellationToken);
        _ = await Assert.That(expirationBefore.Found).IsTrue();
        _ = await Assert.That(expirationBefore.HasExpiration).IsTrue();
        _ = await Assert.That(await cache.RemoveExpirationAsync("expiring", cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.RemoveExpirationAsync("expiring", cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("expiring", cancellationToken)).Value).IsEqualTo("v2");
        _ = await Assert.That((await cache.GetExpirationAsync("expiring", cancellationToken)).HasExpiration).IsFalse();
    }
}
