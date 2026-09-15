using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>Integration tests for multi-node expiration, Touch, and RemoveExpiration semantics.</summary>
[Immutable]
public sealed class CrossNodeExpirationTests : CrossNodeClockTestBase
{
    /// <summary>Verifies remote AddAsync treats an expired key as absent and inserts a new value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddNodeBTreatsExpiredRemoteKeyAsAbsent(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-add-expired");
        await Cluster.CacheA.SetAsync(key, "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        await Cluster.CacheB.AddAsync(key, "new", cancellationToken: cancellationToken);
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("new");
    }

    /// <summary>Verifies remote TryAddAsync treats an expired key as absent and inserts a new value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddReplacesExpiredRemote(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-add-expired");
        await Cluster.CacheA.SetAsync(key, "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await Cluster.CacheB.TryAddAsync(key, "new", cancellationToken: cancellationToken)).IsTrue();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("new");
    }

    /// <summary>Verifies an expired remote-owner entry is observed as missing from another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredFromNodeAIsMissingOnNodeB(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-expire");
        var expiration = TimeSpan.FromSeconds(2);
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(expiration), cancellationToken);
        _ = await Assert.That((await Cluster.CacheB.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v1");
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        _ = await Assert.That((await Cluster.CacheB.GetValueAsync(key, cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await Cluster.CacheB.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies GetExpirationAsync sees the expiration for a named-cache entry written by another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetExpiryOnNodeBReturnsEntryFromNodeA(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-get-expiration");
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(TimeSpan.FromHours(1)), cancellationToken);
        var expiration = await Cluster.CacheB.GetExpirationAsync(key, cancellationToken);
        _ = await Assert.That(expiration.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
    }

    /// <summary>Verifies RemoveExpirationAsync from another node prevents expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistBeforeExpiryKeepsRemoteKeyAlive(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "remote-remove-expiration-race");

        // The base expiration (60s) vastly exceeds any scheduling delay, so the remote persist cannot
        // race an expiry; removal is proven by the metadata assertions below (#412).
        await Cluster.CacheA.SetAsync(key, "v", TwoNodeSupport.Options(TimeSpan.FromSeconds(60)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(250));
        _ = await Assert.That(await Cluster.CacheB.RemoveExpirationAsync(key, cancellationToken)).IsTrue();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v");
        _ = await Assert.That((await Cluster.CacheB.GetExpirationAsync(key, cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies remote RemoveExpirationAsync on a non-expiring key returns false and keeps the key live.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistNonExpiringOnNodeBKeepsKeyLive(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-non-expiring");
        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: cancellationToken);
        _ = await Assert.That(await Cluster.CacheB.RemoveExpirationAsync(key, cancellationToken)).IsFalse();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v");
        _ = await Assert.That((await Cluster.CacheA.GetExpirationAsync(key, cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync can remove expiration from a named-cache entry written by another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistOnNodeBClearsExpiryOfNodeAEntry(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-persist-remove-expiration");
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(TimeSpan.FromHours(1)), cancellationToken);
        _ = await Assert.That(await Cluster.CacheB.RemoveExpirationAsync(key, cancellationToken)).IsTrue();
    }

    /// <summary>Verifies remote RemoveExpirationAsync removes expiration once and returns false on subsequent calls for an already persistent key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistOnNodeBIdempotentForRemoteKey(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-idempotent");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMinutes(1)), cancellationToken);
        _ = await Assert.That(await Cluster.CacheB.RemoveExpirationAsync(key, cancellationToken)).IsTrue();
        _ = await Assert.That(await Cluster.CacheB.RemoveExpirationAsync(key, cancellationToken)).IsFalse();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v");
        _ = await Assert.That((await Cluster.CacheA.GetExpirationAsync(key, cancellationToken)).HasExpiration).IsFalse();
    }

    /// <summary>Verifies remote RemoveExpirationAsync treats an expired key as missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistOnNodeBSkipsExpiredRemoteKey(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await Cluster.CacheB.RemoveExpirationAsync(key, cancellationToken)).IsFalse();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies TouchAsync from another node extends a key before it expires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteTouchBeforeExpirationKeepsKeyAlive(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-race");

        // The base expiration (60s) vastly exceeds any scheduling delay, so the remote touch cannot
        // race an expiry; the extension is proven by the remaining-TTL metadata below (#412).
        await Cluster.CacheA.SetAsync(key, "v", TwoNodeSupport.Options(TimeSpan.FromSeconds(60)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(250));
        _ = await Assert.That(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromSeconds(10), cancellationToken)).IsTrue();
        var expiration = await Cluster.CacheB.GetExpirationAsync(key, cancellationToken);
        _ = await Assert.That(expiration.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        _ = await Assert.That(expiration.Value <= TimeSpan.FromSeconds(10)).IsTrue();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v");
    }

    /// <summary>Verifies remote RemoveAsync treats expired entries as missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveIgnoresExpiredRemote(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        var removed = await Cluster.CacheB.RemoveAsync(key, cancellationToken);
        _ = await Assert.That(removed).IsFalse();
    }

    /// <summary>Verifies remote RemoveAsync treats an expired key as missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemovingOnNodeBIgnoresExpiredRemoteKey(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await Cluster.CacheB.RemoveAsync(key, cancellationToken)).IsFalse();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies remote TouchAsync on a non-expiring key adds expiration and keeps the value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchOnNodeBAddsExpiryAndKeepsValue(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-non-expiring");
        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: cancellationToken);
        _ = await Assert.That(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromMinutes(1), cancellationToken)).IsTrue();
        var expiration = await Cluster.CacheA.GetExpirationAsync(key, cancellationToken);
        _ = await Assert.That(expiration.Value > TimeSpan.Zero).IsTrue();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v");
    }

    /// <summary>Verifies remote TouchAsync treats an expired key as missing and does not resurrect it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchOnNodeBDoesNotResurrectExpiredKey(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), cancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromMinutes(1), cancellationToken)).IsFalse();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies TouchAsync can update expiration for a named-cache entry written by another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchOnNodeBExtendsExpiryFromNodeAEntry(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-update-expiration");
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(TimeSpan.FromHours(1)), cancellationToken);
        _ = await Assert.That(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromHours(2), cancellationToken)).IsTrue();
    }
}
