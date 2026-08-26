using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Xunit;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>Integration tests for multi-node expiration, Touch, and RemoveExpiration semantics.</summary>
[Immutable]
public sealed class CrossNodeExpirationTests : CrossNodeClockTestBase
{
    /// <summary>Verifies remote AddAsync treats an expired key as absent and inserts a new value.</summary>
    [Fact]
    public async Task AddNodeBTreatsExpiredRemoteKeyAsAbsent()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-add-expired");
        await Cluster.CacheA.SetAsync(key, "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        await Cluster.CacheB.AddAsync(key, "new", cancellationToken: DefaultCancellationToken);
        Assert.Equal("new", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies an expired remote-owner entry is observed as missing from another node.</summary>
    [Fact]
    public async Task ExpiredFromNodeAIsMissingOnNodeB()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-expire");
        var expiration = TimeSpan.FromSeconds(2);
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(expiration), DefaultCancellationToken);
        Assert.Equal("v1", (await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        Assert.False((await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Found);
        Assert.False((await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetExpirationAsync sees the expiration for a named-cache entry written by another node.</summary>
    [Fact]
    public async Task GetExpiryOnNodeBReturnsEntryFromNodeA()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-get-expiration");
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(TimeSpan.FromHours(1)), DefaultCancellationToken);
        var expiration = await Cluster.CacheB.GetExpirationAsync(key, DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
    }

    /// <summary>Verifies RemoveExpirationAsync from another node prevents expiration.</summary>
    [Fact]
    public async Task PersistBeforeExpiryKeepsRemoteKeyAlive()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "remote-remove-expiration-race");

        // The base expiration (60s) vastly exceeds any scheduling delay, so the remote persist cannot
        // race an expiry; removal is proven by the metadata assertions below (#412).
        await Cluster.CacheA.SetAsync(key, "v", TwoNodeSupport.Options(TimeSpan.FromSeconds(60)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(250));
        Assert.True(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.False((await Cluster.CacheB.GetExpirationAsync(key, DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies remote RemoveExpirationAsync on a non-expiring key returns false and keeps the key live.</summary>
    [Fact]
    public async Task PersistNonExpiringOnNodeBKeepsKeyLive()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-non-expiring");
        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: DefaultCancellationToken);
        Assert.False(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.False((await Cluster.CacheA.GetExpirationAsync(key, DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies RemoveExpirationAsync can remove expiration from a named-cache entry written by another node.</summary>
    [Fact]
    public async Task PersistOnNodeBClearsExpiryOfNodeAEntry()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-persist-remove-expiration");
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(TimeSpan.FromHours(1)), DefaultCancellationToken);
        Assert.True(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
    }

    /// <summary>Verifies remote RemoveExpirationAsync removes expiration once and returns false on subsequent calls for an already persistent key.</summary>
    [Fact]
    public async Task PersistOnNodeBIdempotentForRemoteKey()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-idempotent");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMinutes(1)), DefaultCancellationToken);
        Assert.True(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.False(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.False((await Cluster.CacheA.GetExpirationAsync(key, DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>Verifies remote RemoveExpirationAsync treats an expired key as missing.</summary>
    [Fact]
    public async Task PersistOnNodeBSkipsExpiredRemoteKey()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.False(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies TouchAsync from another node extends a key before it expires.</summary>
    [Fact]
    public async Task RemoteTouchBeforeExpirationKeepsKeyAlive()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-race");

        // The base expiration (60s) vastly exceeds any scheduling delay, so the remote touch cannot
        // race an expiry; the extension is proven by the remaining-TTL metadata below (#412).
        await Cluster.CacheA.SetAsync(key, "v", TwoNodeSupport.Options(TimeSpan.FromSeconds(60)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(250));
        Assert.True(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromSeconds(10), DefaultCancellationToken));
        var expiration = await Cluster.CacheB.GetExpirationAsync(key, DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Value <= TimeSpan.FromSeconds(10));
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies remote RemoveAsync treats an expired key as missing.</summary>
    [Fact]
    public async Task RemovingOnNodeBIgnoresExpiredRemoteKey()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.False(await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken));
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies remote TouchAsync on a non-expiring key adds expiration and keeps the value.</summary>
    [Fact]
    public async Task TouchOnNodeBAddsExpiryAndKeepsValue()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-non-expiring");
        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: DefaultCancellationToken);
        Assert.True(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromMinutes(1), DefaultCancellationToken));
        var expiration = await Cluster.CacheA.GetExpirationAsync(key, DefaultCancellationToken);
        Assert.True(expiration.Value > TimeSpan.Zero);
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies remote TouchAsync treats an expired key as missing and does not resurrect it.</summary>
    [Fact]
    public async Task TouchOnNodeBDoesNotResurrectExpiredKey()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.False(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromMinutes(1), DefaultCancellationToken));
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies TouchAsync can update expiration for a named-cache entry written by another node.</summary>
    [Fact]
    public async Task TouchOnNodeBExtendsExpiryFromNodeAEntry()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-update-expiration");
        await Cluster.CacheA.SetAsync(key, "v1", TwoNodeSupport.Options(TimeSpan.FromHours(1)), DefaultCancellationToken);
        Assert.True(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromHours(2), DefaultCancellationToken));
    }

    /// <summary>Verifies remote TryAddAsync treats an expired key as absent and inserts a new value.</summary>
    [Fact]
    public async Task TryAddOnNodeBAllowsExpiredRemoteKey()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-add-expired");
        await Cluster.CacheA.SetAsync(key, "expired", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        Assert.True(await Cluster.CacheB.TryAddAsync(key, "new", cancellationToken: DefaultCancellationToken));
        Assert.Equal("new", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies remote RemoveAsync treats expired entries as missing.</summary>
    [Fact]
    public async Task TryRemoveOnNodeBIgnoresExpiredEntry()
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-expired");
        await Cluster.CacheA.SetAsync(key, "v", Expiry.In(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        Clock.Advance(TimeSpan.FromMilliseconds(1800));
        var removed = await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken);
        Assert.False(removed);
    }
}
