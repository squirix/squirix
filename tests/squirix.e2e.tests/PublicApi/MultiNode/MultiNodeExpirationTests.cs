using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Xunit;

namespace Squirix.E2ETests.PublicApi.MultiNode;

/// <summary>
/// Integration tests for multi-node expiration, Touch, and RemoveExpiration semantics.
/// </summary>
public sealed class MultiNodeExpirationTests(TwoNodeFixture fixture) : PublicApiMultiNodeTestBase(fixture)
{
    /// <summary>
    /// Verifies remote AddAsync treats an expired key as absent and inserts a new value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task AddNodeBTreatsExpiredRemoteKeyAsAbsent()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-add-expired");

        await Cluster.CacheA.SetAsync(
            key,
            "expired",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);
        await Cluster.CacheB.AddAsync(key, "new", cancellationToken: DefaultCancellationToken);
        Assert.Equal("new", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies an expired remote-owner entry is observed as missing from another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task ExpiredEntryInsertedOnNodeAIsMissingWhenReadFromNodeB()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-expire");
        var expiration = TimeSpan.FromSeconds(2);

        await Cluster.CacheA.SetAsync(key, "v1", MultiNodeTestSupport.Options(expiration), DefaultCancellationToken);

        Assert.Equal("v1", (await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Value);

        await Task.Delay(expiration + TimeSpan.FromMilliseconds(500), DefaultCancellationToken);

        Assert.False((await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Found);
        Assert.False((await Cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies GetExpirationAsync sees the expiration for a named-cache entry written by another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task GetExpirationOnNodeBReturnsExpirationForEntryInsertedOnNodeA()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-get-expiration");

        await Cluster.CacheA.SetAsync(key, "v1", MultiNodeTestSupport.Options(TimeSpan.FromHours(1)), DefaultCancellationToken);
        var expiration = await Cluster.CacheB.GetExpirationAsync(key, DefaultCancellationToken);
        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
    }

    /// <summary>
    /// Verifies remote RemoveExpirationAsync removes expiration once and returns false on subsequent calls for an already persistent key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task PersistOnNodeBIsIdempotentForExistingRemoteKey()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-idempotent");

        await Cluster.CacheA.SetAsync(
            key,
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.False(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.False((await Cluster.CacheA.GetExpirationAsync(key, DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies remote RemoveExpirationAsync on a non-expiring key returns false and keeps the key live.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task PersistOnNodeBNonExpiringRemoteKeyReturnsFalseAndKeepsKeyLive()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-non-expiring");

        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: DefaultCancellationToken);

        Assert.False(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.False((await Cluster.CacheA.GetExpirationAsync(key, DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync can remove expiration from a named-cache entry written by another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task PersistOnNodeBRemovesExpirationFromEntryInsertedOnNodeA()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-persist-remove-expiration");

        await Cluster.CacheA.SetAsync(key, "v1", MultiNodeTestSupport.Options(TimeSpan.FromHours(1)), DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies remote RemoveExpirationAsync treats an expired key as missing.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task PersistOnNodeBTreatsExpiredRemoteKeyAsMissing()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expiration-expired");

        await Cluster.CacheA.SetAsync(
            key,
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.False(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveExpirationAsync from another node prevents expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RemotePersistBeforeExpirationKeepsKeyAlive()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeB", "remote-remove-expiration-race");

        // Margins wide enough for slow thread pools and parallel test runs (Rider full suite).
        await Cluster.CacheA.SetAsync(key, "v", MultiNodeTestSupport.Options(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(200), DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.RemoveExpirationAsync(key, DefaultCancellationToken));
        await Task.Delay(TimeSpan.FromMilliseconds(350), DefaultCancellationToken);

        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
        Assert.False((await Cluster.CacheB.GetExpirationAsync(key, DefaultCancellationToken)).HasExpiration);
    }

    /// <summary>
    /// Verifies TouchAsync from another node extends a key before it expires.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RemoteTouchBeforeExpirationKeepsKeyAlive()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-race");

        // Margins wide enough for slow thread pools and parallel test runs (Rider full suite).
        await Cluster.CacheA.SetAsync(key, "v", MultiNodeTestSupport.Options(TimeSpan.FromMilliseconds(500)), DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(200), DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromSeconds(2), DefaultCancellationToken));

        await Task.Delay(TimeSpan.FromMilliseconds(350), DefaultCancellationToken);

        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies remote RemoveAsync treats an expired key as missing.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RemoveNodeBTreatsExpiredRemoteKeyAsMissing()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-remove-expired");

        await Cluster.CacheA.SetAsync(
            key,
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.False(await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken));
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies remote TouchAsync on a non-expiring key adds expiration and keeps the value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TouchOnNodeBNonExpiringRemoteKeyAddsExpirationAndKeepsValue()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-non-expiring");

        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromMinutes(1), DefaultCancellationToken));

        var expiration = await Cluster.CacheA.GetExpirationAsync(key, DefaultCancellationToken);

        Assert.True(expiration.Value > TimeSpan.Zero);
        Assert.Equal("v", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies remote TouchAsync treats an expired key as missing and does not resurrect it.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TouchOnNodeBTreatsExpiredRemoteKeyAsMissingAndDoesNotResurrect()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-expired");

        await Cluster.CacheA.SetAsync(
            key,
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.False(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromMinutes(1), DefaultCancellationToken));
        Assert.False((await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies TouchAsync can update expiration for a named-cache entry written by another node.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TouchOnNodeBUpdatesExpirationForEntryInsertedOnNodeA()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-touch-update-expiration");

        await Cluster.CacheA.SetAsync(key, "v1", MultiNodeTestSupport.Options(TimeSpan.FromHours(1)), DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.TouchAsync(key, TimeSpan.FromHours(2), DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies remote TryAddAsync treats an expired key as absent and inserts a new value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryAddOnNodeBTreatsExpiredRemoteKeyAsAbsent()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-add-expired");

        await Cluster.CacheA.SetAsync(
            key,
            "expired",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(100),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(300), DefaultCancellationToken);

        Assert.True(await Cluster.CacheB.TryAddAsync(key, "new", cancellationToken: DefaultCancellationToken));
        Assert.Equal("new", (await Cluster.CacheA.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies remote RemoveAsync treats expired entries as missing.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task TryRemoveOnNodeBTreatsExpiredRemoteEntryAsMissing()
    {
        var key = MultiNodeTestSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-expired");

        await Cluster.CacheA.SetAsync(
            key,
            "v",
            new CacheEntryOptions
            {
                Expiration = TimeSpan.FromMilliseconds(50),
            },
            DefaultCancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(150), DefaultCancellationToken);

        var removed = await Cluster.CacheB.RemoveAsync(key, DefaultCancellationToken);

        Assert.False(removed);
    }
}
