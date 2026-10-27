using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node public CRUD operations.</summary>
[Immutable]
public sealed class CrudTests : TestBase
{
    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddAsyncEntryAddsOnMissThrowsOnHit(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("add-async-entry", cancellationToken);
        await cache.AddAsync("k1", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
        _ = await NodeAsyncAssert.ThrowsAsync<CacheConflictException>(cache.AddAsync("k1", "v2", cancellationToken: cancellationToken));
    }

    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddAsyncValueAddsOnMissThrowsOnHit(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("add-async-value", cancellationToken);
        await cache.AddAsync("k1", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
        _ = await NodeAsyncAssert.ThrowsAsync<CacheConflictException>(cache.AddAsync("k1", "v2", cancellationToken: cancellationToken));
    }

    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddEntryAddsOnMissThrowsOnHit(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("add-entry", cancellationToken);
        await cache.AddAsync("k1", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
        _ = await NodeAsyncAssert.ThrowsAsync<CacheConflictException>(cache.AddAsync("k1", "v2", cancellationToken: cancellationToken));
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddEntryAsyncKeepsExisting(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-add-async-entry", cancellationToken);
        _ = await Assert.That(await cache.TryAddAsync("k1", "v1", cancellationToken: cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.TryAddAsync("k1", "v2", cancellationToken: cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddEntryKeepsExisting(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-add-entry", cancellationToken);
        _ = await Assert.That(await cache.TryAddAsync("k1", "v1", cancellationToken: cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.TryAddAsync("k1", "v2", cancellationToken: cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
    }

    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddValueAddsOnMissThrowsOnHit(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("add-value", cancellationToken);
        await cache.AddAsync("k1", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
        _ = await NodeAsyncAssert.ThrowsAsync<CacheConflictException>(cache.AddAsync("k1", "v2", cancellationToken: cancellationToken));
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddValueAsyncKeepsExisting(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-add-async-value", cancellationToken);
        _ = await Assert.That(await cache.TryAddAsync("k1", "v1", cancellationToken: cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.TryAddAsync("k1", "v2", cancellationToken: cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddValueKeepsExisting(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-add-value", cancellationToken);
        _ = await Assert.That(await cache.TryAddAsync("k1", "v1", cancellationToken: cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.TryAddAsync("k1", "v2", cancellationToken: cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v1");
    }

    /// <summary>Verifies the public core transport does not round-trip internal tag metadata.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetEntryAsyncOmitsInternalTagMetadata(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("immutable-output-tags-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);
        var entry = await cache.GetEntryAsync("k", cancellationToken);
        _ = await Assert.That(entry.Found).IsTrue();
    }

    /// <summary>Verifies GetValueAsync returns proper flags and value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetValueAsyncReturnsFlagsAndValue(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-get-async", cancellationToken);
        var miss = await cache.GetValueAsync("missing", cancellationToken);
        _ = await Assert.That(miss.Found).IsFalse();
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        var hit = await cache.GetValueAsync("k1", cancellationToken);
        _ = await Assert.That(hit.Found).IsTrue();
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InsertEntryUpserts(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("insert-entry", cancellationToken);
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v2");
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InsertValueUpserts(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("insert-value", cancellationToken);
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v2");
    }

    /// <summary>Verifies RemoveAsync deletes when present and returns false on miss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAsyncDeletesWhenPresent(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove-async", cancellationToken);
        _ = await Assert.That(await cache.RemoveAsync("missing", cancellationToken)).IsFalse();
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.RemoveAsync("k1", cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies RemoveAsync returns whether a live entry was removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAsyncReturnsFlagAndValue(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-remove-async", cancellationToken);
        var miss = await cache.RemoveAsync("missing", cancellationToken);
        _ = await Assert.That(miss).IsFalse();
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        var hit = await cache.RemoveAsync("k1", cancellationToken);
        _ = await Assert.That(hit).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies RemoveAsync deletes when present and returns false on miss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveDeletesWhenPresent(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("remove", cancellationToken);
        _ = await Assert.That(await cache.RemoveAsync("missing", cancellationToken)).IsFalse();
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.RemoveAsync("k1", cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies two cache facades for the same name share logical storage before client disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedGetCacheAsyncSharesStorage(CancellationToken cancellationToken)
    {
        var first = await Client.GetCacheAsync<string>("same-name-facades-public-extra", cancellationToken);
        var second = await Client.GetCacheAsync<string>("same-name-facades-public-extra", cancellationToken);
        await first.SetAsync("k", "v", cancellationToken: cancellationToken);
        _ = await Assert.That((await second.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAsyncEntryUpserts(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("insert-async-entry", cancellationToken);
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v2");
    }

    /// <summary>Verifies SetAsync rejects options that specify both ExpiresAt and Expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAsyncRejectsExpiresUtcPlusExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("invalid-expiration-both-public-extra", cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<ArgumentException>(
            cache.SetAsync("k", "v", new CacheEntryOptions { ExpiresAt = DateTimeOffset.UtcNow.AddMinutes(1), Expiration = TimeSpan.FromMinutes(1) }, cancellationToken));
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAsyncValueUpserts(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("insert-async-value", cancellationToken);
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Value).IsEqualTo("v2");
    }
}
