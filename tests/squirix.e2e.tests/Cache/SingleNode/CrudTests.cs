using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Support.Cluster.Fixtures;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node public CRUD operations.</summary>
public sealed class CrudTests(SingleNodeFixture fixture) : SingleNodeTestBase(fixture)
{
    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    [Fact]
    public async Task AddAsyncEntryAddsOnMissThrowsOnHit()
    {
        var cache = await Client.GetCacheAsync<string>("add-async-entry", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(() => cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>Verifies AddAsync with options preserves expiration metadata through the public API.</summary>
    [Fact]
    public async Task AddAsyncEntryPreservesExpirationThroughPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("missing-add-entry-expiration", DefaultCancellationToken);

        await cache.AddAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(10),
            },
            DefaultCancellationToken);

        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Value > TimeSpan.Zero);
    }

    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    [Fact]
    public async Task AddAsyncValueAddsOnMissThrowsOnHit()
    {
        var cache = await Client.GetCacheAsync<string>("add-async-value", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(() => cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    [Fact]
    public async Task AddEntryAddsOnMissThrowsOnHit()
    {
        var cache = await Client.GetCacheAsync<string>("add-entry", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(() => cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>Verifies AddAsync(string, T) adds on miss and throws on existing key.</summary>
    [Fact]
    public async Task AddValueAddsOnMissThrowsOnHit()
    {
        var cache = await Client.GetCacheAsync<string>("add-value", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(() => cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>Verifies the public core transport does not round-trip internal tag metadata.</summary>
    [Fact]
    public async Task GetEntryAsyncDoesNotRoundTripInternalTagMetadata()
    {
        var cache = await Client.GetCacheAsync<string>("immutable-output-tags-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        var entry = await cache.GetEntryAsync("k", DefaultCancellationToken);
        Assert.True(entry.Found);
    }

    /// <summary>Verifies GetEntryAsync returns entry or null when missing or expired.</summary>
    [Fact]
    public async Task GetEntryAsyncReturnsEntryOrNull()
    {
        var cache = await Client.GetCacheAsync<string>("get-entry-async", DefaultCancellationToken);

        Assert.False((await cache.GetEntryAsync("missing", DefaultCancellationToken)).Found);

        await cache.SetAsync("k1", "v1", new CacheEntryOptions { Expiration = Delay60 }, DefaultCancellationToken);
        var e = await cache.GetEntryAsync("k1", DefaultCancellationToken);
        Assert.True(e.Found);
        Assert.Equal("v1", e.Value);

        await Task.Delay(Delay90, TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetEntryAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetEntry returns entry with metadata or null when missing or expired.</summary>
    [Fact]
    public async Task GetEntryReturnsEntryOrNull()
    {
        var cache = await Client.GetCacheAsync<string>("get-entry", DefaultCancellationToken);

        Assert.False((await cache.GetEntryAsync("missing", DefaultCancellationToken)).Found);

        await cache.SetAsync("k1", "v1", new CacheEntryOptions { Expiration = Delay60 }, DefaultCancellationToken);
        var e = await cache.GetEntryAsync("k1", DefaultCancellationToken);
        Assert.True(e.Found);
        Assert.Equal("v1", e.Value);

        await Task.Delay(Delay90, TimeProvider.System, DefaultCancellationToken);
        Assert.False((await cache.GetEntryAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetValueAsync returns proper flags and value.</summary>
    [Fact]
    public async Task GetValueAsyncReturnsFlagsAndValue()
    {
        var cache = await Client.GetCacheAsync<string>("try-get-async", DefaultCancellationToken);

        var miss = await cache.GetValueAsync("missing", DefaultCancellationToken);
        Assert.False(miss.Found);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var hit = await cache.GetValueAsync("k1", DefaultCancellationToken);
        Assert.True(hit.Found);
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    [Fact]
    public async Task InsertEntryUpserts()
    {
        var cache = await Client.GetCacheAsync<string>("insert-entry", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    [Fact]
    public async Task InsertValueUpserts()
    {
        var cache = await Client.GetCacheAsync<string>("insert-value", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies RemoveAsync deletes when present and returns false on miss.</summary>
    [Fact]
    public async Task RemoveAsyncDeletesWhenPresent()
    {
        var cache = await Client.GetCacheAsync<string>("remove-async", DefaultCancellationToken);

        Assert.False(await cache.RemoveAsync("missing", DefaultCancellationToken));

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.True(await cache.RemoveAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync returns whether a live entry was removed.</summary>
    [Fact]
    public async Task RemoveAsyncReturnsFlagAndValue()
    {
        var cache = await Client.GetCacheAsync<string>("try-remove-async", DefaultCancellationToken);

        var miss = await cache.RemoveAsync("missing", DefaultCancellationToken);
        Assert.False(miss);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var hit = await cache.RemoveAsync("k1", DefaultCancellationToken);
        Assert.True(hit);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync returns the removed entry metadata before deleting the key.</summary>
    [Fact]
    public async Task RemoveAsyncReturnsRemovedEntryMetadata()
    {
        var cache = await Client.GetCacheAsync<string>("try-remove-entry-metadata-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        var before = await cache.GetEntryAsync("k", DefaultCancellationToken);
        Assert.True(before.Found);
        Assert.Equal("v", before.Value);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    [Fact]
    public async Task RemoveAsyncReturnsRemovedForStoredEntryNullValue()
    {
        var cache = await Client.GetCacheAsync<object?>("try-remove-null-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);

        var result = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(result);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    [Fact]
    public async Task RemoveAsyncReturnsRemovedForStoredNullValue()
    {
        var cache = await Client.GetCacheAsync<string?>("try-remove-null-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    [Fact]
    public async Task RemoveAsyncStoredNullReportsRemoved()
    {
        var cache = await Client.GetCacheAsync<object?>("try-remove-null-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync deletes when present and returns false on miss.</summary>
    [Fact]
    public async Task RemoveDeletesWhenPresent()
    {
        var cache = await Client.GetCacheAsync<string>("remove", DefaultCancellationToken);

        Assert.False(await cache.RemoveAsync("missing", DefaultCancellationToken));

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.True(await cache.RemoveAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies two cache facades for the same name share logical storage before client disposal.</summary>
    [Fact]
    public async Task RepeatedGetCacheAsyncForSameNameSharesLogicalStorage()
    {
        var first = await Client.GetCacheAsync<string>("same-name-facades-public-extra", DefaultCancellationToken);
        var second = await Client.GetCacheAsync<string>("same-name-facades-public-extra", DefaultCancellationToken);

        await first.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v", (await second.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies SetAsync rejects options that specify both ExpiresAt and Expiration.</summary>
    [Fact]
    public async Task SetAsyncEntryRejectsBothExpiresUtcAndExpiration()
    {
        var cache = await Client.GetCacheAsync<string>("invalid-expiration-both-public-extra", DefaultCancellationToken);

        _ = await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
        {
            await cache.SetAsync(
                "k",
                "v",
                new CacheEntryOptions
                {
                    ExpiresAt = DateTimeOffset.UtcNow.AddMinutes(1),
                    Expiration = TimeSpan.FromMinutes(1),
                },
                DefaultCancellationToken);
        });

        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    [Fact]
    public async Task SetAsyncEntryUpserts()
    {
        var cache = await Client.GetCacheAsync<string>("insert-async-entry", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies SetAsync(string, T) upserts unconditionally.</summary>
    [Fact]
    public async Task SetAsyncValueUpserts()
    {
        var cache = await Client.GetCacheAsync<string>("insert-async-value", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TryAddAsync with options preserves expiration metadata through the public API.</summary>
    [Fact]
    public async Task TryAddAsyncEntryPreservesExpirationThroughPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("missing-try-add-entry-expiration", DefaultCancellationToken);

        var added = await cache.TryAddAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(10),
            },
            DefaultCancellationToken);

        Assert.True(added);
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Value > TimeSpan.Zero);
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    [Fact]
    public async Task TryAddAsyncEntryRespectsExistence()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-async-entry", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    [Fact]
    public async Task TryAddAsyncValueRespectsExistence()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-async-value", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    [Fact]
    public async Task TryAddEntryRespectsExistence()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-entry", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TryAddAsync(string, T) returns true on miss and false on hit.</summary>
    [Fact]
    public async Task TryAddValueRespectsExistence()
    {
        var cache = await Client.GetCacheAsync<string>("try-add-value", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies TryGetValue returns proper flags and value.</summary>
    [Fact]
    public async Task TryGetValueReturnsFlagsAndValue()
    {
        var cache = await Client.GetCacheAsync<string>("try-get", DefaultCancellationToken);

        var miss = await cache.GetValueAsync("missing", DefaultCancellationToken);
        Assert.False(miss.Found);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var found = await cache.GetValueAsync("k1", DefaultCancellationToken);
        Assert.True(found.Found);
    }

    /// <summary>Verifies TryRemove returns whether a live entry was removed.</summary>
    [Fact]
    public async Task TryRemoveReturnsFlagAndValue()
    {
        var cache = await Client.GetCacheAsync<string>("try-remove", DefaultCancellationToken);

        var miss = await cache.RemoveAsync("missing", DefaultCancellationToken);
        Assert.False(miss);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var removed = await cache.RemoveAsync("k1", DefaultCancellationToken);
        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }
}
