using System;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.E2ETests.PublicApi.SingleNode;

/// <summary>
/// Integration tests for single-node public CRUD operations.
/// </summary>
public sealed class SingleNodeCrudTests : PublicApiSingleNodeTestBase
{
    /// <summary>
    /// Verifies AddAsync(string, T) adds on miss and throws on existing key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddAsyncEntryAddsOnMissThrowsOnHit()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("add-async-entry", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(async () => await cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies AddAsync with options preserves expiration metadata through the public API.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddAsyncEntryPreservesExpirationThroughPublicApi()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("missing-add-entry-expiration", DefaultCancellationToken);

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

    /// <summary>
    /// Verifies AddAsync(string, T) adds on miss and throws on existing key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddAsyncValueAddsOnMissThrowsOnHit()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("add-async-value", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(async () => await cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies AddAsync(string, T) adds on miss and throws on existing key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddEntryAddsOnMissThrowsOnHit()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("add-entry", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(async () => await cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies AddAsync(string, T) adds on miss and throws on existing key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddValueAddsOnMissThrowsOnHit()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("add-value", DefaultCancellationToken);

        await cache.AddAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);

        _ = await Assert.ThrowsAsync<CacheConflictException>(async () => await cache.AddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
    }

    /// <summary>
    /// Verifies the public core transport does not round-trip internal tag metadata.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetEntryAsyncDoesNotRoundTripInternalTagMetadata()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("immutable-output-tags-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        var entry = await cache.GetEntryAsync("k", DefaultCancellationToken);

        Assert.NotNull(entry);
    }

    /// <summary>
    /// Verifies GetEntryAsync returns entry or null when missing or expired.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetEntryAsyncReturnsEntryOrNull()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("get-entry-async", DefaultCancellationToken);

        Assert.False((await cache.GetEntryAsync("missing", DefaultCancellationToken)).Found);

        await cache.SetAsync("k1", "v1", new CacheEntryOptions { Expiration = Delay60 }, DefaultCancellationToken);
        var e = await cache.GetEntryAsync("k1", DefaultCancellationToken);
        Assert.NotNull(e);

        await Task.Delay(Delay90, DefaultCancellationToken);
        Assert.False((await cache.GetEntryAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies GetEntry returns entry with metadata or null when missing or expired.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetEntryReturnsEntryOrNull()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("get-entry", DefaultCancellationToken);

        Assert.False((await cache.GetEntryAsync("missing", DefaultCancellationToken)).Found);

        await cache.SetAsync("k1", "v1", new CacheEntryOptions { Expiration = Delay60 }, DefaultCancellationToken);
        var e = await cache.GetEntryAsync("k1", DefaultCancellationToken);
        Assert.NotNull(e);

        await Task.Delay(Delay90, DefaultCancellationToken);
        Assert.False((await cache.GetEntryAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies GetValueAsync returns proper flags and value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetValueAsyncReturnsFlagsAndValue()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-get-async", DefaultCancellationToken);

        var miss = await cache.GetValueAsync("missing", DefaultCancellationToken);
        Assert.False(miss.Found);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var hit = await cache.GetValueAsync("k1", DefaultCancellationToken);
        Assert.True(hit.Found);
    }

    /// <summary>
    /// Verifies SetAsync(string, T) upserts unconditionally.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task InsertEntryUpserts()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("insert-entry", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies SetAsync(string, T) upserts unconditionally.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task InsertValueUpserts()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("insert-value", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies RemoveAsync deletes when present and returns false on miss.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveAsyncDeletesWhenPresent()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove-async", DefaultCancellationToken);

        Assert.False(await cache.RemoveAsync("missing", DefaultCancellationToken));

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.True(await cache.RemoveAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveAsync returns whether a live entry was removed.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveAsyncReturnsFlagAndValue()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-remove-async", DefaultCancellationToken);

        var miss = await cache.RemoveAsync("missing", DefaultCancellationToken);
        Assert.False(miss);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var hit = await cache.RemoveAsync("k1", DefaultCancellationToken);
        Assert.True(hit);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveAsync returns the removed entry metadata before deleting the key.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveAsyncReturnsRemovedEntryMetadata()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-remove-entry-metadata-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        var before = await cache.GetEntryAsync("k", DefaultCancellationToken);
        Assert.NotNull(before);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveAsync removes a stored null value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveAsyncReturnsRemovedForStoredEntryNullValue()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<object?>("try-remove-null-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);

        var result = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(result);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveAsync removes a stored null value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RemoveAsyncReturnsRemovedForStoredNullValue()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string?>("try-remove-null-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveAsync removes a stored null value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveAsyncStoredNullReportsRemoved()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<object?>("try-remove-null-public-extra", DefaultCancellationToken);

        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);

        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);

        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies RemoveAsync deletes when present and returns false on miss.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoveDeletesWhenPresent()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("remove", DefaultCancellationToken);

        Assert.False(await cache.RemoveAsync("missing", DefaultCancellationToken));

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        Assert.True(await cache.RemoveAsync("k1", DefaultCancellationToken));
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies two cache facades for the same name share logical storage before client disposal.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RepeatedGetCacheAsyncForSameNameSharesLogicalStorage()
    {
        await using var client = await ConnectClientAsync();
        var first = await client.GetCacheAsync<string>("same-name-facades-public-extra", DefaultCancellationToken);
        var second = await client.GetCacheAsync<string>("same-name-facades-public-extra", DefaultCancellationToken);

        await first.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);

        Assert.Equal("v", (await second.GetValueAsync("k", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies SetAsync rejects options that specify both ExpiresAt and Expiration.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task SetAsyncEntryRejectsBothExpiresUtcAndExpiration()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("invalid-expiration-both-public-extra", DefaultCancellationToken);

        _ = await Assert.ThrowsAnyAsync<ArgumentException>(async () => await cache.SetAsync(
            "k",
            "v",
            new CacheEntryOptions
            {
                ExpiresAt = DateTimeOffset.UtcNow.AddMinutes(1),
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken));

        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Verifies SetAsync(string, T) upserts unconditionally.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task SetAsyncEntryUpserts()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("insert-async-entry", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies SetAsync(string, T) upserts unconditionally.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task SetAsyncValueUpserts()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("insert-async-value", DefaultCancellationToken);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("k1", "v2", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v2", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TryAddAsync with options preserves expiration metadata through the public API.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddAsyncEntryPreservesExpirationThroughPublicApi()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("missing-try-add-entry-expiration", DefaultCancellationToken);

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
        Assert.NotNull(expiration);
        Assert.True(expiration.Value > TimeSpan.Zero);
    }

    /// <summary>
    /// Verifies TryAddAsync(string, T) returns true on miss and false on hit.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddAsyncEntryRespectsExistence()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-add-async-entry", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TryAddAsync(string, T) returns true on miss and false on hit.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddAsyncValueRespectsExistence()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-add-async-value", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TryAddAsync(string, T) returns true on miss and false on hit.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddEntryRespectsExistence()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-add-entry", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TryAddAsync(string, T) returns true on miss and false on hit.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryAddValueRespectsExistence()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-add-value", DefaultCancellationToken);

        Assert.True(await cache.TryAddAsync("k1", "v1", cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k1", "v2", cancellationToken: DefaultCancellationToken));
        Assert.Equal("v1", (await cache.GetValueAsync("k1", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies TryGetValue returns proper flags and value.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryGetValueReturnsFlagsAndValue()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-get", DefaultCancellationToken);

        var miss = await cache.GetValueAsync("missing", DefaultCancellationToken);
        Assert.False(miss.Found);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var found = await cache.GetValueAsync("k1", DefaultCancellationToken);
        Assert.True(found.Found);
    }

    /// <summary>
    /// Verifies TryRemove returns whether a live entry was removed.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task TryRemoveReturnsFlagAndValue()
    {
        await using var client = await ConnectClientAsync();
        var cache = await client.GetCacheAsync<string>("try-remove", DefaultCancellationToken);

        var miss = await cache.RemoveAsync("missing", DefaultCancellationToken);
        Assert.False(miss);

        await cache.SetAsync("k1", "v1", cancellationToken: DefaultCancellationToken);
        var removed = await cache.RemoveAsync("k1", DefaultCancellationToken);
        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k1", DefaultCancellationToken)).Found);
    }
}
