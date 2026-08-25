using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Null and missing-value CRUD integration tests on a controllable clock.</summary>
[Immutable]
public sealed class CrudNullValueTests : ClockTestBase
{
    /// <summary>Verifies GetEntryAsync returns entry or null when missing or expired.</summary>
    [Fact]
    public async Task GetEntryAsyncReturnsEntryOrNull()
    {
        var cache = await Client.GetCacheAsync<string>("get-entry-async", DefaultCancellationToken);
        Assert.False((await cache.GetEntryAsync("missing", DefaultCancellationToken)).Found);

        // Generous expiration so expiry does not overtake the immediate read on a loaded CI runner.
        var expiration = TimeSpan.FromSeconds(2);
        await cache.SetAsync("k1", "v1", Expiry.In(expiration), DefaultCancellationToken);
        var e = await cache.GetEntryAsync("k1", DefaultCancellationToken);
        Assert.True(e.Found);
        Assert.Equal("v1", e.Value);
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        Assert.False((await cache.GetEntryAsync("k1", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies GetEntry returns entry with metadata or null when missing or expired.</summary>
    [Fact]
    public async Task GetEntryReturnsEntryOrNull()
    {
        var cache = await Client.GetCacheAsync<string>("get-entry", DefaultCancellationToken);
        Assert.False((await cache.GetEntryAsync("missing", DefaultCancellationToken)).Found);

        // Generous expiration so the immediate read is not overtaken by expiry on a loaded CI runner.
        var expiration = TimeSpan.FromSeconds(2);
        await cache.SetAsync("k1", "v1", Expiry.In(expiration), DefaultCancellationToken);
        var e = await cache.GetEntryAsync("k1", DefaultCancellationToken);
        Assert.True(e.Found);
        Assert.Equal("v1", e.Value);
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        Assert.False((await cache.GetEntryAsync("k1", DefaultCancellationToken)).Found);
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
    public async Task RemoveAsyncStoredNullReportsRemoved()
    {
        var cache = await Client.GetCacheAsync<object?>("try-remove-null-stored-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);
        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);
        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    [Fact]
    public async Task RemoveReturnsRemovedForStoredNullEntry()
    {
        var cache = await Client.GetCacheAsync<object?>("try-remove-null-entry-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);
        var result = await cache.RemoveAsync("k", DefaultCancellationToken);
        Assert.True(result);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    [Fact]
    public async Task RemoveReturnsRemovedForStoredNullValue()
    {
        var cache = await Client.GetCacheAsync<string?>("try-remove-null-value-public-extra", DefaultCancellationToken);
        await cache.SetAsync("k", null, cancellationToken: DefaultCancellationToken);
        var removed = await cache.RemoveAsync("k", DefaultCancellationToken);
        Assert.True(removed);
        Assert.False((await cache.GetValueAsync("k", DefaultCancellationToken)).Found);
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
