using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Unit tests for <see cref="PhysicalCache{T}" /> update/expiration races.</summary>
[Immutable]
public sealed class PhysicalCacheTests : ServerUnitTestBase
{
    /// <summary>Update on an expired key removes it and returns false.</summary>
    [Fact]
    public async Task UpdateAsyncRemovesExpiredEntryAndReturnsFalse()
    {
        var time = new FakeTimeProvider();
        await using var cache = new PhysicalCache<string>(time);
        var key = new CacheKey("ns", "expired");
        await cache.SetAsync(
            key,
            new NodeCacheEntry<string>
            {
                Value = "old",
                Version = 1,
                ExpiresUtc = time.GetUtcNow().UtcDateTime.AddMinutes(1),
            },
            DefaultCancellationToken);

        time.Advance(TimeSpan.FromMinutes(2));

        Assert.False(await cache.UpdateAsync(key, "new", DefaultCancellationToken));
        Assert.Null(await cache.GetEntryAsync(key, DefaultCancellationToken));
    }

    /// <summary>Update replaces a live value and reports success.</summary>
    [Fact]
    public async Task UpdateAsyncReplacesLiveValue()
    {
        await using var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "live");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "a", Version = 1 }, DefaultCancellationToken);

        Assert.True(await cache.UpdateAsync(key, "b", DefaultCancellationToken));
        var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
        Assert.Equal("b", entry!.Value);
    }

    /// <summary>Update with the same live value is a successful no-op.</summary>
    [Fact]
    public async Task UpdateAsyncSameValueIsNoOpSuccess()
    {
        await using var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "same");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "same", Version = 1 }, DefaultCancellationToken);

        Assert.True(await cache.UpdateAsync(key, "same", DefaultCancellationToken));
    }

    /// <summary>Update on a missing key returns false.</summary>
    [Fact]
    public async Task UpdateAsyncMissingKeyReturnsFalse()
    {
        await using var cache = new PhysicalCache<string>();
        Assert.False(await cache.UpdateAsync(new CacheKey("ns", "missing"), "v", DefaultCancellationToken));
    }
}
