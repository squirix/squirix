using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Expiration-preserving CRUD integration tests on a controllable clock.</summary>
[Immutable]
public sealed class CrudExpiryTests : ClockTestBase
{
    /// <summary>Verifies AddAsync with options preserves expiration metadata through the public API.</summary>
    [Fact]
    public async Task AddAsyncPreservesExpiryThroughPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("missing-add-entry-expiration", DefaultCancellationToken);
        await cache.AddAsync("k", "v", Expiry.In(TimeSpan.FromSeconds(10)), DefaultCancellationToken);
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Value > TimeSpan.Zero);
    }

    /// <summary>Verifies TryAddAsync with options preserves expiration metadata through the public API.</summary>
    [Fact]
    public async Task TryAddAsyncPreservesExpiryPublicApi()
    {
        var cache = await Client.GetCacheAsync<string>("missing-try-add-entry-expiration", DefaultCancellationToken);
        var added = await cache.TryAddAsync("k", "v", Expiry.In(TimeSpan.FromSeconds(10)), DefaultCancellationToken);
        Assert.True(added);
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        Assert.True(expiration.Value > TimeSpan.Zero);
    }
}
