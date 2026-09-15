using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Expiration-preserving CRUD integration tests on a controllable clock.</summary>
[Immutable]
public sealed class CrudExpiryTests : ClockTestBase
{
    /// <summary>Verifies TryAddAsync with options preserves expiration metadata through the public API.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddAbsentPreservesExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("missing-try-add-entry-expiration", cancellationToken);
        var added = await cache.TryAddAsync("k", "v", Expiry.In(TimeSpan.FromSeconds(10)), cancellationToken);
        _ = await Assert.That(added).IsTrue();
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Value > TimeSpan.Zero).IsTrue();
    }

    /// <summary>Verifies AddAsync with options preserves expiration metadata through the public API.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddPreservesExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("missing-add-entry-expiration", cancellationToken);
        await cache.AddAsync("k", "v", Expiry.In(TimeSpan.FromSeconds(10)), cancellationToken);
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Value > TimeSpan.Zero).IsTrue();
    }
}
