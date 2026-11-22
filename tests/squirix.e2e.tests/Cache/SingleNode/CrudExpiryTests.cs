using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Expiration-preserving CRUD integration tests on a controllable clock.</summary>
[Immutable]
public sealed class CrudExpiryTests : ClockTestBase
{
    /// <summary>Verifies both add operations store the requested relative expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddOperationsPreserveRelativeExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("add-relative-expiry", cancellationToken);
        var expiry = Expiry.In(TimeSpan.FromSeconds(10));
        var added = await cache.TryAddAsync("try-add", "v", expiry, cancellationToken);
        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That((await cache.GetExpirationAsync("try-add", cancellationToken)).Value > TimeSpan.Zero).IsTrue();

        await cache.AddAsync("add", "v", expiry, cancellationToken);
        _ = await Assert.That((await cache.GetExpirationAsync("add", cancellationToken)).Value > TimeSpan.Zero).IsTrue();
    }
}
