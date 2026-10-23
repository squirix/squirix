using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Tests the adapter from cache name and key to the physical cache.</summary>
[Immutable]
public sealed class ClientCacheTests : ServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "item";

    /// <summary>A value hit through the adapter returns the value stored under the cache name and key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ValueHitReturnsStoredValue(CancellationToken cancellationToken)
    {
        var physical = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var cache = new ClientCache<string>(physical, physical);
        await cache.SetEntryAsync("op", CacheName, Key, new NodeCacheEntry<string>("v"), cancellationToken);

        _ = await Assert.That(await cache.GetValueAsync(CacheName, Key, cancellationToken)).IsEqualTo(new NodeCacheValueResult<string>(true, "v"));
    }
}
