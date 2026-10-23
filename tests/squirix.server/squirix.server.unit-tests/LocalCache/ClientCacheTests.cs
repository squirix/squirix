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
    private const int Calls = 1000;

    /// <summary>A value hit allocates nothing: the key the adapter builds for the lookup is a value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ValueHitAllocatesNothing(CancellationToken cancellationToken)
    {
        var physical = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var cache = new ClientCache<string>(physical, physical);
        await cache.SetEntryAsync("op", CacheName, Key, new NodeCacheEntry<string>("v"), cancellationToken);
        for (var i = 0; i < Calls; i++)
            _ = await cache.GetValueAsync(CacheName, Key, cancellationToken);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Calls; i++)
            _ = await cache.GetValueAsync(CacheName, Key, cancellationToken);

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        _ = await Assert.That(allocated).IsEqualTo(0L);
        _ = await Assert.That(await cache.GetValueAsync(CacheName, Key, cancellationToken)).IsEqualTo(new NodeCacheValueResult<string>(true, "v"));
    }
}
