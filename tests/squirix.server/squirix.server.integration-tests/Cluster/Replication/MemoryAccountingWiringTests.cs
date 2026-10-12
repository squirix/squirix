using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The memory accounting of a host reads the cache instance the local chain writes.</summary>
public sealed class MemoryAccountingWiringTests : NodeIntegrationTestBase
{
    private const string CacheName = "accounting-wiring";

    /// <summary>A write through the host's cache pipeline shows in the accounting, which reports exactly what the physical cache records.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PipelineWriteShowsInAccounting(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("n1", new IntegrationStartOptions { EnableReplication = false }, cancellationToken);
        var host = cluster["n1"];
        var accounting = host.GetRequiredService<IMemoryUsageAccounting>();
        var physical = host.GetRequiredService<PhysicalCache<object?>>();
        var before = accounting.ReadEstimatedBytes();
        var entry = new NodeCacheEntry<object?> { Value = "wired", Version = 1 };

        await host.GetRequiredService<ISquirixServerEntryCachePipeline<object?>>().SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, "k", entry, cancellationToken);

        var expected = host.GetRequiredService<ICacheEntrySizeEstimator<object?>>().EstimateBytes(new CacheKey(CacheName, "k"), entry, false);
        _ = await Assert.That(before).IsEqualTo(0L);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(expected);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1L);
        _ = await Assert.That((await physical.GetValueAsync(new CacheKey(CacheName, "k"), cancellationToken)).Found).IsTrue();
    }
}
