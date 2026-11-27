using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Background snapshots on a persistent node whose memory pressure is critical.</summary>
public sealed class SnapshotCriticalPressureTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";

    private static readonly TimeSpan SnapshotTimeout = TimeSpan.FromSeconds(30);

    /// <summary>A node held at critical pressure by its stored entries still publishes a snapshot once overwrites pass the operation threshold.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotAdvancesWhileMemoryIsCritical(CancellationToken cancellationToken)
    {
        const string nodeId = "node-critical";
        await using var cluster = await StartClusterAsync(
            nodeId,
            new IntegrationStartOptions
            {
                UsePersistence = true,
                ExtraScope = "snapshot-critical",
                MemoryPressureByNode = new Dictionary<string, PressureOptions>(StringComparer.Ordinal)
                {
                    [nodeId] = new() { MemoryAccountingEnabled = true, MaxEstimatedCacheBytes = 1 },
                },
                ServicesConfigure = ConfigureEagerSnapshots,
            },
            cancellationToken);
        var node = cluster[nodeId];
        var coordinator = node.GetRequiredService<Coordinator>();
        var ledger = node.GetRequiredService<Ledger>();
        var previousIndex = (await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).LastSnapshot?.Index ?? 0;
        var cache = node.GetCache<StoredValue>(CacheName);

        // An entry that bypasses admission, as a recovered one does, leaves the node critical; the same-size overwrite does not grow it.
        node.GetRequiredService<PhysicalCache<StoredValue>>().Set(new CacheKey(CacheName, "k"), new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText("v1"), Version = 1 }, cancellationToken);
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, "k", new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText("v2"), Version = 1 }, cancellationToken);
        await SnapshotPublicationWait.WaitAsync(ledger, coordinator, previousIndex, SnapshotTimeout, cancellationToken);

        _ = await Assert.That(node.GetRequiredService<ICriticalMemoryPressureReader>().IsCritical()).IsTrue();
    }

    private static void ConfigureEagerSnapshots(TestServiceCollection services)
    {
        _ = services.RemoveAll<TriggerOptions>().AddSingleton(
            new TriggerOptions { MinGapBetweenSnapshots = TimeSpan.Zero, SnapshotEveryNOps = 1 });
    }
}
