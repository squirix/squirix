using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
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
        await using var cluster = await StartClusterAsync(
            "node-critical",
            new IntegrationStartOptions
            {
                UsePersistence = true,
                ExtraScope = "snapshot-critical",
                ServicesConfigure = ForceCriticalEagerSnapshots,
            },
            cancellationToken);
        var node = cluster["node-critical"];
        var coordinator = node.GetRequiredService<Coordinator>();
        var ledger = node.GetRequiredService<Ledger>();
        var previousIndex = (await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).LastSnapshot?.Index ?? 0;
        var cache = node.GetCache<object?>(CacheName);

        // An entry that bypasses admission, as a recovered one does, leaves the node critical; the same-size overwrite does not grow it.
        node.GetRequiredService<PhysicalCache<object?>>().Set(new CacheKey(CacheName, "k"), new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, "k", new NodeCacheEntry<object?> { Value = "v2", Version = 1 }, cancellationToken);
        await SnapshotPublicationWait.WaitAsync(ledger, coordinator, previousIndex, SnapshotTimeout, cancellationToken);

        _ = await Assert.That(node.GetRequiredService<ICriticalMemoryPressureReader>().IsCritical()).IsTrue();
    }

    private static void ForceCriticalEagerSnapshots(IServiceCollection services)
    {
        _ = services.RemoveAll<IMemoryPressureStateEvaluator>().AddSingleton<IMemoryPressureStateEvaluator>(
            new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 1 }));
        _ = services.RemoveAll<TriggerOptions>().AddSingleton(
            new TriggerOptions { MinGapBetweenSnapshots = TimeSpan.Zero, SnapshotEveryNOps = 1 });
    }
}
