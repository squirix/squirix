using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies the hosted graphs for optional cache memory accounting.</summary>
public sealed class MemoryAccountingModeTests : NodeIntegrationTestBase
{
    /// <summary>Tests absence of accounting services and successful writes through the default graph.</summary>
    /// <param name="metrics">Whether unrelated operation metrics are enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DefaultGraphOmitsSizeAccounting(bool metrics, CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { MetricsEnabled = metrics };
        await using var cluster = await StartClusterAsync("n1", options, cancellationToken);
        var node = cluster["n1"];
        _ = await Assert.That(node.GetService<MemoryAdmissionCacheDecorator<StoredValue>>()).IsNull();
        _ = await Assert.That(node.GetService<ICacheEntrySizeEstimator<StoredValue>>()).IsNull();
        _ = await Assert.That(node.GetService<IMemoryUsageAccounting>()).IsNull();
        _ = await Assert.That(node.GetService<PressureGate>()).IsNull();
        _ = await Assert.That(node.GetService<ICriticalMemoryPressureReader>()).IsNull();
        _ = await Assert.That(node.GetService<MemoryPressureMetrics>()).IsNull();
        var cache = node.GetRequiredService<ILogicalNamespacedCache<StoredValue>>();
        await cache.SetEntryAsync(string.Empty, "users", "alice", BuildEntry("ok"), cancellationToken);
        var read = await cache.GetValueAsync("users", "alice", cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(node.GetRequiredService<ILocalCacheStats>().EstimatedBytes).IsEqualTo(0L);
    }

    /// <summary>A RAM node reports each refused growing write once and retains the preceding entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RamQuotaReportsOnlyRefusedGrowth(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions
        {
            MemoryPressureByNode = new Dictionary<string, PressureOptions>(StringComparer.Ordinal)
            {
                ["n1"] = new() { MemoryAccountingEnabled = true, MaxEstimatedCacheBytes = 4096 },
            },
        };
        await using var cluster = await StartClusterAsync("n1", options, cancellationToken);
        var node = cluster["n1"];
        var cache = node.GetRequiredService<ILogicalNamespacedCache<StoredValue>>();
        var stats = node.GetRequiredService<IMemoryUsageAccounting>();
        await cache.SetEntryAsync(string.Empty, "users", "alice", BuildEntry("small"), cancellationToken);
        var bytes = stats.ReadEstimatedBytes();
        _ = await Assert.That(bytes).IsGreaterThan(0L);
        var large = BuildEntry(new string('v', 4096));
        var write = cache.SetEntryAsync(string.Empty, "users", "alice", large, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(write, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(stats.ReadEstimatedBytes()).IsEqualTo(bytes);
        _ = await Assert.That(stats.ReadRejectedWriteCount()).IsEqualTo(1L);
        var read = await cache.GetValueAsync("users", "alice", cancellationToken);
        _ = await Assert.That(read.Value.Memory.Length).IsEqualTo(5);
        _ = await Assert.That(await cache.RemoveAsync(string.Empty, "users", "alice", cancellationToken)).IsTrue();
        _ = await Assert.That(stats.ReadEstimatedBytes()).IsEqualTo(0L);
    }
}
