using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>The growth admission computes for a write with a relative expiration equals the bytes the cache records for the stored entry.</summary>
[Immutable]
public sealed class AdmissionDeadlineAgreementTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "k";

    private readonly Meter _testMeter = new("test");

    /// <summary>A write with a relative expiration that fits the limit exactly is admitted and records what admission computed; with one byte less it is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpirationWriteFitsExactlyThenIsRefused(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<string>();
        var entry = new NodeCacheEntry<string>("v", 1, null, TimeSpan.FromMinutes(1));
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(new CacheKey(CacheName, Key), null, false, entry, false, estimator, out _);
        var (fitting, fittingCache) = Create(growth + 1, estimator);
        var (tight, tightCache) = Create(growth, estimator);

        await fitting.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, entry, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(SetAsync(tight, entry, cancellationToken));

        _ = await Assert.That(BytesOf(fittingCache)).IsEqualTo(growth);
        _ = await Assert.That(BytesOf(tightCache)).IsEqualTo(0L);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static Task SetAsync(MemoryAdmissionCacheDecorator<string> cache, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
        cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, entry, cancellationToken).AsTask();

    private (MemoryAdmissionCacheDecorator<string> Decorator, PhysicalCache<string> Cache) Create(long maxEstimatedCacheBytes, CacheEntrySizeEstimator<string> estimator)
    {
        var physical = new PhysicalCache<string>(TimeProvider.System, estimator);
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = maxEstimatedCacheBytes, HighPressureThresholdPercent = 99, CriticalPressureThresholdPercent = 100 });
        var gate = new PressureGate(new StateEvaluator(options), new MemoryUsageAccounting(physical), "node-a", _testMeter);
        return (new MemoryAdmissionCacheDecorator<string>(new ClientCache<string>(physical, physical), gate, estimator), physical);
    }
}
