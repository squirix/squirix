using System;
using System.Collections.Frozen;
using System.Collections.Generic;
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

    /// <summary>A write of an object value with a relative expiration, sized by the production estimator, fits exactly and records what admission computed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ObjectValueWriteFitsExactlyThenIsRefused(CancellationToken cancellationToken)
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var entry = new NodeCacheEntry<object?>(new AdmissionDataPayload { Data = new string('y', 2000) }, 1, null, TimeSpan.FromMinutes(1));
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(new CacheKey(CacheName, Key), null, false, entry, false, estimator, out _);
        var (fitting, fittingCache) = Create(growth + 1, estimator);
        var (tight, tightCache) = Create(growth, estimator);

        await fitting.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, entry, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(SetAsync(tight, entry, cancellationToken));

        _ = await Assert.That(BytesOf(fittingCache)).IsEqualTo(growth);
        _ = await Assert.That(BytesOf(tightCache)).IsEqualTo(0L);
    }

    /// <summary>An update of a tagged entry is admitted exactly when the growth the cache records fits; with one byte less it is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TaggedUpdateFitsExactlyThenIsRefused(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<string>();
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "a-tenant-with-a-long-name", ["origin"] = "repro" }.ToFrozenDictionary();
        var seeded = new NodeCacheEntry<string>("a", 1, null, null, tags);
        var updated = new NodeCacheEntry<string>("a much longer value than the seeded one", 1, null, null, tags);
        var key = new CacheKey(CacheName, Key);
        var seededBytes = estimator.EstimateBytes(key, seeded, false);
        var growth = estimator.EstimateBytes(key, updated, false) - seededBytes;
        var (fitting, fittingCache) = Create(seededBytes + growth + 1, estimator);
        var (tight, tightCache) = Create(seededBytes + growth, estimator);
        await fittingCache.SetAsync(key, seeded, cancellationToken);
        await tightCache.SetAsync(key, seeded, cancellationToken);

        _ = await Assert.That(await fitting.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, updated.Value, cancellationToken)).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException, bool>(UpdateAsync(tight, updated.Value, cancellationToken));

        _ = await Assert.That(BytesOf(fittingCache)).IsEqualTo(seededBytes + growth);
        _ = await Assert.That(BytesOf(tightCache)).IsEqualTo(seededBytes);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static Task SetAsync<T>(MemoryAdmissionCacheDecorator<T> cache, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, entry, cancellationToken).AsTask();

    private static ValueTask<bool> UpdateAsync(MemoryAdmissionCacheDecorator<string> cache, string? value, CancellationToken cancellationToken) =>
        cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, value, cancellationToken);

    private (MemoryAdmissionCacheDecorator<T> Decorator, PhysicalCache<T> Cache) Create<T>(long maxEstimatedCacheBytes, ICacheEntrySizeEstimator<T> estimator)
    {
        var physical = new PhysicalCache<T>(TimeProvider.System, estimator);
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = maxEstimatedCacheBytes, HighPressureThresholdPercent = 99, CriticalPressureThresholdPercent = 100 });
        var gate = new PressureGate(new StateEvaluator(options), new MemoryUsageAccounting(physical), "node-a", _testMeter);
        return (new MemoryAdmissionCacheDecorator<T>(new ClientCache<T>(physical, physical), gate, estimator), physical);
    }
}
