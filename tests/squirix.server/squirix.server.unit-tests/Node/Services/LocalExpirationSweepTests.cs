using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The local sweep removes the entries nobody reads once the local clock passes their deadline, and frees the room they held.</summary>
[Immutable]
public sealed class LocalExpirationSweepTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>The event id of a sweep stopped by the host.</summary>
    private const int SweepStoppedEventId = 3030;

    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    /// <summary>A pass releases the bytes of the expired entries and keeps those of the live one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepReleasesBytesOfExpiredKeys(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var estimator = new CacheEntrySizeEstimator<object?>();
        var physical = new PhysicalCache<object?>(clock, estimator);
        ILocalCacheStats stats = physical;
        await physical.SetAsync(new CacheKey(CacheName, "a"), new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await physical.SetAsync(new CacheKey(CacheName, "b"), new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await physical.SetAsync(new CacheKey(CacheName, "live"), new NodeCacheEntry<object?>("v"), cancellationToken);
        var liveBytes = estimator.EstimateBytes(new CacheKey(CacheName, "live"), new NodeCacheEntry<object?>("v"), false);
        clock.Advance(Ttl);
        using var sweep = new LocalExpirationSweepService(physical.ExpiredKeys, clock, new EventRecordingLogger());

        var removed = sweep.SweepOnce();

        _ = await Assert.That(removed).IsEqualTo(2);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(liveBytes);
    }

    /// <summary>A write that memory admission refused at the limit is admitted once the sweep removed the entries that filled the cache.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepMakesRoomForRefusedWrite(CancellationToken cancellationToken)
    {
        using var meter = new Meter("squirix-local-sweep-admission");
        var clock = new FakeTimeProvider(Start);
        var estimator = new CacheEntrySizeEstimator<object?>();
        var physical = new PhysicalCache<object?>(clock, estimator);
        var storedBytes = estimator.EstimateBytes(new CacheKey(CacheName, "a"), new NodeCacheEntry<object?>("v", 1, Start.UtcDateTime + Ttl), false);
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 4 * storedBytes });
        var gate = new PressureGate(new StateEvaluator(options.Value), new MemoryUsageAccounting(physical), "n1", meter);
        var admission = new MemoryAdmissionCacheDecorator<object?>(new ClientCache<object?>(physical, physical), gate, estimator);
        foreach (var key in new[] { "a", "b", "c" })
            await admission.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(
            admission.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "d", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken).AsTask());
        clock.Advance(Ttl);
        using var sweep = new LocalExpirationSweepService(physical.ExpiredKeys, clock, new EventRecordingLogger());
        _ = sweep.SweepOnce();
        await admission.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "d", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);

        ILocalCacheStats stats = physical;
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(storedBytes);
    }

    /// <summary>A sweep stopped by the host ends without a failure and logs one shutdown line.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepStopsCleanly(CancellationToken cancellationToken)
    {
        var clock = new DueTimerClock(LocalExpirationSweepService.Interval);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        var log = new EventRecordingLogger();
        using var sweep = new LocalExpirationSweepService(physical.ExpiredKeys, clock, log);

        await sweep.StartAsync(cancellationToken);

        // The host starts the loop in the background: stop it only once it waits for its first pass.
        _ = await Assert.That(await clock.TimerCreated.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken)).IsTrue();
        await sweep.StopAsync(cancellationToken);

        _ = await Assert.That(sweep.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(log.Count(SweepStoppedEventId)).IsEqualTo(1);
    }
}
