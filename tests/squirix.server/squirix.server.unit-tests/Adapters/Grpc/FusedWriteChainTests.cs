using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A hosted write through the production decorator chain waits for one flush, and a remove answers what it saw when it was prepared.</summary>
[Immutable]
public sealed class FusedWriteChainTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private readonly Meter _testMeter = new("test-fused-write-chain");

    /// <summary>A set of an absent key through memory admission and payload preparation flushes once and records its outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetThroughHostChainFlushesOnce(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal, null, CreateChain);
        var request = FusedWriteHarness.Set(FusedWriteHarness.OpId(1), "a");

        var flushesBefore = harness.FlushCount;
        _ = await harness.Adapter.SetEntry(request, new TestServerCallContext());
        var flushes = harness.FlushCount - flushesBefore;
        await journal.ShutdownAsync();
        var frames = harness.ReadFrames(cancellationToken);

        _ = await Assert.That(flushes).IsEqualTo(1);
        _ = await Assert.That(frames).Count().IsEqualTo(2);
        _ = await Assert.That(frames[1].Operation).IsEqualTo(JournalOperationKind.IdempotencyOutcome);
    }

    /// <summary>A remove of an entry that is live when it is prepared and expires while its flush is parked answers what it removed, and memory matches the journal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiringWhileParkedReportsRemoved(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        _ = await harness.Adapter.SetEntry(FusedWriteHarness.Set(FusedWriteHarness.OpId(1), "a", ttl: TimeSpan.FromSeconds(1)), new TestServerCallContext());
        journal.Writer.Flush.Arm();

        var remove = harness.Adapter.Remove(FusedWriteHarness.Remove(FusedWriteHarness.OpId(2)), new TestServerCallContext());
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        harness.Clock.Advance(TimeSpan.FromSeconds(2));
        journal.Writer.Flush.Release();
        var response = await remove.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await journal.ShutdownAsync();
        var (memory, store) = await harness.RecoverAsync(cancellationToken);

        _ = await Assert.That(response.Removed).IsTrue();
        _ = await Assert.That(response.PreviousValue).IsNotNull();
        _ = await Assert.That(await harness.Physical.GetEntryAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken)).IsNull();
        _ = await Assert.That(await memory.GetEntryAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken)).IsNull();
        _ = await Assert.That(store.RecordCount).IsEqualTo(2);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private MemoryAdmissionCacheDecorator<object?> CreateChain(JournalLoggingCacheDecorator<object?> journal)
    {
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var accounting = new MemoryUsageAccounting();
        var gate = new PressureGate(new StateEvaluator(options), accounting, "node-a", _testMeter);
        return new MemoryAdmissionCacheDecorator<object?>(new JournalPayloadPrepareCacheDecorator<object?>(journal), gate, new CacheEntrySizeEstimator<object?>(), accounting);
    }
}
