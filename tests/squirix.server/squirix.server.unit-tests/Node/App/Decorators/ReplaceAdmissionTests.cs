using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Verifies memory admission for conditional replacements decided by the single-copy journal.</summary>
public sealed class ReplaceAdmissionTests : IsolatedStorageTestBase
{
    /// <summary>A queued replacement uses the size left by the preceding durable replacement.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task QueuedReplacementChecksGrowthAfterShrink(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal, replacementAdmission: static _ => CriticalAdmission());
        const string cacheName = FusedWriteHarness.CacheName;
        const string key = FusedWriteHarness.Key;
        var shrink = Entry("small");
        var growth = Entry(new string('b', 512));
        await harness.Cache.SetEntryAsync(string.Empty, cacheName, key, Entry(new string('a', 1024)), cancellationToken);

        journal.Writer.Flush.Arm();
        var shrinking = harness.Cache.TryReplaceEntryAsync(string.Empty, cacheName, key, shrink, cancellationToken).AsTask();
        try
        {
            await journal.Writer.Flush.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            var growing = harness.Cache.TryReplaceEntryAsync(string.Empty, cacheName, key, growth, cancellationToken).AsTask();
            _ = await Assert.That(growing.IsCompleted).IsFalse();
            journal.Writer.Flush.Release();

            _ = await Assert.That(await shrinking).IsTrue();
            _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(
                growing, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.Flush.Release();
        }

        var current = await harness.Cache.GetEntryAsync(cacheName, key, cancellationToken);
        _ = await Assert.That(current!.Value.Memory.Span.SequenceEqual(shrink.Value.Memory.Span)).IsTrue();
        _ = await Assert.That(harness.ReadFrames(cancellationToken).Count).IsEqualTo(2);
    }

    /// <summary>A missing key reports a failed condition without being admitted or recorded as a write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingReplacementSkipsJournal(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal, replacementAdmission: static _ => CriticalAdmission());
        var result = await harness.Cache.TryReplaceEntryAsync(
            string.Empty, FusedWriteHarness.CacheName, FusedWriteHarness.Key, Entry(new string('b', 512)), cancellationToken);

        _ = await Assert.That(result).IsFalse();
        _ = await Assert.That(journal.Journal.AppendedOps).IsEqualTo(0);
    }

    /// <summary>A pending replacement counts against admission for another key before its journal flush finishes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task PendingReplacementReservesJournalBudget(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal, replacementAdmission: static physical => new ReplacementAdmission<StoredValue>(
            new PressureGate(
                new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 2_000, CriticalPressureThresholdPercent = 100 }),
                new MemoryUsageAccounting(physical),
                "node",
                null),
            new CacheEntrySizeEstimator<StoredValue>()));
        const string cacheName = FusedWriteHarness.CacheName;
        const string firstKey = "a";
        const string secondKey = "b";
        var initial = Entry("small");
        var larger = Entry(new string('v', 1200));
        await harness.Cache.SetEntryAsync(string.Empty, cacheName, firstKey, initial, cancellationToken);
        await harness.Cache.SetEntryAsync(string.Empty, cacheName, secondKey, initial, cancellationToken);

        journal.Writer.Flush.Arm();
        var first = harness.Cache.TryReplaceEntryAsync(string.Empty, cacheName, firstKey, larger, cancellationToken).AsTask();
        try
        {
            await journal.Writer.Flush.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            var second = harness.Cache.TryReplaceEntryAsync(string.Empty, cacheName, secondKey, larger, cancellationToken).AsTask();
            _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(
                second, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            _ = await Assert.That(journal.Journal.AppendedOps).IsEqualTo(3);
        }
        finally
        {
            journal.Writer.Flush.Release();
        }

        _ = await Assert.That(await first).IsTrue();
        var unchanged = await harness.Cache.GetEntryAsync(cacheName, secondKey, cancellationToken);
        _ = await Assert.That(unchanged!.Value.Memory.Span.SequenceEqual(initial.Value.Memory.Span)).IsTrue();
        _ = await Assert.That(await harness.Cache.TryReplaceEntryAsync(string.Empty, cacheName, firstKey, initial, cancellationToken)).IsTrue();
        _ = await Assert.That(await harness.Cache.TryReplaceEntryAsync(string.Empty, cacheName, secondKey, larger, cancellationToken)).IsTrue();
        _ = await Assert.That(harness.ReadFrames(cancellationToken).Count).IsEqualTo(5);
    }

    private static NodeCacheEntry<StoredValue> Entry(string value) => new() { Value = StoredValue.FromText(value) };

    private static ReplacementAdmission<StoredValue> CriticalAdmission()
    {
        var accounting = IMemoryUsageAccounting.Mock(MockBehavior.Strict);
        _ = accounting.ReadEstimatedBytes().Returns(9600);
        _ = accounting.RecordAdmissionRejection();
        var pressure = new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 10_000, CriticalPressureThresholdPercent = 95 });
        return new ReplacementAdmission<StoredValue>(new PressureGate(pressure, accounting.Object, "node", null), new CacheEntrySizeEstimator<StoredValue>());
    }
}
