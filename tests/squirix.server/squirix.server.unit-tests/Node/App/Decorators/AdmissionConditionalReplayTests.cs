using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>
/// Admission hands a conditional write it could answer without writing to an inner pipeline that records outcomes by operation id, so a
/// retry of a committed write replays its outcome instead of the answer the current state suggests.
/// </summary>
[Immutable]
public sealed class AdmissionConditionalReplayTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "k";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <summary>A retried add over the key it added replays the recorded <see langword="true" /> and keeps the entry accounted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedAddReplaysRecordedOutcome(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        _ = inner.Setups.TryAddEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal));

        var added = await cache.TryAddEntryAsync("op-1", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
    }

    /// <summary>A retried update of a key removed since replays the recorded <see langword="true" /> without accounting an entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedUpdateReplaysRecordedOutcome(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.UpdateAsync("op-1", CacheName, Key, "v2", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal));

        var updated = await cache.UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(0);
    }

    /// <summary>An add over a present key whose operation has no recorded outcome is refused here, without a log record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnrecordedAddAnsweredHere(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _) => false);

        // The inner double has no add set up: a call would throw.
        var added = await cache.TryAddEntryAsync("op-2", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsFalse();
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(0);
    }

    /// <summary>Without a recording inner pipeline an add over a present key is answered here, without calling the inner pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SingleCopyAddAnsweredHere(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting, null);

        // The inner double has no add set up: a call would throw.
        var added = await cache.TryAddEntryAsync("op-3", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsFalse();
    }

    /// <summary>
    /// Under critical memory pressure a retried set whose outcome is recorded replays it instead of being refused, counts no rejection and
    /// accounts the entry the inner cache holds after the replay.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordedSetReplaysUnderPressure(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        var reads = 0;

        // The key is absent when admission reads it and holds the replayed entry when the replay is accounted.
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>())
                 .Callback((_, _, _) => ValueTask.FromResult(Interlocked.Increment(ref reads) == 1 ? null : entry));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1);

        await cache.SetEntryAsync("op-1", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(0L);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
    }

    /// <summary>Under critical memory pressure a set without a recorded outcome is refused before the inner pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnrecordedSetRefusedUnderPressure(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _) => false, 1);

        // The inner double has no set set up: a call would throw something else.
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("op-2", CacheName, Key, entry, cancellationToken).AsTask());

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(1L);
    }

    /// <summary>Under critical memory pressure retried updates and adds whose outcomes are recorded replay them and account the stored entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordedUpdateAndAddReplayUnderPressure(CancellationToken cancellationToken)
    {
        var stored = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var present = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = present.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(stored));
        _ = present.Setups.UpdateAsync("op-1", CacheName, Key, "v2", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var absent = new ILogicalNamespacedCacheCreateExpectations<string>();
        var reads = 0;

        // The key is absent when admission reads it and holds the replayed entry when the replay is accounted.
        _ = absent.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>())
                  .Callback((_, _, _) => ValueTask.FromResult(Interlocked.Increment(ref reads) == 1 ? null : stored));
        _ = absent.Setups.TryAddEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var updateAccounting = new MemoryUsageAccounting();
        var addAccounting = new MemoryUsageAccounting();
        var updated = await Create(present.Instance(), updateAccounting, static (_, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1).UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);
        var added = await Create(absent.Instance(), addAccounting, static (_, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1).TryAddEntryAsync("op-1", CacheName, Key, stored, cancellationToken);

        _ = await Assert.That(updated).IsTrue();
        _ = await Assert.That(updateAccounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(updateAccounting.ReadRejectedWriteCount()).IsEqualTo(0L);
        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That(addAccounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(addAccounting.ReadRejectedWriteCount()).IsEqualTo(0L);
    }

    /// <summary>A retried set replayed after a smaller entry replaced the key accounts the stored entry, not the larger one it sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayedSetAccountsStoredEntry(CancellationToken cancellationToken)
    {
        var large = new NodeCacheEntry<string> { Value = new string('x', 4096), Version = 1 };
        var small = new NodeCacheEntry<string> { Value = "s", Version = 2 };
        NodeCacheEntry<string>? stored = null;
        var firstAttempts = 0;
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).Callback((_, _, _) => ValueTask.FromResult(stored));

        // Every set stores its entry, except the retry of op-1, which replays the recorded outcome and writes nothing.
        _ = inner.Setups.SetEntryAsync(Arg.Any<string>(), CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                 .Callback((operationId, _, _, entry, _) =>
                 {
                     if (!string.Equals(operationId, "op-1", StringComparison.Ordinal) || firstAttempts++ == 0)
                         stored = entry;

                     return ValueTask.CompletedTask;
                 });
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _) => false);

        await cache.SetEntryAsync("op-1", CacheName, Key, large, cancellationToken);
        await cache.SetEntryAsync("op-2", CacheName, Key, small, cancellationToken);
        await cache.SetEntryAsync("op-1", CacheName, Key, large, cancellationToken);

        _ = await Assert.That(stored).IsSameReferenceAs(small);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(new CacheEntrySizeEstimator<string>().EstimateBytes(new CacheKey(CacheName, Key), small, false));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private MemoryAdmissionCacheDecorator<string> Create(
        ILogicalNamespacedCache<string> inner,
        MemoryUsageAccounting accounting,
        Func<string, string, bool>? hasRecordedOutcome,
        long maxEstimatedCacheBytes = 10_000_000_000)
    {
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = maxEstimatedCacheBytes, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options), accounting, Self, _testMeter);
        return new MemoryAdmissionCacheDecorator<string>(inner, gate, new CacheEntrySizeEstimator<string>(), accounting, hasRecordedOutcome);
    }
}
