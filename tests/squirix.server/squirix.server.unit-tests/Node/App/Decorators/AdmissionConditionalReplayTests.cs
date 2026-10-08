using System;
using System.Collections.Generic;
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
    private const string OtherKey = "other";
    private const string Self = "node-a";

    private static readonly NodeCacheEntry<string> Large = new() { Value = new string('x', 4096), Version = 1 };
    private static readonly NodeCacheEntry<string> Small = new() { Value = "s", Version = 2 };

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// The recorded-outcome lookup is asked with the written key, so it reaches the replica group that owns the key: under critical memory
    /// pressure the operation replays for the key whose group recorded it and is refused for a key whose group did not.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutcomeLookupUsesKeyGroup(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        var reads = 0;
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>())
                 .Callback((_, _, _) => ValueTask.FromResult(Interlocked.Increment(ref reads) == 1 ? null : entry));
        _ = inner.Setups.GetEntryAsync(CacheName, OtherKey, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var accounting = new MemoryUsageAccounting();
        var cache = Create(
            inner.Instance(),
            accounting,
            static (cacheName, key, operationId) => string.Equals(cacheName, CacheName, StringComparison.Ordinal)
                                                    && string.Equals(key, Key, StringComparison.Ordinal)
                                                    && string.Equals(operationId, "op-1", StringComparison.Ordinal),
            1);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("op-1", CacheName, OtherKey, entry, cancellationToken).AsTask());
        await cache.SetEntryAsync("op-1", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(1L);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
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
        var cache = Create(inner.Instance(), accounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1);

        await cache.SetEntryAsync("op-1", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(0L);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
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
        var updated = await Create(present.Instance(), updateAccounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1)
           .UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);
        var added = await Create(absent.Instance(), addAccounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1)
           .TryAddEntryAsync("op-1", CacheName, Key, stored, cancellationToken);

        _ = await Assert.That(updated).IsTrue();
        _ = await Assert.That(updateAccounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(updateAccounting.ReadRejectedWriteCount()).IsEqualTo(0L);
        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That(addAccounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(addAccounting.ReadRejectedWriteCount()).IsEqualTo(0L);
    }

    /// <summary>A retried add replayed after a remove accounts nothing, since the key holds nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayedAddAfterRemoveAccountsNothing(CancellationToken cancellationToken)
    {
        var store = new ReplayingStore();
        var accounting = new MemoryUsageAccounting();
        var cache = Create(store.CreateInner(), accounting, static (_, _, _) => false);

        var added = await cache.TryAddEntryAsync("op-1", CacheName, Key, Large, cancellationToken);
        _ = await cache.RemoveAsync("op-2", CacheName, Key, cancellationToken);
        var replayed = await cache.TryAddEntryAsync("op-1", CacheName, Key, Large, cancellationToken);

        _ = await Assert.That((added, replayed)).IsEqualTo((true, true));
        _ = await Assert.That((accounting.ReadEntryCount(), accounting.ReadEstimatedBytes())).IsEqualTo((0L, 0L));
    }

    /// <summary>A retried remove replayed after another write stored the key again keeps that entry accounted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayedRemoveKeepsStoredEntry(CancellationToken cancellationToken)
    {
        var store = new ReplayingStore();
        var accounting = new MemoryUsageAccounting();
        var cache = Create(store.CreateInner(), accounting, static (_, _, _) => false);
        await cache.SetEntryAsync("op-0", CacheName, Key, Large, cancellationToken);

        var removed = await cache.RemoveAsync("op-1", CacheName, Key, cancellationToken);
        await cache.SetEntryAsync("op-2", CacheName, Key, Small, cancellationToken);
        var replayed = await cache.RemoveAsync("op-1", CacheName, Key, cancellationToken);

        _ = await Assert.That((removed.Removed, replayed.Removed)).IsEqualTo((true, true));
        _ = await Assert.That((accounting.ReadEntryCount(), accounting.ReadEstimatedBytes())).IsEqualTo((1L, EstimateBytes(Small)));
    }

    /// <summary>A retried set replayed after a smaller entry replaced the key accounts the stored entry, not the larger one it sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayedSetAccountsStoredEntry(CancellationToken cancellationToken)
    {
        var store = new ReplayingStore();
        var accounting = new MemoryUsageAccounting();
        var cache = Create(store.CreateInner(), accounting, static (_, _, _) => false);

        await cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);
        await cache.SetEntryAsync("op-2", CacheName, Key, Small, cancellationToken);
        await cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);

        _ = await Assert.That(store.Stored).IsSameReferenceAs(Small);
        _ = await Assert.That((accounting.ReadEntryCount(), accounting.ReadEstimatedBytes())).IsEqualTo((1L, EstimateBytes(Small)));
    }

    /// <summary>A retried set replayed after a remove accounts nothing, since the key holds nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayedSetAfterRemoveAccountsNothing(CancellationToken cancellationToken)
    {
        var store = new ReplayingStore();
        var accounting = new MemoryUsageAccounting();
        var cache = Create(store.CreateInner(), accounting, static (_, _, _) => false);

        await cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);
        _ = await cache.RemoveAsync("op-2", CacheName, Key, cancellationToken);
        await cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);

        _ = await Assert.That(store.Stored).IsNull();
        _ = await Assert.That((accounting.ReadEntryCount(), accounting.ReadEstimatedBytes())).IsEqualTo((0L, 0L));
    }

    /// <summary>A retried update replayed after a smaller entry replaced the key accounts the stored entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplayedUpdateAccountsStoredEntry(CancellationToken cancellationToken)
    {
        var store = new ReplayingStore();
        var accounting = new MemoryUsageAccounting();
        var cache = Create(store.CreateInner(), accounting, static (_, _, _) => false);
        await cache.SetEntryAsync("op-0", CacheName, Key, Small, cancellationToken);

        _ = await cache.UpdateAsync("op-1", CacheName, Key, Large.Value, cancellationToken);
        await cache.SetEntryAsync("op-2", CacheName, Key, Small, cancellationToken);
        var replayed = await cache.UpdateAsync("op-1", CacheName, Key, Large.Value, cancellationToken);

        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That((accounting.ReadEntryCount(), accounting.ReadEstimatedBytes())).IsEqualTo((1L, EstimateBytes(Small)));
    }

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
        var cache = Create(inner.Instance(), accounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal));

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
        var cache = Create(inner.Instance(), accounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal));

        var updated = await cache.UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
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

    /// <summary>Without a recording inner pipeline a set accounts the entry it wrote without reading the key again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SingleCopySetReadsOnce(CancellationToken cancellationToken)
    {
        var store = new ReplayingStore();
        var accounting = new MemoryUsageAccounting();
        var cache = Create(store.CreateInner(), accounting, null);
        await cache.SetEntryAsync("op-0", CacheName, Key, Small, cancellationToken);
        var reads = store.Reads;

        await cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);

        _ = await Assert.That(store.Reads - reads).IsEqualTo(1);
        _ = await Assert.That((accounting.ReadEntryCount(), accounting.ReadEstimatedBytes())).IsEqualTo((1L, EstimateBytes(Large)));
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
        var cache = Create(inner.Instance(), accounting, static (_, _, _) => false);

        // The inner double has no add set up: a call would throw.
        var added = await cache.TryAddEntryAsync("op-2", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsFalse();
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(0);
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
        var cache = Create(inner.Instance(), accounting, static (_, _, _) => false, 1);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("op-2", CacheName, Key, entry, cancellationToken).AsTask());

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(1L);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static long EstimateBytes(NodeCacheEntry<string> entry) => new CacheEntrySizeEstimator<string>().EstimateBytes(new CacheKey(CacheName, Key), entry, false);

    private MemoryAdmissionCacheDecorator<string> Create(
        ILogicalNamespacedCache<string> inner,
        MemoryUsageAccounting accounting,
        Func<string, string, string, bool>? hasRecordedOutcome,
        long maxEstimatedCacheBytes = 10_000_000_000)
    {
        var options = Options.Create(
            new PressureOptions { MaxEstimatedCacheBytes = maxEstimatedCacheBytes, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options), accounting, Self, _testMeter);
        return new MemoryAdmissionCacheDecorator<string>(inner, gate, new CacheEntrySizeEstimator<string>(), accounting, hasRecordedOutcome);
    }

    /// <summary>
    /// One key of an inner pipeline that records outcomes by operation id: the first attempt of an operation applies, a retry replays the
    /// recorded answer and writes nothing.
    /// </summary>
    [Mutable]
    private sealed class ReplayingStore
    {
        private readonly Dictionary<string, object> _outcomes = [with(StringComparer.Ordinal)];

        internal int Reads { get; private set; }

        internal NodeCacheEntry<string>? Stored { get; private set; }

        internal ILogicalNamespacedCache<string> CreateInner()
        {
            var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
            _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).Callback((_, _, _) =>
            {
                Reads++;
                return ValueTask.FromResult(Stored);
            });
            _ = inner.Setups.SetEntryAsync(Arg.Any<string>(), CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                     .Callback((operationId, _, _, entry, _) =>
                      {
                          _ = Decide(operationId, () => Stored = entry);
                          return ValueTask.CompletedTask;
                      });
            _ = inner.Setups.RemoveAsync(Arg.Any<string>(), CacheName, Key, Arg.Any<CancellationToken>()).Callback((operationId, _, _, _) => ValueTask.FromResult(
                Decide(
                    operationId,
                    () =>
                    {
                        var result = new CacheRemoveResult<string>(Stored != null, Stored?.Value);
                        Stored = null;
                        return result;
                    })));
            _ = inner.Setups.TryAddEntryAsync(Arg.Any<string>(), CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                     .Callback((operationId, _, _, entry, _) => ValueTask.FromResult(
                          Decide(
                              operationId,
                              () =>
                              {
                                  if (Stored != null)
                                      return false;

                                  Stored = entry;
                                  return true;
                              })));
            _ = inner.Setups.UpdateAsync(Arg.Any<string>(), CacheName, Key, Arg.Any<string?>(), Arg.Any<CancellationToken>()).Callback((operationId, _, _, value, _) =>
                ValueTask.FromResult(
                    Decide(
                        operationId,
                        () =>
                        {
                            if (Stored == null)
                                return false;

                            Stored = new NodeCacheEntry<string> { Value = value, Version = Stored.Version, ExpiresUtc = Stored.ExpiresUtc, Expiration = Stored.Expiration };
                            return true;
                        })));
            return inner.Instance();
        }

        private TOutcome Decide<TOutcome>(string operationId, Func<TOutcome> apply)
            where TOutcome : notnull
        {
            if (_outcomes.TryGetValue(operationId, out var recorded) && recorded is TOutcome replayed)
                return replayed;

            var outcome = apply();
            _outcomes[operationId] = outcome;
            return outcome;
        }
    }
}
