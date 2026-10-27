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
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>
/// While a member of the replica group that owns a key reports critical memory pressure, admission refuses the writes that grow memory,
/// because every copy holds the same entries; removals, shrinking writes, and retried writes whose outcome is recorded pass.
/// </summary>
[Immutable]
public sealed class ReplicaPressureAdmissionTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "k";
    private const string RejectionsInstrument = "squirix_memory_rejections_total";
    private const string Self = "node-a";

    private static readonly NodeCacheEntry<string> Large = new() { Value = new string('x', 1000), Version = 1 };
    private static readonly NodeCacheEntry<string> Small = new() { Value = "s", Version = 2 };

    private readonly Meter _testMeter = new("replica-pressure-admission");

    /// <summary>A write that grows memory is refused with the replica pressure reason while the group has a pressured member, though this node has room.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrowingWriteRefusedOnMemberPressure(CancellationToken cancellationToken)
    {
        using var sink = new MeasurementSink(_testMeter);
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        var accounting = CreateAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _) => true, null);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken).AsTask());

        _ = await Assert.That(sink.HasEvent(RejectionsInstrument, ("reason", PressureGate.ReplicaPressureReason), ("operation", AdmissionOperations.Set))).IsTrue();
        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(1L);
    }

    /// <summary>The group lookup is asked with the cache and the key of the write, so it reaches the group that owns the key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MemberLookupUsesKeyGroup(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var cache = Create(
            inner.Instance(),
            CreateAccounting(),
            static (cacheName, key) => string.Equals(cacheName, CacheName, StringComparison.Ordinal) && string.Equals(key, "pressured", StringComparison.Ordinal),
            null);

        await cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("op-1", CacheName, "pressured", Large, cancellationToken).AsTask());
    }

    /// <summary>A write passes while no member reports pressure, and a host that replicates nothing has no lookup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteWithoutMemberPressurePasses(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var instance = inner.Instance();
        var accounting = CreateAccounting();

        await Create(instance, accounting, static (_, _) => false, null).SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);
        await Create(instance, accounting, null, null).SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(0L);
    }

    /// <summary>A removal passes while a member is pressured: it frees memory on every copy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveIsAdmittedWhileMemberPressured(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.RemoveAsync("op-1", CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(new CacheRemoveResult<string>(true, "v")));
        var cache = Create(inner.Instance(), CreateAccounting(), static (_, _) => true, null);

        var removed = await cache.RemoveAsync("op-1", CacheName, Key, cancellationToken);

        _ = await Assert.That(removed.Removed).IsTrue();
    }

    /// <summary>A write that shrinks the entry passes while a member is pressured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShrinkingWriteAdmittedOnMemberPressure(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(Large));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var accounting = CreateAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _) => true, null);

        await cache.SetEntryAsync("op-1", CacheName, Key, Small, cancellationToken);

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(0L);
    }

    /// <summary>A retried write whose outcome is recorded is replayed while a member is pressured, never refused, and counts no rejection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordedWriteReplaysWhileMemberPressured(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var accounting = CreateAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _) => true, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal));

        await cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken);

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(0L);
    }

    /// <summary>When this node itself refuses the write, its own reason is reported, not the replica one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnPressureTakesPrecedence(CancellationToken cancellationToken)
    {
        using var sink = new MeasurementSink(_testMeter);
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        var cache = Create(inner.Instance(), CreateAccounting(), static (_, _) => true, null, 1);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("op-1", CacheName, Key, Large, cancellationToken).AsTask());

        _ = await Assert.That(sink.HasEvent(RejectionsInstrument, ("reason", PressureGate.EstimatedLimitReason))).IsTrue();
        _ = await Assert.That(sink.HasEvent(RejectionsInstrument, ("reason", PressureGate.ReplicaPressureReason))).IsFalse();
    }

    private static MemoryUsageAccounting CreateAccounting() => new(new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>()));

    private MemoryAdmissionCacheDecorator<string> Create(
        ILogicalNamespacedCache<string> inner,
        MemoryUsageAccounting accounting,
        Func<string, string, bool>? hasMemberPressure,
        Func<string, string, string, bool>? hasRecordedOutcome,
        long maxEstimatedCacheBytes = 10_000_000_000)
    {
        var options = Options.Create(
            new PressureOptions { MaxEstimatedCacheBytes = maxEstimatedCacheBytes, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options), accounting, Self, _testMeter);
        return new MemoryAdmissionCacheDecorator<string>(inner, gate, new CacheEntrySizeEstimator<string>(), hasRecordedOutcome, null, null, hasMemberPressure);
    }
}
