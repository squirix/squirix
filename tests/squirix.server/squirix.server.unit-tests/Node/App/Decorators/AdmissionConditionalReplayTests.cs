using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
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
        var cache = Create(inner.Instance(), accounting);

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
        var cache = Create(inner.Instance(), accounting);

        var updated = await cache.UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(0);
    }

    /// <summary>An add refused by the recording inner pipeline is answered <see langword="false" /> and accounts nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedAddAccountsNothing(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        _ = inner.Setups.TryAddEntryAsync("op-2", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(false));
        var accounting = new MemoryUsageAccounting();
        var cache = Create(inner.Instance(), accounting);

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
        var cache = Create(inner.Instance(), accounting, false);

        // The inner double has no add set up: a call would throw.
        var added = await cache.TryAddEntryAsync("op-3", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsFalse();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private MemoryAdmissionCacheDecorator<string> Create(ILogicalNamespacedCache<string> inner, MemoryUsageAccounting accounting, bool innerRecordsOutcomes = true)
    {
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options), accounting, Self, _testMeter);
        return new MemoryAdmissionCacheDecorator<string>(inner, gate, new CacheEntrySizeEstimator<string>(), accounting, RocksDoubles.CreateOwnerLocator(Self), Self, innerRecordsOutcomes);
    }
}
