using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>
/// The fenced read of memory admission runs only where admission would refuse a conditional write, and the outcome the fence itself
/// recorded for the retried write is replayed after it.
/// </summary>
[Immutable]
public sealed class AdmissionFencedReadTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "k";
    private const string OperationId = "op-1";

    private static readonly NodeCacheEntry<string> Stored = new() { Value = "s", Version = 2 };

    private readonly Meter _testMeter = new("test");

    /// <summary>An add refused by the fenced read replays the outcome the fence recorded for the same operation instead of answering false.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutcomeRecordedByFenceReplaysAdd(CancellationToken cancellationToken)
    {
        var recorded = false;
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(Stored));
        _ = inner.Setups.TryAddEntryAsync(OperationId, CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(
            inner.Instance(),
            (_, _, _) => Volatile.Read(ref recorded),
            (_, _, _) =>
            {
                Volatile.Write(ref recorded, true);
                return ValueTask.FromResult(new NodeCacheEntryPeek<string>(Stored, false));
            });

        var added = await cache.TryAddEntryAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(added).IsTrue();
    }

    /// <summary>An update refused by the fenced read replays the outcome the fence recorded for the same operation instead of answering false.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutcomeRecordedByFenceReplaysUpdate(CancellationToken cancellationToken)
    {
        var recorded = false;
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.UpdateAsync(OperationId, CacheName, Key, "v2", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(
            inner.Instance(),
            (_, _, _) => Volatile.Read(ref recorded),
            (_, _, _) =>
            {
                Volatile.Write(ref recorded, true);
                return ValueTask.FromResult(default(NodeCacheEntryPeek<string>));
            });

        var updated = await cache.UpdateAsync(OperationId, CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
    }

    /// <summary>An add of an absent key is admitted and written without a fenced read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdmittedAddSkipsFence(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.TryAddEntryAsync(OperationId, CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), static (_, _, _) => false, static (_, _, _) => throw new InvalidOperationException("The fence must not run."));

        var added = await cache.TryAddEntryAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(added).IsTrue();
    }

    /// <summary>An update of a present key is admitted and written without a fenced read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdmittedUpdateSkipsFence(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(Stored));
        _ = inner.Setups.UpdateAsync(OperationId, CacheName, Key, "v2", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), static (_, _, _) => false, static (_, _, _) => throw new InvalidOperationException("The fence must not run."));

        var updated = await cache.UpdateAsync(OperationId, CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
    }

    /// <summary>A fenced read that confirms the refusal, with no outcome recorded, answers false without calling the inner pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConfirmedRefusalAnswersFalse(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(Stored));
        var cache = Create(inner.Instance(), static (_, _, _) => false, static (_, _, _) => ValueTask.FromResult(new NodeCacheEntryPeek<string>(Stored, false)));

        // The inner double has no add set up: a call would throw.
        var added = await cache.TryAddEntryAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(added).IsFalse();
    }

    /// <summary>An update whose fenced read finds an entry past its deadline goes to the inner pipeline, which commits the expiry before it answers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingExpiryHandsUpdateToInner(CancellationToken cancellationToken)
    {
        var reached = false;
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.UpdateAsync(OperationId, CacheName, Key, "v2", Arg.Any<CancellationToken>()).Callback((_, _, _, _, _) =>
        {
            Volatile.Write(ref reached, true);
            return ValueTask.FromResult(false);
        });
        var cache = Create(inner.Instance(), static (_, _, _) => false, static (_, _, _) => ValueTask.FromResult(new NodeCacheEntryPeek<string>(Stored, true)));

        var updated = await cache.UpdateAsync(OperationId, CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsFalse();
        _ = await Assert.That(Volatile.Read(ref reached)).IsTrue();
    }

    /// <summary>An update whose fenced read finds the key absent, with no outcome recorded, answers false without calling the inner pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbsentUpdateAnswersFalse(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        var cache = Create(inner.Instance(), static (_, _, _) => false, static (_, _, _) => ValueTask.FromResult(default(NodeCacheEntryPeek<string>)));

        // The inner double has no update set up: a call would throw.
        var updated = await cache.UpdateAsync(OperationId, CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsFalse();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private MemoryAdmissionCacheDecorator<string> Create(
        ILogicalNamespacedCache<string> inner,
        Func<string, string, string, bool> hasRecordedOutcome,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntryPeek<string>>> readFenced)
    {
        var accounting = new MemoryUsageAccounting(new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>()));
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options.Value), accounting, "node-a", _testMeter);
        return new MemoryAdmissionCacheDecorator<string>(inner, gate, new CacheEntrySizeEstimator<string>(), hasRecordedOutcome, null, readFenced);
    }
}
