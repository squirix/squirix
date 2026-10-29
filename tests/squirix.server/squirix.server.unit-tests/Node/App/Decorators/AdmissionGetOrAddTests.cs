using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Rocks;
using Squirix.Server.Core;
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

/// <summary>GetOrAdd reuses confirmed admission reads while preserving recorded add outcomes and concurrent misses.</summary>
public sealed class AdmissionGetOrAddTests : DisposableServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "key";
    private const string OperationId = "operation";
    private static readonly NodeCacheEntry<string> Stored = new("stored", 1);
    private readonly Meter _meter = new("get-or-add-admission-tests");

    /// <summary>A confirmed live value is returned without another read or an add, including stored null values.</summary>
    /// <param name="value">The confirmed value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Test]
    [Arguments("confirmed")]
    [Arguments(null)]
    public async Task HitReturnsConfirmedValue(string? value, CancellationToken cancellationToken)
    {
        var inner = StoredCache();
        var confirmed = new NodeCacheEntry<string>(value, 2);
        var fences = 0;
        var cache = Create(inner.Instance(), static (_, _, _) => false, (_, _, _) =>
        {
            fences++;
            return ValueTask.FromResult(new NodeCacheEntryPeek<string>(confirmed, false));
        });

        var result = await cache.AddOrGetAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(result.Added).IsFalse();
        _ = await Assert.That(result.Existing).IsEqualTo(new NodeCacheValueResult<string>(true, value));
        _ = await Assert.That(fences).IsEqualTo(1);
    }

    /// <summary>A recorded successful add takes precedence over the current stored value and does not need a read fence.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Test]
    public async Task RecordedAddReplaysBeforeFence(CancellationToken cancellationToken)
    {
        var inner = StoredCache();
        _ = inner.Setups.TryAddEntryAsync(OperationId, CacheName, Key, Stored, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), static (_, _, _) => true, static (_, _, _) => throw new InvalidOperationException("Unexpected fence."));

        var result = await cache.AddOrGetAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(result.Added).IsTrue();
        _ = await Assert.That(result.Existing.Found).IsFalse();
    }

    /// <summary>A fence may itself apply the retried add, whose newly recorded outcome must then be replayed.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Test]
    public async Task FenceRecordedAddReplays(CancellationToken cancellationToken)
    {
        var inner = StoredCache();
        _ = inner.Setups.TryAddEntryAsync(OperationId, CacheName, Key, Stored, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var recorded = false;
        var cache = Create(inner.Instance(), (_, _, _) => recorded, (_, _, _) =>
        {
            recorded = true;
            return ValueTask.FromResult(new NodeCacheEntryPeek<string>(Stored, false));
        });

        var result = await cache.AddOrGetAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(result.Added).IsTrue();
    }

    /// <summary>A lost add race returns the value read after the unsuccessful inner add, or a concurrent miss.</summary>
    /// <param name="found">Whether the concurrent entry remains present.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LostAddRaceReadsInnerValue(bool found, CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.TryAddEntryAsync(OperationId, CacheName, Key, Stored, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(false));
        var existing = new NodeCacheValueResult<string>(found, found ? "concurrent" : null);
        _ = inner.Setups.GetValueAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(existing));
        var cache = Create(inner.Instance(), static (_, _, _) => false, static (_, _, _) => throw new InvalidOperationException("Unexpected fence."));

        var result = await cache.AddOrGetAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new NodeCacheAddOrGetResult<string>(false, existing));
    }

    /// <summary>A refused fence never returns an unconfirmed stored value.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Test]
    public async Task FailedFenceDoesNotReturnStoredValue(CancellationToken cancellationToken)
    {
        var failure = new InvalidOperationException("Read authority refused.");
        var cache = Create(StoredCache().Instance(), static (_, _, _) => false, (_, _, _) => ValueTask.FromException<NodeCacheEntryPeek<string>>(failure));

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, NodeCacheAddOrGetResult<string>>(
            cache.AddOrGetAsync(OperationId, CacheName, Key, Stored, cancellationToken));

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
    }

    /// <summary>An entry whose expiry is pending is handed to the inner add, which decides the expiration and the new write.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Test]
    public async Task PendingExpiryRunsInnerAdd(CancellationToken cancellationToken)
    {
        var inner = StoredCache();
        _ = inner.Setups.TryAddEntryAsync(OperationId, CacheName, Key, Stored, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), static (_, _, _) => false, static (_, _, _) => ValueTask.FromResult(new NodeCacheEntryPeek<string>(Stored, true)));

        var result = await cache.AddOrGetAsync(OperationId, CacheName, Key, Stored, cancellationToken);

        _ = await Assert.That(result.Added).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _meter.Dispose();

    private static ILogicalNamespacedCacheCreateExpectations<string> StoredCache()
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(Stored));
        return inner;
    }

    private MemoryAdmissionCacheDecorator<string> Create(
        ILogicalNamespacedCache<string> inner,
        Func<string, string, string, bool> recorded,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntryPeek<string>>> fence)
    {
        var estimator = new CacheEntrySizeEstimator<string>();
        var accounting = new MemoryUsageAccounting(new PhysicalCache<string>(TimeProvider.System, estimator));
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options), accounting, "node", _meter);
        return new MemoryAdmissionCacheDecorator<string>(inner, gate, estimator, recorded, null, fence);
    }
}
