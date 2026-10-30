using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Options;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
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
        _ = inner.Setups.GetEntryAsync(CacheName, Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var accounting = CreateAccounting();
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
    }

    /// <summary>Under critical memory pressure a retried set whose outcome is recorded replays it instead of being refused and counts no rejection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordedSetReplaysUnderPressure(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var accounting = CreateAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1);

        await cache.SetEntryAsync("op-1", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(0L);
    }

    /// <summary>Under critical memory pressure retried updates and adds whose outcomes are recorded replay them instead of being refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordedUpdateAndAddReplayUnderPressure(CancellationToken cancellationToken)
    {
        var stored = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var present = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = present.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(stored));
        _ = present.Setups.UpdateAsync("op-1", CacheName, Key, "v2", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var absent = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = absent.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = absent.Setups.TryAddEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var updateAccounting = CreateAccounting();
        var addAccounting = CreateAccounting();
        var updated = await Create(present.Instance(), updateAccounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1)
           .UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);
        var added = await Create(absent.Instance(), addAccounting, static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal), 1)
           .TryAddEntryAsync("op-1", CacheName, Key, stored, cancellationToken);

        _ = await Assert.That(updated).IsTrue();
        _ = await Assert.That(updateAccounting.ReadRejectedWriteCount()).IsEqualTo(0L);
        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That(addAccounting.ReadRejectedWriteCount()).IsEqualTo(0L);
    }

    /// <summary>A retried add over the key it added replays the recorded <see langword="true" />.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedAddReplaysRecordedOutcome(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        _ = inner.Setups.TryAddEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), CreateAccounting(), static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal));

        var added = await cache.TryAddEntryAsync("op-1", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsTrue();
    }

    /// <summary>A retried update of a key removed since replays the recorded <see langword="true" />.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedUpdateReplaysRecordedOutcome(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.UpdateAsync("op-1", CacheName, Key, "v2", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), CreateAccounting(), static (_, _, operationId) => string.Equals(operationId, "op-1", StringComparison.Ordinal));

        var updated = await cache.UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
    }

    /// <summary>Without a recording inner pipeline an add over a present key is answered here, without calling the inner pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SingleCopyAddAnsweredHere(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        var cache = Create(inner.Instance(), CreateAccounting(), null);

        // The inner double has no add set up: a call would throw.
        var added = await cache.TryAddEntryAsync("op-3", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsFalse();
    }

    /// <summary>Without pressure a set over a present key writes it once and does not read it: admission cannot refuse it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SingleCopySetSkipsRead(CancellationToken cancellationToken)
    {
        var reads = 0;
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>())
                 .Callback((_, _, _) =>
                  {
                      _ = Interlocked.Increment(ref reads);
                      return ValueTask.FromResult<NodeCacheEntry<string>?>(Small);
                  });
        _ = inner.Setups.SetEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var cache = Create(inner.Instance(), CreateAccounting(), null);

        await cache.SetEntryAsync("op-1", CacheName, Key, new NodeCacheEntry<string> { Value = new string('x', 4096), Version = 1 }, cancellationToken);

        _ = await Assert.That(reads).IsEqualTo(0);
    }

    /// <summary>An add over a present key whose operation has no recorded outcome is refused here, without a log record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnrecordedAddAnsweredHere(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        var cache = Create(inner.Instance(), CreateAccounting(), static (_, _, _) => false);

        // The inner double has no add set up: a call would throw.
        var added = await cache.TryAddEntryAsync("op-2", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsFalse();
    }

    /// <summary>Under critical memory pressure a set without a recorded outcome is refused before the inner pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnrecordedSetRefusedUnderPressure(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        var accounting = CreateAccounting();
        var cache = Create(inner.Instance(), accounting, static (_, _, _) => false, 1);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(cache.SetEntryAsync("op-2", CacheName, Key, entry, cancellationToken).AsTask());

        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(1L);
    }

    /// <summary>An add whose stored read finds the key present is admitted when the fenced read finds it absent, and refused under critical pressure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FencedReadThatFindsKeyAbsentAdmitsAdd(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        _ = inner.Setups.TryAddEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var instance = inner.Instance();
        var admitted = await Create(instance, CreateAccounting(), static (_, _, _) => false, 10_000_000_000, FencedAbsentAsync)
           .TryAddEntryAsync("op-1", CacheName, Key, entry, cancellationToken);
        var accounting = CreateAccounting();
        var refusing = Create(instance, accounting, static (_, _, _) => false, 1, FencedAbsentAsync);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(refusing.TryAddEntryAsync("op-2", CacheName, Key, entry, cancellationToken).AsTask());

        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(accounting.ReadRejectedWriteCount()).IsEqualTo(1L);
    }

    /// <summary>An update whose stored read finds the key missing is admitted when the fenced read finds it present.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FencedReadThatFindsKeyAdmitsUpdate(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(null));
        _ = inner.Setups.UpdateAsync("op-1", CacheName, Key, "v2", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), CreateAccounting(), static (_, _, _) => false, 10_000_000_000, static (_, _, _) => ValueTask.FromResult(new NodeCacheEntryPeek<string>(Small, false)));

        var updated = await cache.UpdateAsync("op-1", CacheName, Key, "v2", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
    }

    /// <summary>A retry whose outcome is recorded goes to the inner pipeline without a fenced read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordedRetrySkipsFence(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        _ = inner.Setups.TryAddEntryAsync("op-1", CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));
        var cache = Create(inner.Instance(), CreateAccounting(), static (_, _, _) => true, 10_000_000_000, static (_, _, _) => throw new InvalidOperationException("The fence must not run."));

        var added = await cache.TryAddEntryAsync("op-1", CacheName, Key, entry, cancellationToken);

        _ = await Assert.That(added).IsTrue();
    }

    /// <summary>A fence that refuses fails the write with the refusal and writes nothing to the inner pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FenceRefusalWritesNothing(CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<string>?>(entry));
        var cache = Create(
            inner.Instance(),
            CreateAccounting(),
            static (_, _, _) => false,
            10_000_000_000,
            static (_, _, _) => ValueTask.FromException<NodeCacheEntryPeek<string>>(new RpcException(new Status(StatusCode.Unavailable, "unconfirmed"))));

        // The inner double has no add set up: a call would throw a different exception.
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(cache.TryAddEntryAsync("op-2", CacheName, Key, entry, cancellationToken).AsTask());

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static ValueTask<NodeCacheEntryPeek<string>> FencedAbsentAsync(string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(default(NodeCacheEntryPeek<string>));

    private static MemoryUsageAccounting CreateAccounting() => new(new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>()));

    private MemoryAdmissionCacheDecorator<string> Create(
        ILogicalNamespacedCache<string> inner,
        MemoryUsageAccounting accounting,
        Func<string, string, string, bool>? hasRecordedOutcome,
        long maxEstimatedCacheBytes = 10_000_000_000,
        Func<string, string, CancellationToken, ValueTask<NodeCacheEntryPeek<string>>>? readFenced = null)
    {
        var options = Options.Create(
            new PressureOptions { MaxEstimatedCacheBytes = maxEstimatedCacheBytes, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options.Value), accounting, Self, _testMeter);
        return new MemoryAdmissionCacheDecorator<string>(inner, gate, new CacheEntrySizeEstimator<string>(), hasRecordedOutcome, null, readFenced);
    }
}
