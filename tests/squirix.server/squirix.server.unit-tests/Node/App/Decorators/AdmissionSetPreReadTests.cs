using System;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Rocks;
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

/// <summary>A Set reads the stored entry only when admission could refuse it: the full size of the proposed entry bounds the growth of any replacement.</summary>
public sealed class AdmissionSetPreReadTests : DisposableServerUnitTestBase
{
    private const long CriticalBytes = 9600;
    private const long LimitBytes = 10_000;
    private const long NearLimitBytes = 8400;
    private const string CacheName = "orders";
    private const string Key = "k";
    private const string OperationId = "op-1";

    private static readonly NodeCacheEntry<string> Large = new() { Value = new string('x', 1000), Version = 1 };
    private static readonly NodeCacheEntry<string> SlightlyLarger = new() { Value = new string('x', 1010), Version = 2 };

    private readonly Meter _testMeter = new("admission-set-pre-read");

    /// <summary>Without pressure a Set goes straight to the inner pipeline and never reads the stored entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetWithoutPressureSkipsStoredRead(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, 0, null, null);

        await harness.Cache.SetEntryAsync(OperationId, CacheName, Key, Large, cancellationToken);

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(0);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(1);
    }

    /// <summary>A member lookup that reports no pressure leaves the read out too.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetWithoutMemberPressureSkipsStoredRead(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, 0, static (_, _) => false, null);

        await harness.Cache.SetEntryAsync(OperationId, CacheName, Key, Large, cancellationToken);

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(0);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(1);
    }

    /// <summary>The full size of the proposed entry is refused but the real growth is not: the stored entry is read and the write is admitted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReadsStoredAndAdmitsRealGrowth(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, NearLimitBytes, null, null);

        await harness.Cache.SetEntryAsync(OperationId, CacheName, Key, SlightlyLarger, cancellationToken);

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(1);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(1);
    }

    /// <summary>A replacement that really grows past the limit is read and refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReadsStoredAndRefusesPastLimit(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, NearLimitBytes, null, null);
        var larger = new NodeCacheEntry<string> { Value = new string('x', 3000), Version = 2 };

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(harness.Cache.SetEntryAsync(OperationId, CacheName, Key, larger, cancellationToken).AsTask());

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(1);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(0);
    }

    /// <summary>Under critical pressure a growing Set reads the stored entry and is refused as before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReadsStoredAndRefusesCritical(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, CriticalBytes, null, null);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(harness.Cache.SetEntryAsync(OperationId, CacheName, Key, SlightlyLarger, cancellationToken).AsTask());

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(1);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(0);
    }

    /// <summary>A pressured member of the owning group sends the Set through the stored read, and a growing write is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReadsStoredOnMemberPressure(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, 0, static (_, _) => true, null);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(harness.Cache.SetEntryAsync(OperationId, CacheName, Key, SlightlyLarger, cancellationToken).AsTask());

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(1);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(0);
    }

    /// <summary>A shrinking Set under member pressure reads the stored entry and is admitted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReadsStoredAndAdmitsShrink(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, 0, static (_, _) => true, null);
        var small = new NodeCacheEntry<string> { Value = "s", Version = 2 };

        await harness.Cache.SetEntryAsync(OperationId, CacheName, Key, small, cancellationToken);

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(1);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(1);
    }

    /// <summary>A retry whose outcome is recorded passes under critical pressure, after the stored read, as before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordedSetRetryPassesUnderPressure(CancellationToken cancellationToken)
    {
        var harness = new Harness(this, CriticalBytes, null, static (_, _, operationId) => string.Equals(operationId, OperationId, StringComparison.Ordinal));

        await harness.Cache.SetEntryAsync(OperationId, CacheName, Key, SlightlyLarger, cancellationToken);

        _ = await Assert.That(harness.Reads.Value).IsEqualTo(1);
        _ = await Assert.That(harness.Sets.Value).IsEqualTo(1);
    }

    private sealed class Harness
    {
        internal Harness(
            AdmissionSetPreReadTests owner,
            long currentBytes,
            Func<string, string, bool>? hasMemberPressure,
            Func<string, string, string, bool>? hasRecordedOutcome)
        {
            var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
            _ = inner.Setups.SetEntryAsync(OperationId, CacheName, Key, Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                .Callback((_, _, _, _, _) =>
                {
                    Sets.Value++;
                    return ValueTask.CompletedTask;
                });
            var accounting = new IMemoryUsageAccountingCreateExpectations();
            _ = accounting.Setups.ReadEstimatedBytes().ReturnValue(currentBytes);
            _ = accounting.Setups.RecordAdmissionRejection();
            var options = Options.Create(
                new PressureOptions { MaxEstimatedCacheBytes = LimitBytes, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
            var gate = new PressureGate(new StateEvaluator(options.Value), accounting.Instance(), "node-a", owner._testMeter);
            Cache = new MemoryAdmissionCacheDecorator<string>(
                inner.Instance(),
                gate,
                new CacheEntrySizeEstimator<string>(),
                hasRecordedOutcome,
                (_, _, _) =>
                {
                    Reads.Value++;
                    return ValueTask.FromResult<NodeCacheEntry<string>?>(Large);
                },
                null,
                hasMemberPressure);
        }

        internal MemoryAdmissionCacheDecorator<string> Cache { get; }

        internal StrongBox<int> Reads { get; } = new();

        internal StrongBox<int> Sets { get; } = new();
    }
}
