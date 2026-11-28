using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Generated;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitBatchKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies admission against the replica leader's prepare-time entry.</summary>
public sealed class ReplicaReplacementAdmissionTests : IsolatedStorageTestBase
{
    /// <summary>After a shrink, a replacement that grows the entry is refused under local or member pressure.</summary>
    /// <param name="memberPressure">Whether the pressure comes from a follower rather than the leader.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReplicaReplacementChecksGrowth(bool memberPressure, CancellationToken cancellationToken)
    {
        const string cacheName = "resp";
        const string key = "k";
        var cache = new PhysicalCache<StoredValue>(TimeProvider.System, null);
        var local = new ClientCache<StoredValue>(cache, cache);
        var accounting = IMemoryUsageAccounting.Mock(MockBehavior.Strict);
        _ = accounting.ReadEstimatedBytes().Returns(memberPressure ? 0 : 9600);
        _ = accounting.RecordAdmissionRejection();
        var pressure = new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 10_000, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(pressure, accounting.Object, "node", null);
        var factory = new ReplicaMutationFactory(local, "node", 1UL, TimeProvider.System, NullLogger.Instance)
        {
            ReplacementAdmission = new ReplacementAdmission<StoredValue>(gate, new CacheEntrySizeEstimator<StoredValue>()),
            HasMemberPressure = () => memberPressure,
        };
        var large = new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText(new string('a', 1024)) };
        var small = new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText("small") };
        var medium = new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText(new string('b', 512)) };
        await local.SetEntryAsync(string.Empty, cacheName, key, large, cancellationToken);
        var shrink = await factory.PrepareReplaceEntryAsync("shrink", cacheName, key, small, 1UL, cancellationToken);
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(shrink.OutcomePayload)).IsTrue();
        await local.SetEntryAsync(string.Empty, cacheName, key, small, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(
            factory.PrepareReplaceEntryAsync("growth", cacheName, key, medium, 2UL, cancellationToken).AsTask(),
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            cancellationToken);
    }

    /// <summary>A replacement accounts for queued writes to other keys before it is admitted.</summary>
    /// <param name="firstIsSet">Whether the preceding write in the queue is an ordinary set.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(30_000)]
    public async Task QueuedReplacementsRespectBudget(bool firstIsSet, CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<StoredValue>();
        var physical = new PhysicalCache<StoredValue>(TimeProvider.System, estimator);
        var local = new ClientCache<StoredValue>(physical, physical);
        var original = Entry();
        var replacement = new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText(new string('b', 512)), Version = 1 };
        await local.SetEntryAsync(string.Empty, Cache, "alpha", original, cancellationToken);
        await local.SetEntryAsync(string.Empty, Cache, "beta", original, cancellationToken);
        await local.SetEntryAsync(string.Empty, Cache, "gate", original, cancellationToken);

        var usage = new MemoryUsageAccounting(physical);
        var delta = estimator.EstimateBytes(new CacheKey(Cache, "alpha"), replacement, false) - estimator.EstimateBytes(new CacheKey(Cache, "alpha"), original, false);
        var limit = usage.ReadEstimatedBytes() + delta + (delta / 2);
        var pressure = new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = limit, CriticalPressureThresholdPercent = 100 });
        var admission = new ReplacementAdmission<StoredValue>(new PressureGate(pressure, usage, OwnedGroup, null), estimator);
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(Dir, DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, (TimeProvider.System, null), admission);

        var firstOperationId = NewOperationId();
        var blocked = SetAsync(committer, "gate", cancellationToken);
        try
        {
            await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            Task first = firstIsSet
                ? committer.CommitSetAsync(NewOperationId(), Cache, "alpha", replacement, cancellationToken)
                : committer.CommitReplaceEntryAsync(firstOperationId, Cache, "alpha", replacement, cancellationToken);
            var second = committer.CommitReplaceEntryAsync(NewOperationId(), Cache, "beta", replacement, cancellationToken);
            gateway.Release();

            await first;
            _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(second, StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            gateway.Release();
            await blocked;
        }

        var updated = await local.GetEntryAsync(Cache, "alpha", cancellationToken);
        var unchanged = await local.GetEntryAsync(Cache, "beta", cancellationToken);
        _ = await Assert.That(updated!.Value.Memory.Span.SequenceEqual(replacement.Value.Memory.Span)).IsTrue();
        _ = await Assert.That(unchanged!.Value.Memory.Span.SequenceEqual(original.Value.Memory.Span)).IsTrue();
        _ = await Assert.That(usage.ReadEstimatedBytes()).IsLessThan(limit);
        _ = await Assert.That((await GetOwnedLog(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);

        if (!firstIsSet)
            await AssertReplayUnderPressureAsync(committer, local, firstOperationId, replacement, cancellationToken);
    }

    private static async Task AssertReplayUnderPressureAsync(
        ReplicaGroupCommitter committer,
        ClientCache<StoredValue> local,
        string operationId,
        NodeCacheEntry<StoredValue> replacement,
        CancellationToken cancellationToken)
    {
        var original = Entry();
        await committer.CommitSetAsync(NewOperationId(), Cache, "alpha", original, cancellationToken);
        await committer.CommitSetAsync(NewOperationId(), Cache, "beta", replacement, cancellationToken);

        var replayed = await committer.CommitReplaceEntryAsync(operationId, Cache, "alpha", replacement, cancellationToken);

        _ = await Assert.That(replayed).IsTrue();
        var current = await local.GetEntryAsync(Cache, "alpha", cancellationToken);
        _ = await Assert.That(current!.Value.Memory.Span.SequenceEqual(original.Value.Memory.Span)).IsTrue();
    }
}
