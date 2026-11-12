using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Pending entries preserve index order, replacement and retry semantics independently of insertion order.</summary>
[Immutable]
public sealed class ReplicaPendingAppliesTests : ServerUnitTestBase
{
    /// <summary>A commit applies only its covered prefix, even when retained indexes arrive out of order.</summary>
    [Test]
    public async Task RetainAppliesInLogIndexOrder()
    {
        var pipeline = new RunPipeline();
        var pending = new ReplicaPendingApplies(pipeline, ReplicaBatchKit.NewIdempotency(), static _ => { });
        _ = await Assert.That(pending.IsEmpty).IsTrue();
        _ = await Assert.That(pending.LastIndex).IsEqualTo(0UL);
        _ = await Assert.That(pending.Covers(ulong.MaxValue)).IsFalse();
        await pending.ApplyThroughAsync(0);
        _ = await Assert.That(pipeline.Applied).IsEmpty();
        foreach (var index in new[] { 7UL, 3UL, 9UL, 1UL, 5UL })
            pending.Retain(Mutation(index));

        _ = await Assert.That(pending.LastIndex).IsEqualTo(9UL);
        _ = await Assert.That(pending.Covers(0)).IsFalse();
        _ = await Assert.That(pending.Covers(1)).IsTrue();
        await pending.ApplyThroughAsync(4);

        await SequenceAssert.EqualAsync([1UL, 3UL], pipeline.Applied.ToArray());
        _ = await Assert.That(pending.IsEmpty).IsFalse();
        _ = await Assert.That(pending.Covers(4)).IsFalse();
        _ = await Assert.That(pending.Covers(5)).IsTrue();
        _ = await Assert.That(pending.LastIndex).IsEqualTo(9UL);
        await pending.ApplyThroughAsync(9);

        await SequenceAssert.EqualAsync([1UL, 3UL, 5UL, 7UL, 9UL], pipeline.Applied.ToArray());
        _ = await Assert.That(pending.IsEmpty).IsTrue();
        _ = await Assert.That(pending.LastIndex).IsEqualTo(0UL);
        _ = await Assert.That(pending.Covers(ulong.MaxValue)).IsFalse();
    }

    /// <summary>Retaining the same index replaces its payload and term rather than keeping or duplicating the old entry.</summary>
    [Test]
    public async Task RetainReplacesSameLogIndex()
    {
        var pipeline = new RunPipeline();
        List<PreparedReplicaMutation> resolved = [];
        var pending = new ReplicaPendingApplies(pipeline, ReplicaBatchKit.NewIdempotency(), resolved.Add);
        var replacement = Mutation(2, 3);
        pending.Retain(Mutation(2));
        pending.Retain(Mutation(1));
        pending.Retain(replacement);
        pending.Retain(Mutation(3));
        await pending.ApplyThroughAsync(3, 1, 3, true);

        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
        _ = await Assert.That(resolved.Count).IsEqualTo(3);
        _ = await Assert.That(resolved[1]).IsSameReferenceAs(replacement);
        _ = await Assert.That(pending.IsEmpty).IsTrue();
    }

    /// <summary>Failed apply retains the failed entry and the suffix while the successful prefix stays removed on retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedApplyRetainsRemainingEntries(CancellationToken cancellationToken)
    {
        var pipeline = new RunPipeline { FailApplyAt = 2 };
        var pending = new ReplicaPendingApplies(pipeline, ReplicaBatchKit.NewIdempotency(), static _ => { });
        pending.Retain(Mutation(3));
        pending.Retain(Mutation(1));
        pending.Retain(Mutation(2));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            pending.ApplyThroughAsync(3, 1, 3, false),
            ReplicaBatchKit.StallTimeout,
            TimeProvider.System,
            cancellationToken);

        await SequenceAssert.EqualAsync([1UL], pipeline.Applied.ToArray());
        _ = await Assert.That(pending.IsEmpty).IsFalse();
        _ = await Assert.That(pending.LastIndex).IsEqualTo(3UL);
        _ = await Assert.That(pending.Covers(1)).IsFalse();
        _ = await Assert.That(pending.Covers(2)).IsTrue();
        pipeline.StopFailingApply();
        await pending.ApplyThroughAsync(3, 1, 3, false);

        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
        _ = await Assert.That(pending.IsEmpty).IsTrue();
        _ = await Assert.That(pending.LastIndex).IsEqualTo(0UL);
    }

    /// <summary>Log indexes retain unsigned ordering across the signed boundary and at the maximum value.</summary>
    [Test]
    public async Task LargeIndexesKeepUnsignedOrder()
    {
        const ulong high = 1UL << 63;
        var pipeline = new RunPipeline();
        var pending = new ReplicaPendingApplies(pipeline, ReplicaBatchKit.NewIdempotency(), static _ => { });
        pending.Retain(Mutation(ulong.MaxValue));
        pending.Retain(Mutation(high));
        pending.Retain(Mutation(1));
        pending.Retain(Mutation(high - 1));
        _ = await Assert.That(pending.LastIndex).IsEqualTo(ulong.MaxValue);
        await pending.ApplyThroughAsync(high);

        await SequenceAssert.EqualAsync([1UL, high - 1, high], pipeline.Applied.ToArray());
        _ = await Assert.That(pending.LastIndex).IsEqualTo(ulong.MaxValue);
        _ = await Assert.That(pending.Covers(ulong.MaxValue - 1)).IsFalse();
        _ = await Assert.That(pending.Covers(ulong.MaxValue)).IsTrue();
        await pending.ApplyThroughAsync(ulong.MaxValue);

        await SequenceAssert.EqualAsync([1UL, high - 1, high, ulong.MaxValue], pipeline.Applied.ToArray());
        _ = await Assert.That(pending.IsEmpty).IsTrue();
    }

    private static PreparedReplicaMutation Mutation(ulong index, ulong term = 1) => new(
        new ReplicaOperationIdentity("group-a", "client", index.ToString("x32", CultureInfo.InvariantCulture), new byte[] { 1 }),
        term,
        index,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));
}
