using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Bounded quorum idempotency reservation and retry tests.</summary>
[Immutable]
public sealed class QuorumIdempotencyTests : ServerUnitTestBase
{
    /// <summary>Full idempotency capacity rejects before the local append boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CapacityRejectsBeforeAppend(CancellationToken cancellationToken)
    {
        var state = new GroupIdempotencyState(1, TimeSpan.MaxValue);
        _ = await Assert.That(state.Reserve("client", "already-reserved", [9], GroupRecordKind.UserMutation, 1, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        var pipeline = new ReplicaCommitTestKit.Pipeline();
        await using var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline, state);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(
            coordinator.CommitAsync(ReplicaCommitTestKit.CreateMutation(), TimeSpan.FromSeconds(1), cancellationToken));
        _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(0);
    }

    /// <summary>A resolved retry returns exact bytes and mismatched reuse is rejected.</summary>
    [Test]
    public async Task CommitUnknownRetryReturnsOriginalOutcome()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.MaxValue);
        _ = await Assert.That(state.Reserve("client", "op-a", [1], GroupRecordKind.UserMutation, 1, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(state.TryResolve("client", "op-a", [7, 8], 1, 1)).IsTrue();
        _ = await Assert.That(state.Lookup("client", "op-a", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([7, 8], record.OutcomePayload.ToArray());
        _ = await Assert.That(state.Lookup("client", "op-a", [9], out _)).IsEqualTo(GroupIdempotencyLookup.Mismatch);
    }

    /// <summary>An unresolved reservation survives expiration and blocks new capacity.</summary>
    [Test]
    public async Task UnresolvedOutcomeSurvivesRetention()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.Zero);
        _ = await Assert.That(state.Reserve("client", "op-a", [1], GroupRecordKind.UserMutation, 1, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(state.UnresolvedCount).IsEqualTo(1);

        state.Expire();

        _ = await Assert.That(state.Reserve("client", "op-b", [2], GroupRecordKind.UserMutation, 2, 1)).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(state.Lookup("client", "op-a", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        _ = await Assert.That(state.UnresolvedCount).IsEqualTo(1);

        _ = await Assert.That(state.TryReleaseUnresolved("client", "op-a", 1, 1)).IsTrue();
        _ = await Assert.That(state.UnresolvedCount).IsEqualTo(0);
    }
}
