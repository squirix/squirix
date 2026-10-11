using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
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
            coordinator.CommitAsync(ReplicaMutationTestKit.CreateMutation(), TimeSpan.FromSeconds(1), cancellationToken));
        _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(0);
    }

    /// <summary>
    /// A full group refuses a new write while its outcomes are younger than the minimum retention, replays a retry inside it, and admits
    /// the write once the oldest outcome is older.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FullGroupAdmitsWriteAfterMinRetention(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var state = new GroupIdempotencyState(2, TimeSpan.FromMinutes(15), clock, TimeSpan.FromMinutes(1));
        var pipeline = new ReplicaCommitTestKit.Pipeline();
        await using var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline, state);
        var timeout = TimeSpan.FromSeconds(1);

        _ = await coordinator.CommitAsync(CreateMutation("op-1", 1), timeout, cancellationToken);
        _ = await coordinator.CommitAsync(CreateMutation("op-2", 2), timeout, cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(59));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(coordinator.CommitAsync(CreateMutation("op-3", 3), timeout, cancellationToken));
        _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(2);

        _ = await coordinator.CommitAsync(CreateMutation("op-1", 1), timeout, cancellationToken);
        _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(2);

        clock.Advance(TimeSpan.FromSeconds(2));
        _ = await coordinator.CommitAsync(CreateMutation("op-3", 3), timeout, cancellationToken);

        _ = await Assert.That(pipeline.LocalAppendCount).IsEqualTo(3);
        _ = await Assert.That(state.Lookup("client", "op-1", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.Lookup("client", "op-2", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.Lookup("client", "op-3", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
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

    /// <summary>A full store refuses a new operation but still pins an entry the log already holds.</summary>
    [Test]
    public async Task FullStorePinsLoggedEntry()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.MaxValue);
        _ = await Assert.That(state.Reserve("client", "op-a", [1], GroupRecordKind.UserMutation, 1, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);

        var fresh = state.Reserve("client", "op-b", [2], GroupRecordKind.UserMutation, 2, 1);
        var logged = state.Reserve("client", "op-c", [3], GroupRecordKind.UserMutation, 3, 1, true);

        _ = await Assert.That(fresh).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(logged).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(state.Lookup("client", "op-c", [3], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
    }

    /// <summary>An unresolved reservation survives expiration and blocks new capacity.</summary>
    [Test]
    public async Task UnresolvedOutcomeSurvivesRetention()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.Zero);
        _ = await Assert.That(state.Reserve("client", "op-a", [1], GroupRecordKind.UserMutation, 1, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(state.Lookup("client", "op-a", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);

        state.Expire();

        _ = await Assert.That(state.Reserve("client", "op-b", [2], GroupRecordKind.UserMutation, 2, 1)).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(state.Lookup("client", "op-a", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        _ = await Assert.That(state.Lookup("client", "op-a", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);

        _ = await Assert.That(state.TryReleaseUnresolved("client", "op-a", 1, 1)).IsTrue();
        _ = await Assert.That(state.Lookup("client", "op-a", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.Reserve("client", "op-b", [2], GroupRecordKind.UserMutation, 2, 1)).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    private static PreparedReplicaMutation CreateMutation(string operationId, ulong logIndex)
    {
        var identity = new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 });
        return new PreparedReplicaMutation(identity, 1, logIndex, new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));
    }
}
