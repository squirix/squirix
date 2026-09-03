using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Bounded quorum idempotency reservation and retry tests.</summary>
[Immutable]
public sealed class QuorumIdempotencyTests : ServerUnitTestBase
{
    /// <summary>Full idempotency capacity rejects before the local append boundary.</summary>
    [Fact]
    public async Task CapacityRejectsBeforeAppend()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.MaxValue);
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "already-reserved", new byte[] { 9 }, GroupRecordKind.UserMutation, 1, 1));
        var pipeline = new ReplicaCommitTestKit.Pipeline();
        await using var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline, state);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(
            coordinator.CommitAsync(ReplicaCommitTestKit.CreateMutation(), TimeSpan.FromSeconds(1), DefaultCancellationToken));
        Assert.Equal(0, pipeline.LocalAppendCount);
    }

    /// <summary>A resolved retry returns exact bytes and mismatched reuse is rejected.</summary>
    [Fact]
    public void CommitUnknownRetryReturnsOriginalOutcome()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.MaxValue);
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "op-a", new byte[] { 1 }, GroupRecordKind.UserMutation, 1, 1));
        Assert.True(state.TryResolve("client", "op-a", new byte[] { 7, 8 }, 1, 1));
        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "op-a", new byte[] { 1 }, out var record));
        Assert.Equal(new byte[] { 7, 8 }, record.OutcomePayload.ToArray());
        Assert.Equal(GroupIdempotencyLookup.Mismatch, state.Lookup("client", "op-a", new byte[] { 9 }, out _));
    }

    /// <summary>An unresolved reservation survives expiration and blocks new capacity.</summary>
    [Fact]
    public void UnresolvedOutcomeSurvivesRetention()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.Zero);
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "op-a", new byte[] { 1 }, GroupRecordKind.UserMutation, 1, 1));
        Assert.Equal(1, state.UnresolvedCount);

        state.Expire();

        Assert.Equal(GroupIdempotencyReserveResult.CapacityExceeded, state.Reserve("client", "op-b", new byte[] { 2 }, GroupRecordKind.UserMutation, 2, 1));
        Assert.Equal(GroupIdempotencyLookup.Unresolved, state.Lookup("client", "op-a", new byte[] { 1 }, out _));
        Assert.Equal(1, state.UnresolvedCount);

        Assert.True(state.TryReleaseUnresolved("client", "op-a", 1, 1));
        Assert.Equal(0, state.UnresolvedCount);
    }
}
