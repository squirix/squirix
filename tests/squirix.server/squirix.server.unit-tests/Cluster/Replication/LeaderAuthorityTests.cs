using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader authority fencing: the minority fails closed and reads wait for the applied index.</summary>
[Immutable]
public sealed class LeaderAuthorityTests : ServerUnitTestBase
{
    /// <summary>A failed quorum confirmation rejects the current read instead of serving stale state.</summary>
    /// <remarks>
    /// #236 mandates the name "FailedQuorumConfirmationRejectsCurrentRead"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Fact]
    public void FailedQuorumRejectsCurrentRead()
    {
        var read = LeaderAuthorityGate.CheckRead(3, true, true, 6, 6, new LeaderReadState(false, 9, 9));
        Assert.False(read.Allowed);
        Assert.Equal(LeaderAuthorityDenial.QuorumNotConfirmed, read.Denial);

        var confirmed = LeaderAuthorityGate.CheckRead(3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        Assert.True(confirmed.Allowed);
        Assert.Equal(LeaderAuthorityDenial.None, confirmed.Denial);
    }

    /// <summary>A leader without majority contact serves neither reads nor writes.</summary>
    [Fact]
    public void MinorityCannotServeReadOrWrite()
    {
        var write = LeaderAuthorityGate.CheckWrite(3, false, true, 4, 4);
        Assert.False(write.Allowed);
        Assert.Equal(LeaderAuthorityDenial.MinorityFenced, write.Denial);

        var read = LeaderAuthorityGate.CheckRead(3, false, true, 4, 4, new LeaderReadState(true, 8, 8));
        Assert.False(read.Allowed);
        Assert.Equal(LeaderAuthorityDenial.MinorityFenced, read.Denial);

        var deposedWrite = LeaderAuthorityGate.CheckWrite(3, true, true, 4, 5);
        Assert.False(deposedWrite.Allowed);
        Assert.Equal(LeaderAuthorityDenial.StaleTerm, deposedWrite.Denial);

        // A non-leader serves nothing even with majority contact.
        var followerWrite = LeaderAuthorityGate.CheckWrite(3, true, false, 4, 4);
        Assert.False(followerWrite.Allowed);
        Assert.Equal(LeaderAuthorityDenial.NotLeader, followerWrite.Denial);

        // An equal observed term steps nothing down: the write stays allowed.
        Assert.True(LeaderAuthorityGate.CheckWrite(3, true, true, 5, 5).Allowed);
    }

    /// <summary>A read waits until the applied index reaches the read index.</summary>
    /// <remarks>
    /// #236 mandates the name "ReadWaitsUntilAppliedIndexReachesReadIndex"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Fact]
    public Task ReadWaitsForAppliedReadIndex()
    {
        var gated = LeaderAuthorityGate.CheckRead(3, true, true, 6, 6, new LeaderReadState(true, 4, 5));
        Assert.False(gated.Allowed);
        Assert.Equal(LeaderAuthorityDenial.ReadIndexNotApplied, gated.Denial);

        var time = new FakeTimeProvider();
        var applied = 4UL;
        var wait = LeaderReadBarrier.WaitUntilAppliedAsync(() => applied, 5, time, TimeSpan.FromMilliseconds(10), DefaultCancellationToken);
        Assert.False(wait.IsCompleted);

        applied = 5;
        time.Advance(TimeSpan.FromMilliseconds(10));
        return wait;
    }

    /// <summary>RF=1 bypasses the authority protocol entirely: no quorum gate and no election timer.</summary>
    [Fact]
    public void RfOneBypassesAuthorityProtocol()
    {
        var write = LeaderAuthorityGate.CheckWrite(1, false, false, 1, 1);
        Assert.True(write.Allowed);

        var read = LeaderAuthorityGate.CheckRead(1, false, false, 1, 1, new LeaderReadState(false, 0, 7));
        Assert.True(read.Allowed);

        using var single = ElectionTimer.Create(1, null, new FakeTimeProvider());
        Assert.Null(single);
        using var quorum = ElectionTimer.Create(3, null, new FakeTimeProvider());
        Assert.NotNull(quorum);
    }
}
