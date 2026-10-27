using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader authority fencing: the minority fails closed and reads wait for the applied index.</summary>
[Immutable]
public sealed class LeaderAuthorityTests : ServerUnitTestBase
{
    /// <summary>A failed quorum confirmation rejects the current read instead of serving stale state.</summary>
    /// <remarks>
    /// The mandated name is "FailedQuorumConfirmationRejectsCurrentRead"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Test]
    public async Task FailedQuorumRejectsCurrentRead()
    {
        var read = LeaderAuthorityGate.CheckRead(3, true, true, 6, 6, new LeaderReadState(false, 9, 9));
        _ = await Assert.That(read.Allowed).IsFalse();
        _ = await Assert.That(read.Denial).IsEqualTo(LeaderAuthorityDenial.QuorumNotConfirmed);

        var confirmed = LeaderAuthorityGate.CheckRead(3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(confirmed.Allowed).IsTrue();
        _ = await Assert.That(confirmed.Denial).IsEqualTo(LeaderAuthorityDenial.None);
    }

    /// <summary>A leader without majority contact serves neither reads nor writes.</summary>
    [Test]
    public async Task MinorityCannotServeReadOrWrite()
    {
        var write = LeaderAuthorityGate.CheckWrite(3, false, true, 4, 4);
        _ = await Assert.That(write.Allowed).IsFalse();
        _ = await Assert.That(write.Denial).IsEqualTo(LeaderAuthorityDenial.MinorityFenced);

        var read = LeaderAuthorityGate.CheckRead(3, false, true, 4, 4, new LeaderReadState(true, 8, 8));
        _ = await Assert.That(read.Allowed).IsFalse();
        _ = await Assert.That(read.Denial).IsEqualTo(LeaderAuthorityDenial.MinorityFenced);

        var deposedWrite = LeaderAuthorityGate.CheckWrite(3, true, true, 4, 5);
        _ = await Assert.That(deposedWrite.Allowed).IsFalse();
        _ = await Assert.That(deposedWrite.Denial).IsEqualTo(LeaderAuthorityDenial.StaleTerm);

        // A non-leader serves nothing even with majority contact.
        var followerWrite = LeaderAuthorityGate.CheckWrite(3, true, false, 4, 4);
        _ = await Assert.That(followerWrite.Allowed).IsFalse();
        _ = await Assert.That(followerWrite.Denial).IsEqualTo(LeaderAuthorityDenial.NotLeader);

        // An equal observed term steps nothing down: the write stays allowed.
        _ = await Assert.That(LeaderAuthorityGate.CheckWrite(3, true, true, 5, 5).Allowed).IsTrue();
    }

    /// <summary>A read waits until the applied index reaches the read index.</summary>
    /// <remarks>
    /// The mandated name is "ReadWaitsUntilAppliedIndexReachesReadIndex"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Test]
    public async Task ReadWaitsForAppliedReadIndex()
    {
        var gated = LeaderAuthorityGate.CheckRead(3, true, true, 6, 6, new LeaderReadState(true, 4, 5));
        _ = await Assert.That(gated.Allowed).IsFalse();
        _ = await Assert.That(gated.Denial).IsEqualTo(LeaderAuthorityDenial.ReadIndexNotApplied);
    }

    /// <summary>RF=1 bypasses the authority protocol entirely: no quorum gate on writes or reads.</summary>
    [Test]
    public async Task RfOneBypassesAuthorityProtocol()
    {
        var write = LeaderAuthorityGate.CheckWrite(1, false, false, 1, 1);
        _ = await Assert.That(write.Allowed).IsTrue();

        var read = LeaderAuthorityGate.CheckRead(1, false, false, 1, 1, new LeaderReadState(false, 0, 7));
        _ = await Assert.That(read.Allowed).IsTrue();
    }
}
