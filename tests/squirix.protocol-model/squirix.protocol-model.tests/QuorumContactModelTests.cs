using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class QuorumContactModelTests
{
    [Test]
    public async Task MismatchMajorityKeepsLeaderWithoutWrites()
    {
        var leader = QuorumContactState.Start(5).Advance()
            .Receive(new QuorumProbeReply(1, 2, 2, QuorumProbeVerdict.LogMismatch))
            .Receive(new QuorumProbeReply(2, 2, 2, QuorumProbeVerdict.LogMismatch))
            .Advance().CheckQuorum();

        _ = await Assert.That((leader.Role, leader.HasRecentContact(1), leader.HasRecentContact(2), leader.HasWriteQuorum))
                        .IsEqualTo((NodeRole.Leader, true, true, false));
        var verified = leader.Receive(new QuorumProbeReply(1, 2, 2, QuorumProbeVerdict.Accepted))
                             .Receive(new QuorumProbeReply(2, 2, 2, QuorumProbeVerdict.Accepted));
        _ = await Assert.That(verified.HasWriteQuorum).IsTrue();
    }

    [Test]
    public async Task PreviousTermReplyCannotRefreshContact()
    {
        var leader = QuorumContactState.Start(3)
            .Receive(new QuorumProbeReply(1, 2, 2, QuorumProbeVerdict.Accepted))
            .ElectNextTerm().Advance()
            .Receive(new QuorumProbeReply(1, 2, 2, QuorumProbeVerdict.Accepted))
            .Receive(new QuorumProbeReply(2, 2, 3, QuorumProbeVerdict.Incomplete));

        _ = await Assert.That((leader.Term, leader.HasRecentContact(1), leader.HasRecentContact(2), leader.CanCountInWriteQuorum(1)))
                        .IsEqualTo((3, false, false, false));
        _ = await Assert.That(leader.Advance().CheckQuorum().Role).IsEqualTo(NodeRole.Follower);
    }

    [Test]
    public async Task SilentMajorityDeposesAfterWindow()
    {
        var grace = QuorumContactState.Start(5).Advance().CheckQuorum();
        var minority = grace.Receive(new QuorumProbeReply(1, 2, 2, QuorumProbeVerdict.LogMismatch));
        _ = await Assert.That(grace.Role).IsEqualTo(NodeRole.Leader);
        _ = await Assert.That(minority.Advance().CheckQuorum().Role).IsEqualTo(NodeRole.Follower);
    }

    [Test]
    public async Task WaitingDoesNotRefreshEarlierReplies()
    {
        var leader = QuorumContactState.Start(5)
            .Receive(new QuorumProbeReply(1, 2, 2, QuorumProbeVerdict.LogMismatch))
            .Advance().Receive(new QuorumProbeReply(2, 2, 2, QuorumProbeVerdict.LogMismatch))
            .Advance();

        _ = await Assert.That((leader.HasRecentContact(1), leader.HasRecentContact(2), leader.CheckQuorum().Role))
                        .IsEqualTo((false, true, NodeRole.Follower));
    }

    [Test]
    public async Task HigherTermReplyDeposesWithContacts()
    {
        var leader = QuorumContactState.Start(3).Advance()
            .Receive(new QuorumProbeReply(1, 2, 2, QuorumProbeVerdict.LogMismatch)).Advance().CheckQuorum();
        _ = await Assert.That(leader.Role).IsEqualTo(NodeRole.Leader);

        var newer = leader.Receive(new QuorumProbeReply(2, 2, 3, QuorumProbeVerdict.Incomplete));
        _ = await Assert.That((newer.Term, newer.Role, newer.HasRecentContact(1))).IsEqualTo((3, NodeRole.Follower, false));
    }

    [Test]
    public async Task SearchFindsDroppedProbeContact()
    {
        var (healthy, broken, seenStates) = ExploreRunner.ExploreQuorumContact();

        _ = await Assert.That(seenStates).IsLessThanOrEqualTo(QuorumContactSearch.MaxStates);
        _ = await Assert.That((healthy.Tick, healthy.Role, broken.Role, healthy.HasWriteQuorum))
                        .IsEqualTo((QuorumContactState.ContactWindow, NodeRole.Leader, NodeRole.Follower, false));
        _ = await Assert.That((healthy.HasRecentContact(1), healthy.HasRecentContact(2))).IsEqualTo((true, true));
    }
}
