using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Only a follower whose log is ready can vote for another leader, so only its answers count when the leader asks who could take over.</summary>
public sealed class ReplicaElectableContactsTests : ServerUnitTestBase
{
    private const int LeaderSlot = 0;
    private const ulong LedTerm = 2UL;
    private static readonly ElectionTimerOptions Options = new() { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 7UL };

    /// <summary>With five replicas, the leader and two ready followers are a quorum, but two followers cannot elect a leader without it.</summary>
    [Test]
    public async Task TwoReadyFollowersAreNotElectable()
    {
        var (state, time) = Lead(5);
        time.Advance(Options.ElectionTimeout);
        state.RecordFollowerReply(1, Ready());
        state.RecordFollowerReply(2, Ready());
        var twoFollowers = (state.HasQuorumContact(LeaderSlot, Options.ElectionTimeout), state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout));

        state.RecordFollowerReply(3, Ready());

        _ = await Assert.That(twoFollowers).IsEqualTo((true, false));
        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsTrue();
    }

    /// <summary>Followers whose logs are not ready answer, which keeps the leader, but they refuse every ballot and are not counted.</summary>
    [Test]
    public async Task NotReadyAnswersAreNotCounted()
    {
        var (state, time) = Lead(3);
        time.Advance(Options.ElectionTimeout);
        state.RecordFollowerReply(1, Refusal(FollowerLogRefusal.NotReady));
        state.RecordFollowerReply(2, Refusal(FollowerLogRefusal.NotReady));
        var notReady = (state.HasQuorumContact(LeaderSlot, Options.ElectionTimeout), state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout));

        state.RecordFollowerReply(1, Refusal(FollowerLogRefusal.LogMismatch));
        state.RecordFollowerReply(2, Ready());

        _ = await Assert.That(notReady).IsEqualTo((true, false));
        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsTrue();
    }

    /// <summary>An incomplete follower votes, so a ready follower can win with its vote.</summary>
    [Test]
    public async Task IncompleteFollowerVotesForReadyOne()
    {
        var (state, time) = Lead(3);
        time.Advance(Options.ElectionTimeout);
        state.RecordFollowerReply(1, Ready());
        state.RecordFollowerReply(2, Refusal(FollowerLogRefusal.Incomplete));

        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsTrue();
    }

    /// <summary>Incomplete followers vote but none can stand, so nobody could win.</summary>
    [Test]
    public async Task IncompleteFollowersAloneHaveNoCandidate()
    {
        var (state, time) = Lead(3);
        time.Advance(Options.ElectionTimeout);
        state.RecordFollowerReply(1, Refusal(FollowerLogRefusal.Incomplete));
        state.RecordFollowerReply(2, Refusal(FollowerLogRefusal.Incomplete));

        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>A ready follower that answers incomplete later no longer stands, so two incomplete followers are no majority.</summary>
    [Test]
    public async Task LatestAnswerDecidesCandidate()
    {
        var (state, time) = Lead(3);
        time.Advance(Options.ElectionTimeout);
        state.RecordFollowerReply(1, Ready());
        state.RecordFollowerReply(2, Refusal(FollowerLogRefusal.Incomplete));
        state.RecordFollowerReply(1, Refusal(FollowerLogRefusal.Incomplete));

        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>With five replicas, one ready and two incomplete followers can elect a leader; three incomplete ones cannot.</summary>
    [Test]
    public async Task FiveReplicasNeedOneReadyVoter()
    {
        var (state, time) = Lead(5);
        time.Advance(Options.ElectionTimeout);
        state.RecordFollowerReply(1, Refusal(FollowerLogRefusal.Incomplete));
        state.RecordFollowerReply(2, Refusal(FollowerLogRefusal.Incomplete));
        state.RecordFollowerReply(3, Refusal(FollowerLogRefusal.Incomplete));
        var noCandidate = state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout);

        state.RecordFollowerReply(3, Ready());

        _ = await Assert.That(noCandidate).IsFalse();
        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsTrue();
    }

    /// <summary>A follower that is not ready refuses the ballot, so an incomplete one beside it is no majority.</summary>
    [Test]
    public async Task NotReadyBesideIncompleteIsNoMajority()
    {
        var (state, time) = Lead(3);
        time.Advance(Options.ElectionTimeout);
        state.RecordFollowerReply(1, Refusal(FollowerLogRefusal.NotReady));
        state.RecordFollowerReply(2, Refusal(FollowerLogRefusal.Incomplete));

        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>A new leader gets no grace: it is covered by the quorum check for one window, but nothing is electable until followers answered.</summary>
    [Test]
    public async Task NewLeaderHasNoElectableGrace()
    {
        var (state, _) = Lead(3);

        _ = await Assert.That(state.HasQuorumContact(LeaderSlot, Options.ElectionTimeout)).IsTrue();
        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>An answer older than the window, and the answers of an earlier leadership, no longer count.</summary>
    [Test]
    public async Task OldAnswersAreForgotten()
    {
        var (state, time) = Lead(3);
        state.RecordFollowerReply(1, Ready());
        state.RecordFollowerReply(2, Ready());
        var fresh = state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout);
        time.Advance(Options.ElectionTimeout);
        var aged = state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout);
        state.RecordFollowerReply(1, Ready());
        state.RecordFollowerReply(2, Ready());
        _ = state.BecomeLeader(LedTerm + 1UL);

        _ = await Assert.That((fresh, aged)).IsEqualTo((true, false));
        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>A reply of a higher term, or one received when this node does not lead, is not recorded as an electable contact.</summary>
    [Test]
    public async Task HigherTermOrFollowerReplyIsNotElectable()
    {
        var (state, _) = Lead(3);
        state.RecordFollowerReply(1, new FollowerLogAppendResult(true, string.Empty, LedTerm + 1UL, 0UL));
        state.RecordFollowerReply(2, new FollowerLogAppendResult(true, string.Empty, LedTerm + 1UL, 0UL));
        var higher = state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout);
        state.BecomeFollower(LedTerm + 1UL, false);
        state.RecordFollowerReply(1, Ready());
        state.RecordFollowerReply(2, Ready());

        _ = await Assert.That(higher).IsFalse();
        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>A late reply of a follower after the leadership changed hands is not recorded either: the new leadership starts with no contact.</summary>
    [Test]
    public async Task LateReplyAfterChangeIsNotElectable()
    {
        var (state, _) = Lead(3);
        state.BecomeFollower(LedTerm, false);
        state.RecordFollowerReply(1, Ready());
        state.RecordFollowerReply(2, Ready());
        _ = state.BecomeLeader(LedTerm + 1UL);

        _ = await Assert.That(state.ElectableContacts.HasMajority(LeaderSlot, Options.ElectionTimeout)).IsFalse();
    }

    private static FollowerLogAppendResult Ready() => new(true, string.Empty, LedTerm, 0UL);

    private static FollowerLogAppendResult Refusal(string code) => new(false, code, LedTerm, 0UL);

    private static (ReplicaGroupState State, FakeTimeProvider Time) Lead(int replicaCount)
    {
        var time = new FakeTimeProvider();
        var state = new ReplicaGroupState(replicaCount, Options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(LedTerm);
        return (state, time);
    }
}
