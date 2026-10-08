using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The election state of a group: contacts that keep a leader, higher terms that depose it, and the authority it may hold.</summary>
[Immutable]
public sealed class ReplicaGroupStateTests : ServerUnitTestBase
{
    private static readonly ElectionTimerOptions Options = new() { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 7UL };

    /// <summary>A leader contact counts as live for one election timeout and names the leader; then it expires.</summary>
    [Test]
    public async Task LeaderContactExpiresAfterElectionTimeout()
    {
        var time = new FakeTimeProvider();
        var state = new ReplicaGroupState(3, Options, time);

        state.ObserveLeaderContact("n2", 4UL);
        time.Advance(TimeSpan.FromMilliseconds(499));
        var live = state.HasRecentLeaderContact(Options.ElectionTimeout);
        time.Advance(TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(live).IsTrue();
        _ = await Assert.That(state.HasRecentLeaderContact(Options.ElectionTimeout)).IsFalse();
        _ = await Assert.That(state.TryGetKnownLeader(out var leader, out var term)).IsTrue();
        _ = await Assert.That((leader, term, state.HighestObservedTerm)).IsEqualTo(("n2", 4UL, 4UL));
    }

    /// <summary>A granted vote postpones the own election but does not count as a leader, so pre-votes are still answered.</summary>
    [Test]
    public async Task GrantedVotePostponesElectionOnly()
    {
        var time = new FakeTimeProvider();
        var state = new ReplicaGroupState(3, Options, time);

        state.ObserveGrantedVote();

        _ = await Assert.That(state.ElectionResetTimestamp()).IsEqualTo(time.GetTimestamp());
        _ = await Assert.That(state.HasRecentLeaderContact(Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>A term above the driver's wakes the driver at once and keeps the leader from gaining authority in its own term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermWakesDriverAndBlocksAuthority(CancellationToken cancellationToken)
    {
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);

        state.ObserveHigherTerm(3UL);

        _ = await Assert.That(await state.WaitAsync(TimeSpan.Zero, cancellationToken)).IsTrue();
        _ = await Assert.That(state.GrantAuthority(2UL)).IsFalse();
        _ = await Assert.That((state.Role, state.HasAuthority)).IsEqualTo((ReplicaGroupRole.Leader, false));
    }

    /// <summary>Without a driver a higher term is recorded but wakes nothing, and the group stays a follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermWithoutDriverWakesNothing(CancellationToken cancellationToken)
    {
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());

        state.ObserveHigherTerm(3UL);

        _ = await Assert.That(await state.WaitAsync(TimeSpan.Zero, cancellationToken)).IsFalse();
        _ = await Assert.That((state.Role, state.HighestObservedTerm)).IsEqualTo((ReplicaGroupRole.Follower, 3UL));
    }

    /// <summary>
    /// A new leader keeps its quorum for one window; after it, only followers that answered within the window count, and a reply of a
    /// higher term is no contact.
    /// </summary>
    [Test]
    public async Task QuorumContactNeedsRecentMajority()
    {
        var time = new FakeTimeProvider();
        var state = new ReplicaGroupState(3, Options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        var fresh = state.HasQuorumContact(0, Options.ElectionTimeout);
        time.Advance(Options.ElectionTimeout);
        var silent = state.HasQuorumContact(0, Options.ElectionTimeout);

        state.RecordFollowerContact(2, 3UL);
        var deposing = state.HasQuorumContact(0, Options.ElectionTimeout);
        state.RecordFollowerContact(1, 2UL);

        _ = await Assert.That((fresh, silent, deposing)).IsEqualTo((true, false, false));
        _ = await Assert.That(state.HasQuorumContact(0, Options.ElectionTimeout)).IsTrue();
        _ = await Assert.That(state.HighestObservedTerm).IsEqualTo(3UL);
    }

    /// <summary>A higher term from any source revokes the authority of a leader at once, before its driver even wakes.</summary>
    [Test]
    public async Task HigherTermRevokesAuthorityAtOnce()
    {
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        var granted = state.GrantAuthority(2UL);

        state.RecordFollowerReply(1, new FollowerLogAppendResult(false, RefusalCodes.StaleTerm, 3UL, 0UL));

        _ = await Assert.That(granted).IsTrue();
        _ = await Assert.That((state.Role, state.HasAuthority)).IsEqualTo((ReplicaGroupRole.Leader, false));
        _ = await Assert.That(state.HasRecentLeaderContact(Options.ElectionTimeout)).IsFalse();
    }

    /// <summary>Once the driver stopped, a step that finishes late can neither lead nor gain authority.</summary>
    [Test]
    public async Task StoppedDriverCannotLead()
    {
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());
        state.SetElectionDriven(true);
        var led = state.BecomeLeader(2UL);
        state.SetElectionDriven(false);

        var authorized = state.GrantAuthority(2UL);
        var ledAgain = state.BecomeLeader(3UL);

        _ = await Assert.That((led, authorized, ledAgain)).IsEqualTo((true, false, false));
        _ = await Assert.That((state.Role, state.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
    }

    /// <summary>
    /// A follower answer from its log counts as contact, a not-ready log included; a refusal before the log does not, though its term is
    /// still observed.
    /// </summary>
    [Test]
    public async Task OnlyLogAnswersCountAsContact()
    {
        var time = new FakeTimeProvider();
        var state = new ReplicaGroupState(3, Options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        time.Advance(Options.ElectionTimeout);

        state.RecordFollowerReply(1, new FollowerLogAppendResult(false, RefusalCodes.TopologyMismatch, 0UL, 0UL));
        var mismatched = state.HasQuorumContact(0, Options.ElectionTimeout);
        state.RecordFollowerReply(2, new FollowerLogAppendResult(false, RefusalCodes.NotMember, 1UL, 0UL));
        state.RecordFollowerReply(1, new FollowerLogAppendResult(false, RefusalCodes.NotReady, 2UL, 0UL));

        _ = await Assert.That(mismatched).IsFalse();
        _ = await Assert.That(state.HasQuorumContact(0, Options.ElectionTimeout)).IsTrue();
        _ = await Assert.That(state.HighestObservedTerm).IsEqualTo(2UL);
    }

    /// <summary>Authority is granted only to the leader of the term, and stepping down clears it before anything else.</summary>
    [Test]
    public async Task AuthorityBelongsToLeaderOfTerm()
    {
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());
        state.SetElectionDriven(true);
        var follower = state.GrantAuthority(1UL);
        _ = state.BecomeLeader(2UL);
        var otherTerm = state.GrantAuthority(3UL);
        var granted = state.GrantAuthority(2UL);
        var leading = state.HasRecentLeaderContact(Options.ElectionTimeout);

        state.BecomeFollower(2UL, false);

        _ = await Assert.That((follower, otherTerm, granted, leading)).IsEqualTo((false, false, true, true));
        _ = await Assert.That((state.Role, state.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
    }
}
