using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaReadinessProbeTests;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Election state changes caused by follower replies to log matching probes.</summary>
[Immutable]
public sealed class ReplicaProbeContactTests
{
    /// <summary>
    /// A follower that answered a start probe from its log counts as in contact with the elected leader; an unreachable or refusing one
    /// does not.
    /// </summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task ProbeAnswersCountAsContact()
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 5UL };
        var state = new ReplicaGroupState(5, options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        time.Advance(options.ElectionTimeout);

        ReplicaReadinessProbe.RecordReply(state, 1, 2UL, new FollowerLogAppendResult(false, RefusalCodes.NotReady, 2, 0));
        var silent = state.HasQuorumContact(0, options.ElectionTimeout);
        ReplicaReadinessProbe.RecordReply(state, 1, 2UL, new FollowerLogAppendResult(true, string.Empty, 2, 3));
        ReplicaReadinessProbe.RecordReply(state, 2, 2UL, new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 2, 1));

        _ = await Assert.That(silent).IsFalse();
        _ = await Assert.That(state.HasQuorumContact(0, options.ElectionTimeout)).IsTrue();
    }

    /// <summary>An incomplete follower can renew contact only for a probe whose term is at least the follower's term.</summary>
    /// <param name="replyTerm">The durable term returned by the follower.</param>
    /// <param name="expectedContact">Whether the probe should renew contact.</param>
    [Test]
    [Arguments(1UL, true)]
    [Arguments(2UL, false)]
    public async Task IncompleteProbeHonorsReplyTerm(ulong replyTerm, bool expectedContact)
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 5UL };
        var state = new ReplicaGroupState(3, options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(1UL);
        time.Advance(options.ElectionTimeout);
        var followers = new ScriptedFollowers();
        followers.Answer("n2", new FollowerLogAppendResult(false, RefusalCodes.Incomplete, replyTerm, 0));

        var results = await ProbeAsync(followers, [false, true, false], 1, CancellationToken.None, state);

        _ = await Assert.That((results[1].Kind, state.HasQuorumContact(0, options.ElectionTimeout)))
                        .IsEqualTo((ReplicaProbeKind.Incomplete, expectedContact));
    }

    /// <summary>A newer term in a probe refusal revokes authority while an older-term contact and a pending probe still exist.</summary>
    /// <param name="refusal">The refusal carrying the newer term.</param>
    [Test]
    [Arguments(RefusalCodes.Incomplete)]
    [Arguments(RefusalCodes.StaleTerm)]
    public async Task NewerProbeTermRevokesAuthority(string refusal)
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 5UL };
        var state = new ReplicaGroupState(5, options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(1UL);
        _ = await Assert.That(state.GrantAuthority(1UL)).IsTrue();
        var followers = new ScriptedFollowers();
        followers.Answer("n2", new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 1, 2));
        followers.Answer("n3", new FollowerLogAppendResult(false, refusal, 2, 0));
        var slow = followers.Hang("n4");

        var pending = ProbeAsync(followers, [false, true, true, true, false], int.MaxValue, CancellationToken.None, state);
        var observedWhileWaiting = (state.HighestObservedTerm, state.HasAuthority, pending.IsCompleted);
        slow.SetResult(new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 1, 2));
        _ = await pending;

        _ = await Assert.That(observedWhileWaiting).IsEqualTo((2UL, false, false));
        _ = await Assert.That(state.HasAuthority).IsFalse();
    }

    /// <summary>An already received probe reply ages while the round waits for a different replica.</summary>
    [Test]
    public async Task FastProbeContactExpiresDuringRound()
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 5UL };
        var state = new ReplicaGroupState(3, options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(1UL);
        time.Advance(options.ElectionTimeout);
        var followers = new ScriptedFollowers();
        followers.Answer("n2", new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 1, 2));
        var slow = followers.Hang("n3");

        var pending = ProbeAsync(followers, [false, true, true], int.MaxValue, CancellationToken.None, state);
        var contactWhileWaiting = state.HasQuorumContact(0, options.ElectionTimeout);
        time.Advance(options.ElectionTimeout);
        slow.SetResult(new FollowerLogAppendResult(false, RefusalCodes.NotReady, 1, 0));
        _ = await pending;

        _ = await Assert.That((contactWhileWaiting, state.HasQuorumContact(0, options.ElectionTimeout))).IsEqualTo((true, false));
    }
}
