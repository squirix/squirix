using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader-side readiness verdicts of <see cref="ReplicaReadinessProbe" />.</summary>
[Immutable]
public sealed class ReplicaReadinessProbeTests
{
    private const string GroupId = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan NeverTimeout = TimeSpan.FromHours(1);

    /// <summary>The leader's own slot is verified from its durable log even while that log carries an uncommitted tail.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task LeaderWithTailIsMarkedReady()
    {
        var eligibility = new ReplicaEligibility(3);
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready);

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in leader, Fingerprint, 1);

        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }

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

        ReplicaReadinessProbe.RecordContacts(state, [default, new(ReplicaProbeKind.Refused, 0), new(ReplicaProbeKind.Unreachable, 0), default, default], 2UL);
        var silent = state.HasQuorumContact(0, options.ElectionTimeout);
        ReplicaReadinessProbe.RecordContacts(state, [default, new(ReplicaProbeKind.Accepted, 3), new(ReplicaProbeKind.LogMismatch, 1), default, default], 2UL);

        _ = await Assert.That(silent).IsFalse();
        _ = await Assert.That(state.HasQuorumContact(0, options.ElectionTimeout)).IsTrue();
    }

    /// <summary>The probes still pending are given up and canceled once enough followers answered from their logs.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task ProbeStopsOnceMajorityAnswered()
    {
        var followers = new ScriptedFollowers();
        followers.Answer("n2", new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 1, 1));
        var silent = followers.Hang("n3");

        var results = await ProbeAsync(followers, [false, true, true], 1, CancellationToken.None);

        _ = await Assert.That((results[0].Kind, results[1].Kind, results[2].Kind))
                        .IsEqualTo((ReplicaProbeKind.Unreachable, ReplicaProbeKind.LogMismatch, ReplicaProbeKind.Unreachable));
        _ = await Assert.That(silent.Task.IsCanceled).IsTrue();
    }

    /// <summary>Without enough answers every probe is awaited, so a slow follower still gets its verdict.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task ProbeWaitsForAllBelowMajority()
    {
        var followers = new ScriptedFollowers();
        followers.Fail("n2");
        var slow = followers.Hang("n3");

        var probing = ProbeAsync(followers, [false, true, true], 1, CancellationToken.None);
        var pendingBefore = !probing.IsCompleted;
        slow.SetResult(new FollowerLogAppendResult(true, string.Empty, 1, 3));
        var results = await probing;

        _ = await Assert.That(pendingBefore).IsTrue();
        _ = await Assert.That((results[1].Kind, results[2].Kind, results[2].LastLogIndex)).IsEqualTo((ReplicaProbeKind.Unreachable, ReplicaProbeKind.Accepted, 3UL));
    }

    /// <summary>Unreachable and refusing followers do not count as answers.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task UnreachableAnswersDoNotCount()
    {
        var followers = new ScriptedFollowers();
        followers.Answer("n2", new FollowerLogAppendResult(true, string.Empty, 1, 3));
        followers.Fail("n3");
        followers.Answer("n4", new FollowerLogAppendResult(false, RefusalCodes.StaleTerm, 1, 0));
        var slow = followers.Hang("n5");

        var probing = ProbeAsync(followers, [false, true, true, true, true], 2, CancellationToken.None);
        var pendingBefore = !probing.IsCompleted;
        slow.SetResult(new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 1, 2));
        var results = await probing;

        _ = await Assert.That(pendingBefore).IsTrue();
        _ = await Assert.That((results[1].Kind, results[2].Kind, results[3].Kind, results[4].Kind))
                        .IsEqualTo((ReplicaProbeKind.Accepted, ReplicaProbeKind.Unreachable, ReplicaProbeKind.Refused, ReplicaProbeKind.LogMismatch));
    }

    /// <summary>Cancellation of the caller propagates and leaves no probe running.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task CallerCancellationPropagates()
    {
        var followers = new ScriptedFollowers();
        var first = followers.Hang("n2");
        var second = followers.Hang("n3");
        using var cancellation = new CancellationTokenSource();

        var probing = ProbeAsync(followers, [false, true, true], 1, cancellation.Token);
        await cancellation.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(probing);
        _ = await Assert.That((first.Task.IsCanceled, second.Task.IsCanceled)).IsEqualTo((true, true));
    }

    /// <summary>A group whose ready slots already form a majority awaits every probe.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task MajorityAlreadyReadyWaitsForAll()
    {
        var eligibility = new ReplicaEligibility(3);
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in leader, Fingerprint, 1);
        var onlyLeader = eligibility.AnswersForMajority(0);

        ReplicaReadinessProbe.ApplyAll(eligibility, 0, [default, new(ReplicaProbeKind.Accepted, 3), default], in leader, Fingerprint, 1, null);

        _ = await Assert.That(onlyLeader).IsEqualTo(1);
        _ = await Assert.That(eligibility.AnswersForMajority(0)).IsEqualTo(int.MaxValue);
    }

    /// <summary>A majority of two replicas needs one follower answer beside the leader.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task TwoReplicasNeedOneAnswer()
    {
        var eligibility = ReadyLeader(2);

        _ = await Assert.That(eligibility.AnswersForMajority(0)).IsEqualTo(1);
    }

    /// <summary>A majority of five replicas needs two follower answers beside the leader.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task FiveReplicasNeedTwoAnswers()
    {
        var eligibility = ReadyLeader(5);

        _ = await Assert.That(eligibility.AnswersForMajority(0)).IsEqualTo(2);
    }

    /// <summary>A single replica is a majority by itself, so no answer is awaited.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task SingleReplicaNeedsNoAnswer()
    {
        var eligibility = ReadyLeader(1);

        _ = await Assert.That(eligibility.AnswersForMajority(0)).IsEqualTo(int.MaxValue);
    }

    /// <summary>A leader slot that does not count in the quorum leaves one more answer to wait for.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task UncountedLeaderNeedsOneMoreAnswer()
    {
        var counted = ReadyLeader(3);
        var uncounted = new ReplicaEligibility(3);

        _ = await Assert.That((counted.AnswersForMajority(0), uncounted.AnswersForMajority(0))).IsEqualTo((1, 2));
    }

    /// <summary>A probe that faults cancels the others and rethrows.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task FaultedProbeCancelsOthersAndRethrows()
    {
        var followers = new ScriptedFollowers();
        followers.Throw("n2");
        var silent = followers.Hang("n3");

        var probing = ProbeAsync(followers, [false, true, true], 2, CancellationToken.None);

        _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(probing);
        _ = await Assert.That(silent.Task.IsCanceled).IsTrue();
    }

    /// <summary>With no limit on the answers every follower is awaited and every verdict returned.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task UnlimitedBudgetReturnsEveryVerdict()
    {
        var followers = new ScriptedFollowers();
        followers.Answer("n2", new FollowerLogAppendResult(true, string.Empty, 1, 3));
        followers.Answer("n3", new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 1, 2));

        var results = await ProbeAsync(followers, [false, true, true], int.MaxValue, CancellationToken.None);

        _ = await Assert.That((results[1].Kind, results[2].Kind)).IsEqualTo((ReplicaProbeKind.Accepted, ReplicaProbeKind.LogMismatch));
    }

    /// <summary>A budget below one answer is refused, as it would give up every probe at once.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task BudgetBelowOneAnswerIsRefused()
    {
        var followers = new ScriptedFollowers();
        _ = followers.Hang("n2");

        var refused = await NodeAsyncAssert.ThrowsAsync<ArgumentOutOfRangeException>(ProbeAsync(followers, [false, true, false], 0, CancellationToken.None));

        _ = await Assert.That(refused.ParamName).IsEqualTo("budget.AnswersNeeded");
    }

    /// <summary>A follower given up at the launch of an elected leadership records no contact, so it cannot form a majority with the leader.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task GivenUpProbeRecordsNoContact()
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 5UL };
        var state = new ReplicaGroupState(5, options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        time.Advance(options.ElectionTimeout);
        var followers = new ScriptedFollowers();
        followers.Answer("n2", new FollowerLogAppendResult(true, string.Empty, 1, 3));
        _ = followers.Hang("n3");
        _ = followers.Hang("n4");
        _ = followers.Hang("n5");

        var results = await ProbeAsync(followers, [false, true, true, true, true], 1, CancellationToken.None);
        ReplicaReadinessProbe.RecordContacts(state, results, 2UL);
        var givenUp = state.HasQuorumContact(0, options.ElectionTimeout);
        state.RecordFollowerContact(2, 2UL);

        _ = await Assert.That(givenUp).IsFalse();
        _ = await Assert.That(state.HasQuorumContact(0, options.ElectionTimeout)).IsTrue();
    }

    private static ReplicaEligibility ReadyLeader(int replicaCount)
    {
        var eligibility = new ReplicaEligibility(replicaCount);
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in leader, Fingerprint, 1);
        return eligibility;
    }

    private static Task<ReplicaProbeResult[]> ProbeAsync(ScriptedFollowers followers, bool[] candidates, int answersNeeded, CancellationToken cancellationToken)
    {
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready);
        var header = new ReplicaRpcHeader(GroupId, Fingerprint, 1, 1, "n1", "n1");
        var members = new string[candidates.Length];
        for (var i = 0; i < members.Length; i++)
            members[i] = string.Create(CultureInfo.InvariantCulture, $"n{i + 1}");

        var budget = new ReplicaProbeBudget(NeverTimeout, answersNeeded);
        var probing = ReplicaReadinessProbe.ProbeAllAsync(followers.Gateway, candidates, members, header, leader, budget, cancellationToken);

        // A regression that waits for a silent follower must fail the test instead of hanging it.
        return probing.WaitAsync(HangGuard, TimeProvider.System, CancellationToken.None);
    }

    /// <summary>Answers each follower probe as scripted: at once, with a transport fault, or never until completed or canceled.</summary>
    private sealed class ScriptedFollowers
    {
        private readonly FollowerLogAppendResult?[] _answers = new FollowerLogAppendResult?[8];
        private readonly bool[] _faulty = new bool[8];
        private readonly bool[] _thrown = new bool[8];
        private readonly TaskCompletionSource<FollowerLogAppendResult>?[] _hanging = new TaskCompletionSource<FollowerLogAppendResult>?[8];

        internal ScriptedFollowers()
        {
            var expectations = new IReplicaRpcGatewayCreateExpectations();
            _ = expectations.Setups.AppendEntriesAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<FollowerBatch>(), Arg.Any<CancellationToken>())
                            .Callback((nodeId, _, _, cancellationToken) => AppendAsync(nodeId, cancellationToken));
            Gateway = expectations.Instance();
        }

        internal IReplicaRpcGateway Gateway { get; }

        internal void Answer(string nodeId, in FollowerLogAppendResult result) => _answers[SlotOf(nodeId)] = result;

        internal void Fail(string nodeId) => _faulty[SlotOf(nodeId)] = true;

        internal void Throw(string nodeId) => _thrown[SlotOf(nodeId)] = true;

        internal TaskCompletionSource<FollowerLogAppendResult> Hang(string nodeId)
        {
            var source = new TaskCompletionSource<FollowerLogAppendResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            _hanging[SlotOf(nodeId)] = source;
            return source;
        }

        private static int SlotOf(string nodeId) => nodeId[1] - '1';

        private Task<FollowerLogAppendResult> AppendAsync(string nodeId, CancellationToken cancellationToken)
        {
            var slot = SlotOf(nodeId);
            if (_answers[slot] is { } answer)
                return Task.FromResult(answer);

            if (_thrown[slot])
                return Task.FromException<FollowerLogAppendResult>(new NotSupportedException("Follower fault."));

            if (_faulty[slot])
                return Task.FromException<FollowerLogAppendResult>(new IOException("Follower unreachable."));

            var source = _hanging[slot]!;
            _ = cancellationToken.Register(() => source.TrySetCanceled(cancellationToken));
            return source.Task.WaitAsync(CancellationToken.None);
        }
    }
}
