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

        _ = await Assert.That((results[0].Kind, results[1].Kind, results[2].Kind)).IsEqualTo((ReplicaProbeKind.Unreachable, ReplicaProbeKind.LogMismatch, ReplicaProbeKind.Unreachable));
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

    private static Task<ReplicaProbeResult[]> ProbeAsync(ScriptedFollowers followers, bool[] candidates, int answersNeeded, CancellationToken cancellationToken)
    {
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready);
        var header = new ReplicaRpcHeader(GroupId, Fingerprint, 1, 1, "n1", "n1");
        var members = new string[candidates.Length];
        for (var i = 0; i < members.Length; i++)
            members[i] = string.Create(CultureInfo.InvariantCulture, $"n{i + 1}");

        return ReplicaReadinessProbe.ProbeAllAsync(followers.Gateway, candidates, members, header, leader, new ReplicaProbeBudget(NeverTimeout, answersNeeded), cancellationToken);
    }

    /// <summary>Answers each follower probe as scripted: at once, with a transport fault, or never until completed or canceled.</summary>
    private sealed class ScriptedFollowers
    {
        private readonly FollowerLogAppendResult?[] _answers = new FollowerLogAppendResult?[8];
        private readonly bool[] _faulty = new bool[8];
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

            if (_faulty[slot])
                return Task.FromException<FollowerLogAppendResult>(new IOException("Follower unreachable."));

            var source = _hanging[slot]!;
            _ = cancellationToken.Register(() => source.TrySetCanceled(cancellationToken));
            return source.Task.WaitAsync(CancellationToken.None);
        }
    }
}
