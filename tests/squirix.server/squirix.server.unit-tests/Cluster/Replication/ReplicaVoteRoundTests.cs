using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
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

/// <summary>When a pre-vote or vote round of <see cref="ReplicaVoteRound" /> ends, and what it counts.</summary>
[Immutable]
public sealed class ReplicaVoteRoundTests
{
    private const ulong CandidateTerm = 3UL;

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly ElectionTimerOptions Timing = new() { VoteRpcTimeout = TimeSpan.FromMilliseconds(250), JitterSeed = 3UL };

    /// <summary>A round that already has a majority returns at once, and the call to the silent voter is canceled.</summary>
    /// <param name="preVote">Whether the round is a pre-vote.</param>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task WinDoesNotWaitForSilentVoter(bool preVote)
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", Grant(preVote));
        _ = voters.Hang("n3");
        var scope = new RoundScope(voters, 3);

        var outcome = await scope.RunAsync(preVote, CandidateTerm, CancellationToken.None);

        _ = await Assert.That(outcome).IsEqualTo((2, 0UL));
        _ = await Assert.That(voters.WasCanceled("n3")).IsTrue();
    }

    /// <summary>A round that can no longer reach a majority returns at once, and the call to the silent voter is canceled.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task DecidedLossDoesNotWaitForSilentVoter()
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", Refusal());
        voters.Fail("n3");
        voters.Answer("n4", Refusal());
        _ = voters.Hang("n5");
        var scope = new RoundScope(voters, 5);

        var outcome = await scope.RunAsync(false, CandidateTerm, CancellationToken.None);

        _ = await Assert.That(outcome).IsEqualTo((1, CandidateTerm));
        _ = await Assert.That(voters.WasCanceled("n5")).IsTrue();
    }

    /// <summary>Synchronous transport failures are no votes and do not disturb the tally of the grants that follow them.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task SyncFailuresDoNotDisturbTally()
    {
        var voters = new ScriptedVoters();
        voters.Fail("n2");
        voters.Fail("n3");
        voters.Answer("n4", Grant(false));
        voters.Answer("n5", Grant(false));
        var scope = new RoundScope(voters, 5);

        var outcome = await scope.RunAsync(false, CandidateTerm, CancellationToken.None);

        _ = await Assert.That(outcome).IsEqualTo((3, 0UL));
    }

    /// <summary>A win with two silent voters returns without them, and both calls are canceled.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task WinDoesNotWaitForTwoSilentVoters()
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", Grant(false));
        voters.Answer("n3", Grant(false));
        _ = voters.Hang("n4");
        _ = voters.Hang("n5");
        var scope = new RoundScope(voters, 5);

        var outcome = await scope.RunAsync(false, CandidateTerm, CancellationToken.None);

        _ = await Assert.That(outcome).IsEqualTo((3, 0UL));
        _ = await Assert.That((voters.WasCanceled("n4"), voters.WasCanceled("n5"))).IsEqualTo((true, true));
    }

    /// <summary>A round whose outcome depends on a silent voter waits for the vote timeout of that call.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task UndecidedRoundWaitsForVoteTimeout()
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", Refusal());
        _ = voters.Hang("n3");
        var scope = new RoundScope(voters, 3);

        var round = scope.RunAsync(false, CandidateTerm, CancellationToken.None);
        var pending = !round.IsCompleted;
        scope.Time.Advance(Timing.VoteRpcTimeout);
        var outcome = await round;

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(outcome).IsEqualTo((1, CandidateTerm));
    }

    /// <summary>A round that depends on a slow voter counts its grant when it arrives.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task UndecidedRoundCountsSlowGrant()
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", Refusal());
        var slow = voters.Hang("n3");
        var scope = new RoundScope(voters, 3);

        var round = scope.RunAsync(false, CandidateTerm, CancellationToken.None);
        var pending = !round.IsCompleted;
        slow.SetResult(Grant(false));
        var outcome = await round;

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(outcome).IsEqualTo((2, CandidateTerm));
    }

    /// <summary>A vote reply with a term above the candidate term ends the round and is reported.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task HigherTermEndsRound()
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", new FollowerLogVoteResult(false, "term", 7UL));
        _ = voters.Hang("n3");
        _ = voters.Hang("n4");
        _ = voters.Hang("n5");
        var scope = new RoundScope(voters, 5);

        var outcome = await scope.RunAsync(false, CandidateTerm, CancellationToken.None);

        _ = await Assert.That(outcome).IsEqualTo((1, 7UL));
        _ = await Assert.That((voters.WasCanceled("n3"), voters.WasCanceled("n4"), voters.WasCanceled("n5"))).IsEqualTo((true, true, true));
    }

    /// <summary>A pre-vote reply above the durable term of the candidate ends the round although it is below the proposed term.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task HigherTermEndsPreVoteRound()
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", new FollowerLogVoteResult(false, "term", 1UL));
        _ = voters.Hang("n3");
        var scope = new RoundScope(voters, 3);

        var outcome = await scope.RunAsync(true, 2UL, 0UL, CancellationToken.None);

        _ = await Assert.That(outcome).IsEqualTo((1, 1UL));
        _ = await Assert.That(voters.WasCanceled("n3")).IsTrue();
    }

    /// <summary>Cancellation of the caller ends the round by throwing.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task CallerCancellationPropagates()
    {
        var voters = new ScriptedVoters();
        _ = voters.Hang("n2");
        _ = voters.Hang("n3");
        var scope = new RoundScope(voters, 3);
        using var source = new CancellationTokenSource();

        var round = scope.RunAsync(false, CandidateTerm, source.Token);
        await source.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(round);
    }

    /// <summary>Cancellation of the caller throws although the gateway reports it as a transport cancellation.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task CallerCancelThrowsOnTransportCancel()
    {
        var voters = new ScriptedVoters();
        _ = voters.Hang("n2", true);
        _ = voters.Hang("n3", true);
        var scope = new RoundScope(voters, 3);
        using var source = new CancellationTokenSource();

        var round = scope.RunAsync(false, CandidateTerm, source.Token);
        await source.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(round);
    }

    /// <summary>An unexpected fault of a call cancels the others and is rethrown.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task FaultedCallCancelsOthersAndRethrows()
    {
        var voters = new ScriptedVoters();
        voters.Throw("n2");
        _ = voters.Hang("n3");
        var scope = new RoundScope(voters, 3);

        var round = scope.RunAsync(false, CandidateTerm, CancellationToken.None);

        _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(round);
        _ = await Assert.That(voters.WasCanceled("n3")).IsTrue();
    }

    /// <summary>A call that ignores its cancellation is awaited, and the term it reports is observed.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task GivenUpCallIsAwaitedAndItsTermObserved()
    {
        var voters = new ScriptedVoters();
        voters.Answer("n2", Grant(false));
        var stubborn = voters.HangIgnoringToken("n3");
        var scope = new RoundScope(voters, 3);

        var round = scope.RunAsync(false, CandidateTerm, CancellationToken.None);
        var pending = !round.IsCompleted;
        stubborn.SetResult(new FollowerLogVoteResult(false, "term", 9UL));
        var outcome = await round;

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(outcome).IsEqualTo((2, 9UL));
    }

    private static FollowerLogVoteResult Grant(bool preVote) => new(true, string.Empty, preVote ? 0UL : CandidateTerm);

    private static FollowerLogVoteResult Refusal() => new(false, "refused", CandidateTerm);

    /// <summary>A round of <c language="text">n1</c> over members <c language="text">n1</c> upward, on a fake clock.</summary>
    private sealed class RoundScope
    {
        private readonly ReplicaVoteRound _round;

        internal RoundScope(ScriptedVoters voters, int members)
        {
            var ids = new string[members];
            for (var i = 0; i < ids.Length; i++)
                ids[i] = "n" + (i + 1).ToString(CultureInfo.InvariantCulture);

            Time = new FakeTimeProvider();
            _round = new ReplicaVoteRound(voters.Gateway, ids, 0, new ReplicaGroupState(members, Timing, Time));
        }

        internal FakeTimeProvider Time { get; }

        internal Task<(int Granted, ulong Highest)> RunAsync(bool preVote, ulong term, CancellationToken cancellationToken) =>
            RunAsync(preVote, term, term, cancellationToken);

        internal Task<(int Granted, ulong Highest)> RunAsync(bool preVote, ulong term, ulong ownTerm, CancellationToken cancellationToken)
        {
            var header = new ReplicaRpcHeader("g", Fingerprint, 1UL, term, string.Empty, "n1");
            var running = _round.RunAsync(preVote, header, ownTerm, (0UL, 0UL), cancellationToken);

            // A regression that waits for a silent voter must fail the test instead of hanging it.
            return running.WaitAsync(HangGuard, TimeProvider.System, CancellationToken.None);
        }
    }

    /// <summary>Answers each vote call as scripted: at once, with a transport fault, with an unexpected fault, or never until completed or canceled.</summary>
    private sealed class ScriptedVoters
    {
        private readonly FollowerLogVoteResult?[] _answers = new FollowerLogVoteResult?[8];
        private readonly bool[] _canceled = new bool[8];
        private readonly bool[] _faulty = new bool[8];
        private readonly TaskCompletionSource<FollowerLogVoteResult>?[] _hanging = new TaskCompletionSource<FollowerLogVoteResult>?[8];
        private readonly bool[] _ignoring = new bool[8];
        private readonly bool[] _thrown = new bool[8];
        private readonly bool[] _transportCancel = new bool[8];

        internal ScriptedVoters()
        {
            var expectations = new IReplicaVoteGatewayCreateExpectations();
            _ = expectations.Setups.PreVoteAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
                            .Callback((nodeId, _, _, _, cancellationToken) => AskAsync(nodeId, cancellationToken));
            _ = expectations.Setups.RequestVoteAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
                            .Callback((nodeId, _, _, _, cancellationToken) => AskAsync(nodeId, cancellationToken));
            Gateway = expectations.Instance();
        }

        internal IReplicaVoteGateway Gateway { get; }

        internal void Answer(string nodeId, in FollowerLogVoteResult result) => _answers[SlotOf(nodeId)] = result;

        internal void Fail(string nodeId) => _faulty[SlotOf(nodeId)] = true;

        internal void Throw(string nodeId) => _thrown[SlotOf(nodeId)] = true;

        internal bool WasCanceled(string nodeId) => Volatile.Read(ref _canceled[SlotOf(nodeId)]);

        internal TaskCompletionSource<FollowerLogVoteResult> Hang(string nodeId, bool transportCancel = false)
        {
            var source = new TaskCompletionSource<FollowerLogVoteResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            _hanging[SlotOf(nodeId)] = source;
            _transportCancel[SlotOf(nodeId)] = transportCancel;
            return source;
        }

        internal TaskCompletionSource<FollowerLogVoteResult> HangIgnoringToken(string nodeId)
        {
            _ignoring[SlotOf(nodeId)] = true;
            return Hang(nodeId);
        }

        private static int SlotOf(string nodeId) => nodeId[1] - '1';

        private Task<FollowerLogVoteResult> AskAsync(string nodeId, CancellationToken cancellationToken)
        {
            var slot = SlotOf(nodeId);
            if (_answers[slot] is { } answer)
                return Task.FromResult(answer);

            if (_thrown[slot])
                return Task.FromException<FollowerLogVoteResult>(new NotSupportedException("Voter fault."));

            if (_faulty[slot])
                return Task.FromException<FollowerLogVoteResult>(new IOException("Voter unreachable."));

            var source = _hanging[slot]!;
            if (_ignoring[slot])
                return source.Task.WaitAsync(CancellationToken.None);

            _ = cancellationToken.Register(() => Cancel(slot, source, cancellationToken));
            return source.Task.WaitAsync(CancellationToken.None);
        }

        private void Cancel(int slot, TaskCompletionSource<FollowerLogVoteResult> source, CancellationToken cancellationToken)
        {
            Volatile.Write(ref _canceled[slot], true);
            _ = _transportCancel[slot]
                ? source.TrySetException(new RpcException(new Status(StatusCode.Cancelled, "Call canceled.")))
                : source.TrySetCanceled(cancellationToken);
        }
    }
}
