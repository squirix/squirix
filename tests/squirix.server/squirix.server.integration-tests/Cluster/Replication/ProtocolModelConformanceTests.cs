using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.ProtocolModel;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Checks production durable boundaries against the protocol transition system.</summary>
public sealed class ProtocolModelConformanceTests : NodeIntegrationTestBase
{
    private const int AllCopies = 3;
    private const int CommitLeaderSlot = 0;
    private const int CommitLastLogTerm = 1;
    private const ulong CommitTerm = 1UL;
    private const int ElectionLastLogTerm = 2;
    private const int ElectionLeaderSlot = 1;
    private const ulong ElectionTerm = 2UL;
    private const ulong FirstIndex = 1UL;
    private const int OwnVoteOnly = 1;
    private const int ReadConfirmations = 2;
    private const int ReadIndexValue = 1;
    private const ulong SecondIndex = 2UL;
    private const int StaleTermSlot = 1;
    private const int TailFollowerSlot = 1;
    private const int TailCommitCopies = 2;
    private const int TailMaxInFlight = 1;
    private const int TailLogEntries = 2;
    private const int TailMaxTerm = 2;
    private const int TailStateBudget = 300_000;
    private const int ThreeVotes = 3;
    private const int UnavailableSlot = 2;
    private static readonly TimeSpan CommitBudget = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan WaitBound = TimeSpan.FromSeconds(30);

    /// <summary>A production commit trace follows a path accepted by the protocol model.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionCommitTraceMatchesModel(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline { LeaderSlot = CommitLeaderSlot };
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));

        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(FirstIndex), CommitBudget, cancellationToken);

        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline);
        await SequenceAssert.EqualAsync(
            [
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 0, 0, CommitLeaderSlot, CommitLastLogTerm),
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 1, 0, CommitLeaderSlot, CommitLastLogTerm, Acks: AllCopies),
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 1, 1, CommitLeaderSlot, CommitLastLogTerm),
            ],
            pipeline.Trace);
    }

    /// <summary>
    /// A real election round of a group driver wins the next term through the vote path of its own log, and the leader-term entry its
    /// promotion commits in that term follows a path the protocol model accepts.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionElectionTraceMatchesModel(CancellationToken cancellationToken)
    {
        var run = await RunElectionAsync(ConformanceTestKit.VoteReply.Grant, ConformanceTestKit.VoteReply.Grant, cancellationToken);

        await ConformanceTestKit.AssertModelAcceptedAsync(run.Pipeline);
        _ = await Assert.That(run.Outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, ElectionTerm));
        _ = await Assert.That((run.Term, run.VotedFor, run.HasAuthority)).IsEqualTo((ElectionTerm, "node-a", true));
        await SequenceAssert.EqualAsync(
            [
                new ConformanceTestKit.TracePoint(ElectionTerm, FirstIndex, 0, 0, ElectionLeaderSlot, ElectionLastLogTerm, ThreeVotes),
                new ConformanceTestKit.TracePoint(ElectionTerm, FirstIndex, 1, 0, ElectionLeaderSlot, ElectionLastLogTerm, ThreeVotes, AllCopies),
                new ConformanceTestKit.TracePoint(ElectionTerm, FirstIndex, 1, 1, ElectionLeaderSlot, ElectionLastLogTerm, ThreeVotes),
            ],
            run.Pipeline.Trace);
    }

    /// <summary>
    /// A candidate whose only counted vote is its own, because one grant reports an older term and the other peer refuses, wins nothing, so
    /// the protocol model sees no leader of the term.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinorityElectionTraceMatchesModel(CancellationToken cancellationToken)
    {
        var run = await RunElectionAsync(ConformanceTestKit.VoteReply.GrantStaleTerm, ConformanceTestKit.VoteReply.Refuse, cancellationToken);

        await ConformanceTestKit.AssertModelAcceptedAsync(run.Pipeline, true);
        _ = await Assert.That(run.Votes.CountVotes()).IsEqualTo(OwnVoteOnly);
        _ = await Assert.That(run.Outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.VoteLost, ElectionTerm));
        _ = await Assert.That((run.Term, run.VotedFor, run.HasAuthority)).IsEqualTo((ElectionTerm, "node-a", false));
        _ = await Assert.That(run.Pipeline.Trace).IsEmpty();
    }

    /// <summary>
    /// A leader whose followers gave no valid acknowledgement, one in an older term and one unavailable, commits nothing, so its trace holds
    /// no commit step and the protocol model accepts it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinorityCommitTraceMatchesModel(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline(unavailableReplica: UnavailableSlot, staleTermReplica: StaleTermSlot) { LeaderSlot = CommitLeaderSlot };
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));

        var commit = coordinator.CommitAsync(ConformanceTestKit.CreateMutation(FirstIndex), CommitBudget, cancellationToken).AsTask();
        await WaitForEndAsync(commit, cancellationToken);

        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(commit);
        _ = await Assert.That(pipeline.CommitIndex).IsEqualTo(0UL);
        await SequenceAssert.EqualAsync(
            [new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 0, 0, CommitLeaderSlot, CommitLastLogTerm)],
            pipeline.Trace);
    }

    /// <summary>
    /// A real read-index round confirmed by the leader and one follower, and an applied-index wait that parks on the parked apply, serve a read the
    /// protocol model accepts.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionQuorumReadTraceMatchesModel(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline(parkApply: true) { LeaderSlot = CommitLeaderSlot };
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));
        var commit = coordinator.CommitAsync(ConformanceTestKit.CreateMutation(FirstIndex), CommitBudget, cancellationToken).AsTask();
        var probe = new ConformanceTestKit.ReadProbe(
            CommitTerm,
            CommitLeaderSlot,
            [ConformanceTestKit.FollowerReply.Answer, ConformanceTestKit.FollowerReply.Answer, ConformanceTestKit.FollowerReply.Busy]);
        ulong readIndex;
        try
        {
            await pipeline.ApplyParked.WaitAsync(WaitBound, TimeProvider.System, cancellationToken);
            var read = probe.StartConfirmAsync(pipeline, cancellationToken);
            await probe.SettleAsync().WaitAsync(WaitBound, TimeProvider.System, cancellationToken);
            readIndex = await read.WaitAsync(WaitBound, TimeProvider.System, cancellationToken);
            await ServeReadAsync(pipeline, probe, readIndex, commit, cancellationToken);
        }
        finally
        {
            pipeline.ReleaseApply();
        }

        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline);
        _ = await Assert.That((readIndex, probe.CountConfirmations())).IsEqualTo((FirstIndex, ReadConfirmations));
        await SequenceAssert.EqualAsync(
            [
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 0, 0, CommitLeaderSlot, CommitLastLogTerm),
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 1, 0, CommitLeaderSlot, CommitLastLogTerm, Acks: AllCopies),
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 1, 1, CommitLeaderSlot, CommitLastLogTerm),
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 1, 1, CommitLeaderSlot, CommitLastLogTerm, ReadIndex: ReadIndexValue, ReadAcks: ReadConfirmations),
            ],
            pipeline.Trace);
    }

    /// <summary>
    /// A follower reply in an older term and a reply to a request sent before the round started confirm nothing, so the round stays open
    /// until it is closed and the read is refused; the protocol model accepts the trace of commit steps alone.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleReplyReadTraceMatchesModel(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline { LeaderSlot = CommitLeaderSlot };
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(FirstIndex), CommitBudget, cancellationToken);
        var probe = new ConformanceTestKit.ReadProbe(
            CommitTerm,
            CommitLeaderSlot,
            [ConformanceTestKit.FollowerReply.Answer, ConformanceTestKit.FollowerReply.StaleTerm, ConformanceTestKit.FollowerReply.Busy]);

        var read = probe.StartConfirmAsync(pipeline, cancellationToken);
        await probe.SettleAsync().WaitAsync(WaitBound, TimeProvider.System, cancellationToken);
        await WaitForEndAsync(read, cancellationToken);

        // A read served anyway is a read the round confirmed without a valid answer; it enters the trace with its confirmations.
        if (read.IsCompletedSuccessfully)
            pipeline.RecordRead(await read, probe.CountConfirmations());

        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline);
        _ = await NodeAsyncAssert.ThrowsAsync<RpcException>(read);
        await SequenceAssert.EqualAsync(
            [
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 0, 0, CommitLeaderSlot, CommitLastLogTerm),
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 1, 0, CommitLeaderSlot, CommitLastLogTerm, Acks: AllCopies),
                new ConformanceTestKit.TracePoint(CommitTerm, FirstIndex, 1, 1, CommitLeaderSlot, CommitLastLogTerm),
            ],
            pipeline.Trace);
    }

    /// <summary>
    /// A leader of term two whose log holds an entry of term one, recovered at start and backed by a follower, does not commit it by counting
    /// copies; it commits it together with the entry it appends in its own term, and the protocol model accepts that path.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredTailTraceMatchesModel(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline(unavailableReplica: UnavailableSlot)
        {
            LeaderSlot = CommitLeaderSlot,
            LeaderTerm = ElectionTerm,
            RecoveredLogIndex = FirstIndex,
            RecoveredLogTerm = CommitTerm,
            RecoveredHolders = [TailFollowerSlot],
            ReportsApplied = false,
        };
        var tail = ConformanceTestKit.CreateRecoveredTail(CommitTerm, ElectionTerm, FirstIndex);
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch), recoveredTail: tail);

        ConformanceTestKit.AdmitTailHolders(coordinator, pipeline);
        var tailCommitted = await coordinator.ApplyCommittedAsync();
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(SecondIndex, ElectionTerm), CommitBudget, cancellationToken);

        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline, false, ExploreProfile.ForLogRules(TailMaxTerm, TailLogEntries, TailMaxInFlight, TailStateBudget));
        _ = await Assert.That(tailCommitted).IsFalse();
        _ = await Assert.That(pipeline.CommitIndex).IsEqualTo(SecondIndex);
        await SequenceAssert.EqualAsync(
            [
                new ConformanceTestKit.TracePoint(ElectionTerm, SecondIndex, 0, ConformanceTestKit.NotObserved, CommitLeaderSlot, ElectionLastLogTerm),
                new ConformanceTestKit.TracePoint(ElectionTerm, SecondIndex, SecondIndex, ConformanceTestKit.NotObserved, CommitLeaderSlot, ElectionLastLogTerm, Acks: TailCommitCopies),
                new ConformanceTestKit.TracePoint(ElectionTerm, SecondIndex, SecondIndex, ConformanceTestKit.NotObserved, CommitLeaderSlot, ElectionLastLogTerm),
                new ConformanceTestKit.TracePoint(ElectionTerm, SecondIndex, SecondIndex, ConformanceTestKit.NotObserved, CommitLeaderSlot, ElectionLastLogTerm),
            ],
            pipeline.Trace);
    }

    /// <summary>Production conformance pins the protocol model version it was verified against.</summary>
    /// <remarks>
    /// Update the pinned hash only together with a model transition or invariant change;
    /// a silent drift between the verified model and production is a conformance failure.
    /// </remarks>
    [Test]
    public async Task ProtocolVersionMatchesModelManifest() => _ = await Assert.That(ExploreRunner.ModelVersionHash).IsEqualTo("45b7e3f043eb22a8");

    /// <summary>Commits the leader-term entry of a won term at the first index, as a promotion does.</summary>
    /// <param name="coordinator">The commit coordinator.</param>
    /// <param name="term">The won term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> once the entry is committed.</returns>
    private static async Task<bool> CommitLeaderTermAsync(ReplicaCommitCoordinator coordinator, ulong term, CancellationToken cancellationToken)
    {
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(FirstIndex, term), CommitBudget, cancellationToken);
        return true;
    }

    /// <summary>Runs the pre-vote and vote rounds of a node that follows term one, and returns what the election and its promotion left.</summary>
    /// <param name="nodeB">What peer node-b answers to a vote request.</param>
    /// <param name="nodeC">What peer node-c answers to a vote request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The run.</returns>
    private static async Task<ElectionRun> RunElectionAsync(ConformanceTestKit.VoteReply nodeB, ConformanceTestKit.VoteReply nodeC, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-trace");
        await using var log = new FollowerLog(dir, "election-trace", GroupComposition.Create("election-trace"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        var recorder = new ConformanceTestKit.VoteRecorder(new Dictionary<string, ConformanceTestKit.VoteReply>(StringComparer.Ordinal) { ["node-b"] = nodeB, ["node-c"] = nodeC });
        var votes = new IReplicaVoteGatewayCreateExpectations();
        _ = votes.Setups.PreVoteAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
                 .ReturnValue(Task.FromResult(new FollowerLogVoteResult(true, string.Empty, 0UL)));

        // The promotion commits the leader-term entry of the won term at the first index, through the production commit coordinator.
        var pipeline = new ConformanceTestKit.Pipeline { LeaderSlot = ElectionLeaderSlot, LeaderTerm = ElectionTerm, Votes = recorder };
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));
        var leadership = new IReplicaLeadershipCreateExpectations();
        var (requestVote, promote) =
            Bind.To<(ConformanceTestKit.VoteRecorder Recorder, ReplicaCommitCoordinator Coordinator), (
                Func<string, ReplicaRpcHeader, ulong, ulong, CancellationToken, Task<FollowerLogVoteResult>>, Func<string, ulong, CancellationToken, Task<bool>>)>(
                (recorder, coordinator),
                static shared => ((nodeId, header, _, _, _) => shared.Recorder.RequestVoteAsync(nodeId, header.Term),
                    (_, term, token) => CommitLeaderTermAsync(shared.Coordinator, term, token)));
        _ = votes.Setups.RequestVoteAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>()).Callback(requestVote);
        _ = leadership.Setups.PromoteAsync(Arg.Any<string>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>()).Callback(promote);

        var time = new FakeTimeProvider();
        var timing = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), MaxJitter = TimeSpan.Zero, JitterSeed = 3UL };
        var state = new ReplicaGroupState(ConformanceTestKit.ReplicaCount, timing, time);
        state.ApplyBlock.Assess();
        var election = new ReplicaGroupElection(
            state,
            log,
            votes.Instance(),
            leadership.Instance(),
            ["node-b", "node-a", "node-c"],
            new ReplicaRpcHeader("election-trace", ReadOnlyMemory<byte>.Of(9), 1UL, 0UL, string.Empty, "node-a"));
        _ = await election.StepAsync(cancellationToken);
        time.Advance(timing.ElectionTimeout);
        var outcome = await election.StepAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        return new ElectionRun(pipeline, recorder, outcome, status.CurrentTerm, status.VotedFor, state.HasAuthority);
    }

    /// <summary>Serves a read through the real applied-index wait and records the read at the applied index it was served at.</summary>
    /// <param name="pipeline">The pipeline of the leader.</param>
    /// <param name="probe">The read probe whose round confirmed the read.</param>
    /// <param name="readIndex">The read index.</param>
    /// <param name="commit">The commit whose apply the pipeline may park; the apply is released before it is awaited.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task ServeReadAsync(
        ConformanceTestKit.Pipeline pipeline,
        ConformanceTestKit.ReadProbe probe,
        ulong readIndex,
        Task commit,
        CancellationToken cancellationToken)
    {
        var applier = new ReplicaGroupApplier(new ILogicalNamespacedCacheCreateExpectations<object?>().Instance(), NullLogger.Instance);
        applier.ResetTo(pipeline.AppliedIndex);
        var wait = applier.WaitAppliedAsync(readIndex, cancellationToken);

        // A wait that is already done served the read before the apply, whatever the read index asked for.
        var servedBeforeApply = wait.IsCompleted;
        if (servedBeforeApply)
            pipeline.RecordRead(readIndex, probe.CountConfirmations());

        pipeline.ReleaseApply();
        await commit.WaitAsync(WaitBound, TimeProvider.System, cancellationToken);
        if (servedBeforeApply)
            return;

        applier.ResetTo(pipeline.AppliedIndex);
        await wait.WaitAsync(WaitBound, TimeProvider.System, cancellationToken);
        pipeline.RecordRead(readIndex, probe.CountConfirmations());
    }

    /// <summary>Waits, bounded, until a task has ended, whatever its outcome.</summary>
    /// <param name="task">The task.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The task did not end within the bound.</exception>
    private static async Task WaitForEndAsync(Task task, CancellationToken cancellationToken)
    {
        await task.WaitAsync(WaitBound, TimeProvider.System, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        if (!task.IsCompleted)
            throw new TimeoutException("The scenario did not end within the bound.");
    }

    private sealed record ElectionRun(
        ConformanceTestKit.Pipeline Pipeline,
        ConformanceTestKit.VoteRecorder Votes,
        ElectionOutcome Outcome,
        ulong Term,
        string VotedFor,
        bool HasAuthority);
}
