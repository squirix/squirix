using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.ProtocolModel;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Checks production durable boundaries against the protocol transition system.</summary>
public sealed class ProtocolModelConformanceTests : NodeIntegrationTestBase
{
    /// <summary>A production commit trace follows a path accepted by the protocol model.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionCommitTraceMatchesModel(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));

        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);

        await SequenceAssert.EqualAsync(
            [
                new ConformanceTestKit.TracePoint(1, 1, 0, 0),
                new ConformanceTestKit.TracePoint(1, 1, 1, 0),
                new ConformanceTestKit.TracePoint(1, 1, 1, 1),
            ],
            pipeline.Trace);
        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline.Trace);
    }

    /// <summary>
    /// A real election round of a group driver wins the next term through the vote path of its own log, and the leader-term entry its
    /// promotion commits in that term follows a path the protocol model accepts.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionElectionTraceMatchesModel(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-trace");
        await using var log = new FollowerLog(dir, "election-trace", GroupComposition.Create("election-trace"), NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        var votes = new IReplicaVoteGatewayCreateExpectations();
        _ = votes.Setups.PreVoteAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
                 .ReturnValue(Task.FromResult(new FollowerLogVoteResult(true, string.Empty, 0UL)));
        _ = votes.Setups.RequestVoteAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
                 .Callback(static (_, header, _, _, _) => Task.FromResult(new FollowerLogVoteResult(true, string.Empty, header.Term)));

        // The promotion commits the leader-term entry of the won term at the first index, through the production commit coordinator.
        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));
        var leadership = new IReplicaLeadershipCreateExpectations();
        _ = leadership.Setups.PromoteAsync(Arg.Any<string>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
                      .Callback((_, term, token) => CommitLeaderTermAsync(coordinator, term, token));

        var time = new FakeTimeProvider();
        var timing = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), MaxJitter = TimeSpan.Zero, JitterSeed = 3UL };
        var state = new ReplicaGroupState(3, timing, time);
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
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 2UL));
        _ = await Assert.That((status.CurrentTerm, status.VotedFor, state.HasAuthority)).IsEqualTo((2UL, "node-a", true));
        await SequenceAssert.EqualAsync(
            [
                new ConformanceTestKit.TracePoint(2, 1, 0, 0),
                new ConformanceTestKit.TracePoint(2, 1, 1, 0),
                new ConformanceTestKit.TracePoint(2, 1, 1, 1),
            ],
            pipeline.Trace);
        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline.Trace);
    }

    /// <summary>Production quorum-read gate and commit trace follow the model read path.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionQuorumReadTraceMatchesModel(CancellationToken cancellationToken)
    {
        var allowed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(allowed.Allowed).IsTrue();

        // The barrier parks on the fake clock below the read index, then serves once applied.
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var applied = new StrongBox<ulong>(4);
        var wait = LeaderReadBarrier.WaitUntilAppliedAsync(() => applied.Value, 9UL, time, TimeSpan.FromMilliseconds(10), cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        applied.Value = 9UL;
        time.Advance(TimeSpan.FromMilliseconds(10));
        await wait;

        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, new FakeTimeProvider(DateTimeOffset.UnixEpoch));
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);
        await ConformanceTestKit.AssertModelAcceptedAsync(pipeline.Trace);
    }

    /// <summary>Production conformance pins the protocol model version it was verified against.</summary>
    /// <remarks>
    /// Update the pinned hash only together with a model transition or invariant change;
    /// a silent drift between the verified model and production is a conformance failure.
    /// </remarks>
    [Test]
    public async Task ProtocolVersionMatchesModelManifest() => _ = await Assert.That(ExploreRunner.ModelVersionHash).IsEqualTo("f0e518fc4db3ce67");

    /// <summary>Commits the leader-term entry of a won term at the first index, as a promotion does.</summary>
    /// <param name="coordinator">The commit coordinator.</param>
    /// <param name="term">The won term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> once the entry is committed.</returns>
    private static async Task<bool> CommitLeaderTermAsync(ReplicaCommitCoordinator coordinator, ulong term, CancellationToken cancellationToken)
    {
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1UL, term), TimeSpan.FromSeconds(2), cancellationToken);
        return true;
    }
}
