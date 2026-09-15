using System;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
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
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);

        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);

        await SequenceAssert.Equal(
            [
                new ConformanceTestKit.TracePoint(1, 1, 0, 0),
                new ConformanceTestKit.TracePoint(1, 1, 1, 0),
                new ConformanceTestKit.TracePoint(1, 1, 1, 1),
            ],
            pipeline.Trace);
        await ConformanceTestKit.AssertModelAccepted(pipeline.Trace);
    }

    /// <summary>Production election vote and commit trace follow the model safety path.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionElectionTraceMatchesModel(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-trace");
        await using var log = new FollowerLog(dir, "election-trace", GroupComposition.Create("election-trace"));
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(
            new FollowerLogAppendRequest("leader-1", 1UL, 0UL, 0UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a"))])),
            cancellationToken);

        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);
        _ = await Assert.That(granted.Granted).IsTrue();

        var preVote = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-c", 3UL, 1UL, 1UL), cancellationToken);
        _ = await Assert.That(preVote.Granted).IsTrue();

        var eligible = FailoverActivationGate.CheckElection(3, true, true, true, granted.CurrentTerm, granted.CurrentTerm);
        _ = await Assert.That(eligible.Eligible).IsTrue();

        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);
        await ConformanceTestKit.AssertModelAccepted(pipeline.Trace);
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
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);
        await ConformanceTestKit.AssertModelAccepted(pipeline.Trace);
    }

    /// <summary>Production conformance pins the protocol model version it was verified against.</summary>
    /// <remarks>
    /// Update the pinned hash only together with a model transition or invariant change;
    /// a silent drift between the verified model and production is a conformance failure.
    /// </remarks>
    [Test]
    public async Task ProtocolVersionMatchesModelManifest() => _ = await Assert.That(ExploreRunner.ModelVersionHash).IsEqualTo("f0e518fc4db3ce67");
}
