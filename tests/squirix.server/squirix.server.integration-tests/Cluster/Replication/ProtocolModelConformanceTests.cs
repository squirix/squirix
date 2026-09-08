using System;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.ProtocolModel;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Checks production durable boundaries against the protocol transition system.</summary>
public sealed class ProtocolModelConformanceTests : NodeIntegrationTestBase
{
    /// <summary>A production commit trace follows a path accepted by the protocol model.</summary>
    [Fact]
    public async Task ProductionCommitTraceMatchesModel()
    {
        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);

        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), DefaultCancellationToken);

        Assert.Equal(
        [
            new ConformanceTestKit.TracePoint(1, 1, 0, 0),
            new ConformanceTestKit.TracePoint(1, 1, 1, 0),
            new ConformanceTestKit.TracePoint(1, 1, 1, 1),
        ],
        pipeline.Trace);
        ConformanceTestKit.AssertModelAccepted(pipeline.Trace);
    }

    /// <summary>Production election vote and commit trace follow the model safety path.</summary>
    [Fact]
    public async Task ProductionElectionTraceMatchesModel()
    {
        using var dir = new TempDirectory("squirix-election-trace");
        await using var log = new FollowerLog(dir, "election-trace", GroupComposition.Create("election-trace"));
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(
            new FollowerLogAppendRequest(
                "leader-1",
                1UL,
                0UL,
                0UL,
                0UL,
                new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a"))])),
            DefaultCancellationToken);

        var granted = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), DefaultCancellationToken);
        Assert.True(granted.Granted);

        var preVote = await log.TryCheckPreVoteAsync(new ElectionVoteRequest("node-c", 3UL, 1UL, 1UL), DefaultCancellationToken);
        Assert.True(preVote.Granted);

        var eligible = FailoverActivationGate.CheckElection(3, true, true, true, granted.CurrentTerm, granted.CurrentTerm);
        Assert.True(eligible.Eligible);

        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), DefaultCancellationToken);
        ConformanceTestKit.AssertModelAccepted(pipeline.Trace);
    }

    /// <summary>Production quorum-read gate and commit trace follow the model read path.</summary>
    [Fact]
    public async Task ProductionQuorumReadTraceMatchesModel()
    {
        var allowed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        Assert.True(allowed.Allowed);

        // The barrier parks on the fake clock below the read index, then serves once applied.
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var applied = new StrongBox<ulong>(4);
        var wait = LeaderReadBarrier.WaitUntilAppliedAsync(() => applied.Value, 9UL, time, TimeSpan.FromMilliseconds(10), DefaultCancellationToken);
        Assert.False(wait.IsCompleted);

        applied.Value = 9UL;
        time.Advance(TimeSpan.FromMilliseconds(10));
        await wait;

        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);
        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), DefaultCancellationToken);
        ConformanceTestKit.AssertModelAccepted(pipeline.Trace);
    }

    /// <summary>Production conformance pins the protocol model version it was verified against.</summary>
    /// <remarks>
    /// Update the pinned hash only together with a model transition or invariant change;
    /// a silent drift between the verified model and production is a conformance failure.
    /// </remarks>
    [Fact]
    public void ProtocolVersionMatchesModelManifest() => Assert.Equal("f0e518fc4db3ce67", ExploreRunner.ModelVersionHash);
}
