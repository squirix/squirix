using System;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Rejoin safety: a rejoined former leader regains eligibility only after catching up.</summary>
public sealed class RejoinSafetyTests : NodeIntegrationTestBase
{
    /// <summary>Lagging rejoin stays fenced until the log reaches the leader commit index.</summary>
    [Fact]
    public async Task LaggingRejoinStaysIneligible()
    {
        using var dir = new TempDirectory("squirix-rejoin-lagging");
        await using var log = new FollowerLog(dir, "rejoin-lagging", GroupComposition.Create("rejoin-lagging"));
        await log.OpenAsync(DefaultCancellationToken);

        const ulong leaderCommitIndex = 2UL;
        var status = await log.GetStatusAsync(DefaultCancellationToken);
        var caughtUp = status.LastLogIndex >= leaderCommitIndex;
        Assert.False(caughtUp);

        var lagging = FailoverActivationGate.CheckElection(3, true, true, caughtUp, 4, 4);
        Assert.False(lagging.Eligible);
        Assert.Equal(FailoverDenial.LogNotCaughtUp, lagging.Denial);
    }

    /// <summary>Caught-up rejoin regains eligibility while stale terms still step it down.</summary>
    [Fact]
    public async Task CaughtUpRejoinRegainsEligibility()
    {
        using var dir = new TempDirectory("squirix-rejoin-safety");
        await using var log = new FollowerLog(dir, "rejoin-safety", GroupComposition.Create("rejoin-safety"));
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

        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), DefaultCancellationToken);
        Assert.True(granted.Granted);

        // The catch-up flag is derived from the replicated log state, not passed literally.
        const ulong leaderCommitIndex = 1UL;
        var status = await log.GetStatusAsync(DefaultCancellationToken);
        var caughtUp = status.LastLogIndex >= leaderCommitIndex;
        Assert.True(caughtUp);

        var eligible = FailoverActivationGate.CheckElection(3, true, true, caughtUp, granted.CurrentTerm, granted.CurrentTerm);
        Assert.True(eligible.Eligible);

        var deposed = FailoverActivationGate.CheckElection(3, true, true, true, granted.CurrentTerm, granted.CurrentTerm + 1);
        Assert.False(deposed.Eligible);
        Assert.Equal(FailoverDenial.StaleTerm, deposed.Denial);
    }
}
