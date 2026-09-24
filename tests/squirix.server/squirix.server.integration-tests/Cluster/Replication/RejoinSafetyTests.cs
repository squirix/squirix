using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Rejoin safety: a rejoined former leader regains eligibility only after catching up.</summary>
public sealed class RejoinSafetyTests : NodeIntegrationTestBase
{
    /// <summary>Caught-up rejoin regains eligibility while stale terms still step it down.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaughtUpRejoinRegainsEligibility(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-rejoin-safety");
        await using var log = new FollowerLog(dir, "rejoin-safety", GroupComposition.Create("rejoin-safety"));
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(
            new FollowerLogAppendRequest("leader-1", 1UL, 0UL, 0UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a")))),
            cancellationToken);

        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);
        _ = await Assert.That(granted.Granted).IsTrue();

        // The catch-up flag is derived from the replicated log state, not passed literally.
        const ulong leaderCommitIndex = 1UL;
        var status = await log.GetStatusAsync(cancellationToken);
        var caughtUp = status.LastLogIndex >= leaderCommitIndex;
        _ = await Assert.That(caughtUp).IsTrue();

        var eligible = FailoverActivationGate.CheckElection(3, true, true, caughtUp, granted.CurrentTerm, granted.CurrentTerm);
        _ = await Assert.That(eligible.Eligible).IsTrue();

        var deposed = FailoverActivationGate.CheckElection(3, true, true, true, granted.CurrentTerm, granted.CurrentTerm + 1);
        _ = await Assert.That(deposed.Eligible).IsFalse();
        _ = await Assert.That(deposed.Denial).IsEqualTo(FailoverDenial.StaleTerm);
    }

    /// <summary>Lagging rejoin stays fenced until the log reaches the leader commit index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LaggingRejoinStaysIneligible(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-rejoin-lagging");
        await using var log = new FollowerLog(dir, "rejoin-lagging", GroupComposition.Create("rejoin-lagging"));
        await log.OpenAsync(cancellationToken);

        const ulong leaderCommitIndex = 2UL;
        var status = await log.GetStatusAsync(cancellationToken);
        var caughtUp = status.LastLogIndex >= leaderCommitIndex;
        _ = await Assert.That(caughtUp).IsFalse();

        var lagging = FailoverActivationGate.CheckElection(3, true, true, caughtUp, 4, 4);
        _ = await Assert.That(lagging.Eligible).IsFalse();
        _ = await Assert.That(lagging.Denial).IsEqualTo(FailoverDenial.LogNotCaughtUp);
    }
}
