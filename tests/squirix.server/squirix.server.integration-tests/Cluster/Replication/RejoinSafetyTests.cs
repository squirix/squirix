using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Rejoin safety: a restarted replica stands for election only once its local log is ready.</summary>
public sealed class RejoinSafetyTests : NodeIntegrationTestBase
{
    /// <summary>A restarted replica with a ready log regains eligibility while stale terms still step it down.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyLogRegainsEligibility(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-rejoin-safety");
        await using (var first = new FollowerLog(dir, "rejoin-safety", GroupComposition.Create("rejoin-safety"), NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await first.OpenSeededAsync(cancellationToken);
            _ = await first.AppendAsync(
                new FollowerLogAppendRequest("leader-1", 1UL, 0UL, 0UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a")))),
                cancellationToken);
        }

        await using var log = new FollowerLog(dir, "rejoin-safety", GroupComposition.Create("rejoin-safety"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenAsync(cancellationToken);
        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);
        _ = await Assert.That(granted.Granted).IsTrue();

        // The gate input is derived from the log status the way the election step derives it.
        var status = await log.GetStatusAsync(cancellationToken);
        var logReady = status.Readiness == FollowerLogReadiness.Ready;
        _ = await Assert.That(logReady).IsTrue();

        var eligible = FailoverActivationGate.CheckElection(3, true, true, true, logReady, granted.CurrentTerm, granted.CurrentTerm);
        _ = await Assert.That(eligible.Eligible).IsTrue();

        var deposed = FailoverActivationGate.CheckElection(3, true, true, true, logReady, granted.CurrentTerm, granted.CurrentTerm + 1);
        _ = await Assert.That(deposed.Eligible).IsFalse();
        _ = await Assert.That(deposed.Denial).IsEqualTo(FailoverDenial.StaleTerm);
    }

    /// <summary>A restarted replica whose log has not finished startup validation stays out of elections.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreadyLogStaysIneligible(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-rejoin-unready");
        await using var log = new FollowerLog(dir, "rejoin-unready", GroupComposition.Create("rejoin-unready"), NullLogger<FollowerLog>.Instance, TimeProvider.System);

        var status = await log.GetStatusAsync(cancellationToken);
        var logReady = status.Readiness == FollowerLogReadiness.Ready;
        _ = await Assert.That(logReady).IsFalse();

        var unready = FailoverActivationGate.CheckElection(3, true, true, true, logReady, 4, 4);
        _ = await Assert.That(unready.Eligible).IsFalse();
        _ = await Assert.That(unready.Denial).IsEqualTo(FailoverDenial.LogNotCaughtUp);
    }
}
