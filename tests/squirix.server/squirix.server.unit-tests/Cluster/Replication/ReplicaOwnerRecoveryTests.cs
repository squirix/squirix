using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ElectionTestDoubles;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Startup after recovering a provisional owner log.</summary>
public sealed class ReplicaOwnerRecoveryTests : ServerUnitTestBase
{
    /// <summary>A recovered owner cannot reuse its provisional term after committing any history, even when peers refuse a new election.</summary>
    /// <param name="grant">Whether peers grant a new election.</param>
    /// <param name="compacted">Whether the entire committed prefix was compacted before restart.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task CommittedOwnerRequiresFreshElection(bool grant, bool compacted, CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(call => grant ? Grant(call) : new FollowerLogVoteResult(false, RefusalCodes.StaleLog, 1UL));
        await using var scope = await OpenAsync("n1", 3, votes);
        await using var reopened = await ReopenOwnerAsync(scope, true, cancellationToken, compacted);
        var election = CreateElection(scope with { Log = reopened }, ["n1", "n2", "n3"]);

        var start = await election.StepAsync(cancellationToken);

        _ = await Assert.That(start).IsEqualTo(new ElectionOutcome(ElectionEvent.None, 1UL));
        _ = await Assert.That(scope.State.HasAuthority).IsFalse();
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo(string.Empty);
        _ = await Assert.That(votes.Calls.Length).IsEqualTo(0);
        scope.Time.Advance(Options.ElectionTimeout);
        var elected = await election.StepAsync(cancellationToken);
        _ = await Assert.That(elected.Event).IsEqualTo(grant ? ElectionEvent.Authorized : ElectionEvent.PreVoteLost);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(grant ? 2UL : 1UL);
        _ = await Assert.That(scope.State.HasAuthority).IsEqualTo(grant);
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo(grant ? "promote:2" : string.Empty);
    }

    /// <summary>A locally durable but uncommitted bootstrap entry still permits provisional leadership after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UncommittedOwnerResumesBootstrap(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(static _ => null);
        await using var scope = await OpenAsync("n1", 3, votes);
        await using var reopened = await ReopenOwnerAsync(scope, false, cancellationToken);
        var election = CreateElection(scope with { Log = reopened }, ["n1", "n2", "n3"]);

        var start = await election.StepAsync(cancellationToken);

        _ = await Assert.That(start).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 1UL));
        _ = await Assert.That(votes.Calls.Length).IsEqualTo(0);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    private static async Task<FollowerLog> ReopenOwnerAsync(ElectionScope scope, bool committed, CancellationToken token, bool compacted = false)
    {
        var entry = new FollowerLogEntry(1UL, 1UL, new byte[] { 1 });
        var append = await scope.Log.AppendAsync(new FollowerLogAppendRequest("n1", 1UL, 0UL, 0UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(entry)), token);
        _ = await Assert.That(append.Success).IsTrue();
        if (committed)
            _ = await Assert.That((await scope.Log.AdvanceCommitAsync(1UL, token)).Success).IsTrue();

        if (compacted)
        {
            _ = await Assert.That((await scope.Log.AdvanceAppliedAsync(1UL, token)).Success).IsTrue();
            _ = await Assert.That(await scope.Log.CompactThroughAsync(1UL, token)).IsEqualTo(GroupCompactionOutcome.Compacted);
        }

        await scope.Log.DisposeAsync();
        var reopened = new FollowerLog(scope.Dir, "n1", GroupComposition.Create("n1"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(token);
        _ = await Assert.That((await reopened.GetStatusAsync(token)).CommitIndex).IsEqualTo(committed ? 1UL : 0UL);
        if (!compacted)
            return reopened;
        IFollowerLog log = reopened;
        var retention = await log.GetRetentionAsync(token);
        _ = await Assert.That((retention.RetainedEntries, retention.SnapshotIndex)).IsEqualTo((0, 1UL));
        return reopened;
    }

    private static FollowerLogVoteResult? Grant(VoteCall call) => new(true, string.Empty, call.PreVote ? 0UL : call.Term);
}
