using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Election term and vote durability across follower-log restarts.</summary>
public sealed class ElectionPersistenceTests : NodeIntegrationTestBase
{
    private const string GroupId = "election-persistence";

    /// <summary>A granted vote survives a restart and still blocks a rival in the same term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrantedVoteSurvivesRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-persist-vote");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);
            _ = await Assert.That(granted.Granted).IsTrue();
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo("node-b");

        var rival = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-c", 2UL, 1UL, 1UL), cancellationToken);
        _ = await Assert.That(rival.Granted).IsFalse();
        _ = await Assert.That(rival.RefusalCode).IsEqualTo(FollowerLogRefusal.AlreadyVoted);

        var replay = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);
        _ = await Assert.That(replay.Granted).IsTrue();
    }

    /// <summary>A higher-term vote clears the previous grant and the stepped term survives a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermVoteClearsPreviousGrant(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-persist-revote");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            var first = await log.RequestVoteAsync(new ElectionVoteRequest("node-a", 1UL, 0UL, 0UL), cancellationToken);
            _ = await Assert.That(first.Granted).IsTrue();
            var stepped = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);
            _ = await Assert.That(stepped.Granted).IsTrue();
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo("node-b");
    }

    /// <summary>Builds an append request for a single entry.</summary>
    /// <param name="index">The entry log index.</param>
    /// <param name="term">The entry term.</param>
    /// <param name="payload">The entry payload.</param>
    /// <returns>The append request.</returns>
    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader-1",
        term,
        index - 1,
        index == 1UL ? 0UL : term,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))));

    /// <summary>Opens a follower log for the election group without materializing storage yet.</summary>
    /// <param name="dir">The persistence root for the test.</param>
    /// <returns>A follower log for the election group.</returns>
    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId));
}
