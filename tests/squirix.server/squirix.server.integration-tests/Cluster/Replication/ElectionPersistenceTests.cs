using System;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Election term and vote durability across follower-log restarts.</summary>
public sealed class ElectionPersistenceTests : NodeIntegrationTestBase
{
    private const string GroupId = "election-persistence";

    /// <summary>A granted vote survives a restart and still blocks a rival in the same term.</summary>
    [Fact]
    public async Task GrantedVoteSurvivesRestart()
    {
        using var dir = new TempDirectory("squirix-election-persist-vote");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            var granted = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), DefaultCancellationToken);
            Assert.True(granted.Granted);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, status.CurrentTerm);
        Assert.Equal("node-b", status.VotedFor);

        var rival = await reopened.TryRequestVoteAsync(new ElectionVoteRequest("node-c", 2UL, 1UL, 1UL), DefaultCancellationToken);
        Assert.False(rival.Granted);
        Assert.Equal(FollowerLogRefusal.AlreadyVoted, rival.RefusalCode);

        var replay = await reopened.TryRequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), DefaultCancellationToken);
        Assert.True(replay.Granted);
    }

    /// <summary>A higher-term vote clears the previous grant and the stepped term survives a restart.</summary>
    [Fact]
    public async Task HigherTermVoteClearsPreviousGrant()
    {
        using var dir = new TempDirectory("squirix-election-persist-revote");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            var first = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-a", 1UL, 0UL, 0UL), DefaultCancellationToken);
            Assert.True(first.Granted);
            var stepped = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), DefaultCancellationToken);
            Assert.True(stepped.Granted);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, status.CurrentTerm);
        Assert.Equal("node-b", status.VotedFor);
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
        new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))]));

    /// <summary>Opens a follower log for the election group without materializing storage yet.</summary>
    /// <param name="dir">The persistence root for the test.</param>
    /// <returns>A follower log for the election group.</returns>
    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId));
}
