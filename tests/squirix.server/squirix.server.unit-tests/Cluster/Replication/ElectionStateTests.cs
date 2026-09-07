using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Durable election state: term and vote persistence plus RF=1 timer suppression.</summary>
[Immutable]
public sealed class ElectionStateTests : ServerUnitTestBase
{
    private const string GroupId = "election-state";

    /// <summary>A granted vote is persisted before the grant is reported and survives a restart.</summary>
    [Fact]
    public async Task PersistsTermAndVoteBeforeGrantingVote()
    {
        using var dir = new TempDirectory("squirix-election-state-persist");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);

            var granted = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), DefaultCancellationToken);
            Assert.True(granted.Granted);
            Assert.Equal(2UL, granted.CurrentTerm);

            var status = await log.GetStatusAsync(DefaultCancellationToken);
            Assert.Equal(2UL, status.CurrentTerm);
            Assert.Equal("node-b", status.VotedFor);

            // The metadata file already carries the term and vote while the log is still open:
            // persistence happens before the grant is reported, not after.
            var metaBytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), DefaultCancellationToken);
            Assert.True(GroupLogCodec.TryDecodeMeta(metaBytes, out var meta));
            Assert.Equal(2UL, meta.CurrentTerm);
            Assert.Equal("node-b", meta.VotedFor);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        var reopenedStatus = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, reopenedStatus.CurrentTerm);
        Assert.Equal("node-b", reopenedStatus.VotedFor);
    }

    /// <summary>A single-node group creates no election timer.</summary>
    [Fact]
    public void RfOneDoesNotCreateElectionTimer()
    {
        var time = new FakeTimeProvider();

        using var redundant = ElectionTimer.Create(0, null, time);
        using var single = ElectionTimer.Create(1, null, time);
        Assert.Null(redundant);
        Assert.Null(single);
        using var quorum = ElectionTimer.Create(3, null, time);
        Assert.NotNull(quorum);
    }

    /// <summary>Opens a follower log for the election group without materializing storage yet.</summary>
    /// <param name="dir">The persistence root for the test.</param>
    /// <returns>A follower log for the election group.</returns>
    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId));
}
