using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Durable election state: term and vote persistence plus RF=1 timer suppression.</summary>
[Immutable]
public sealed class ElectionStateTests : ServerUnitTestBase
{
    private const string GroupId = "election-state";

    /// <summary>A granted vote is persisted before the grant is reported and survives a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistsTermAndVoteBeforeGrantingVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-state-persist");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);

            var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);
            _ = await Assert.That(granted.Granted).IsTrue();
            _ = await Assert.That(granted.CurrentTerm).IsEqualTo(2UL);

            var status = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That(status.CurrentTerm).IsEqualTo(2UL);
            _ = await Assert.That(status.VotedFor).IsEqualTo("node-b");

            // The metadata file already carries the term and vote while the log is still open:
            // persistence happens before the grant is reported, not after.
            var metaBytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
            _ = await Assert.That(GroupLogCodec.TryDecodeMeta(metaBytes, out var meta)).IsTrue();
            _ = await Assert.That(meta.CurrentTerm).IsEqualTo(2UL);
            _ = await Assert.That(meta.VotedFor).IsEqualTo("node-b");
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var reopenedStatus = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(reopenedStatus.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(reopenedStatus.VotedFor).IsEqualTo("node-b");
    }

    /// <summary>A single-node group creates no election timer.</summary>
    [Test]
    public async Task RfOneDoesNotCreateElectionTimer()
    {
        var time = new FakeTimeProvider();

        using var redundant = ElectionTimer.Create(0, null, time);
        using var single = ElectionTimer.Create(1, null, time);
        _ = await Assert.That(redundant).IsNull();
        _ = await Assert.That(single).IsNull();
        using var quorum = ElectionTimer.Create(3, null, time);
        _ = await Assert.That(quorum).IsNotNull();
    }

    /// <summary>Opens a follower log for the election group without materializing storage yet.</summary>
    /// <param name="dir">The persistence root for the test.</param>
    /// <returns>A follower log for the election group.</returns>
    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId));
}
