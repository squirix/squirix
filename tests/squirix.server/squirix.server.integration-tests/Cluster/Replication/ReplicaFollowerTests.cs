using System;
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

/// <summary>Follower replication over real group logs: append, status, and snapshot install.</summary>
public sealed class ReplicaFollowerTests : NodeIntegrationTestBase
{
    private const string GroupId = "follower-service";
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>The replica set of the group; every candidate of these tests is a member.</summary>
    private static readonly ReplicaMembership Members = new(new ReplicaGroupLocator(new PhysicalNodeRing([GroupId, "node-b", "node-c"]), 3), [GroupId]);

    /// <summary>Accepted appends advance the durable log and are visible in status.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendAcceptedAdvancesLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-append");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);

        var result = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("node-b", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL), Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(result.CurrentTerm).IsEqualTo(1UL);
        _ = await Assert.That(result.LastLogIndex).IsEqualTo(2UL);
        var live = await service.GetStatusAsync(GroupId, cancellationToken);
        var current = await Assert.That(live).IsNotNull();
        _ = await Assert.That(current.LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>An append from a deposed leader term is refused without touching the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendStaleTermRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-stale");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);
        _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("node-b", 2UL, 0UL, 0UL, 0UL, Record(1UL, 2UL)), cancellationToken);

        var result = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("node-b", 1UL, 1UL, 2UL, 0UL, Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That(result.CurrentTerm).IsEqualTo(2UL);
        var stale = await service.GetStatusAsync(GroupId, cancellationToken);
        var staleStatus = await Assert.That(stale).IsNotNull();
        _ = await Assert.That(staleStatus.LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>Appending to an unserved group is refused without touching storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendToUnknownGroupRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-unknown");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);

        var result = await service.AppendAsync("missing", Fingerprint, 1UL, Batch("node-b", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL)), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Conflicting fingerprints and older generations are refused as topology mismatches.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendTopologyMismatchRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-topology");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);

        var fingerprint = await service.AppendAsync(GroupId, new byte[] { 4 }, 1UL, Batch("node-b", 1UL, 1UL, 1UL, 1UL, Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(fingerprint.Success).IsFalse();
        _ = await Assert.That(fingerprint.RefusalCode).IsEqualTo(FollowerLogRefusal.TopologyMismatch);

        var generation = await service.AppendAsync(GroupId, Fingerprint, 0UL, Batch("node-b", 1UL, 1UL, 1UL, 1UL, Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(generation.Success).IsFalse();
        _ = await Assert.That(generation.RefusalCode).IsEqualTo(FollowerLogRefusal.TopologyMismatch);
    }

    /// <summary>Codec round-trips every record field and rejects truncation and trailing bytes.</summary>
    [Test]
    public async Task LogCodecRoundTripsRecord()
    {
        var record = Record(7UL, 3UL);
        var bytes = ReplicaLogCodec.Encode(in record);

        var decoded = await Assert.That(ReplicaLogCodec.Decode(bytes)).IsNotNull();
        await SequenceAssert.EqualAsync(bytes, ReplicaLogCodec.Encode(in decoded));
        _ = await Assert.That(ReplicaLogCodec.Decode(bytes.AsMemory(0, bytes.Length - 1))).IsNull();

        var trailed = new byte[bytes.Length + 1];
        bytes.CopyTo(trailed, 0);
        _ = await Assert.That(ReplicaLogCodec.Decode(trailed)).IsNull();
    }

    /// <summary>A higher-term vote from a candidate whose log trails is refused, yet reports the stepped durable term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleLogVoteStepsTermWithoutGrant(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-vote-stale-log");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);
        _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("node-b", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL), Record(2UL, 1UL)), cancellationToken);

        var vote = await service.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("node-b", 3UL, 1UL, 1UL), cancellationToken);

        _ = await Assert.That(vote.Granted).IsFalse();
        _ = await Assert.That(vote.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleLog);
        _ = await Assert.That(vote.CurrentTerm).IsEqualTo(3UL);
        var status = await Assert.That(await service.GetStatusAsync(GroupId, cancellationToken)).IsNotNull();
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(3UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
    }

    /// <summary>Status of an unserved group is null.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StatusUnknownGroupIsNull(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-status-unknown");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);

        _ = await Assert.That(await service.GetStatusAsync("missing", cancellationToken)).IsNull();
    }

    /// <summary>A pre-vote probe answers from the log and leaves the durable term and metadata unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteLeavesTermUnchanged(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-prevote");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);
        _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("node-b", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL)), cancellationToken);
        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);

        var probe = await service.PreVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);

        _ = await Assert.That(probe.Granted).IsTrue();
        _ = await Assert.That(probe.CurrentTerm).IsEqualTo(1UL);
        var status = await Assert.That(await service.GetStatusAsync(GroupId, cancellationToken)).IsNotNull();
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(1UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
        var after = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>A granted vote is durable: the stepped term and the candidate survive a registry restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteGrantPersistsTermAndCandidate(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-vote");
        await using (var registry = await OpenAsync(dir, GroupId, cancellationToken))
        {
            var service = new ReplicaFollower(registry, Members);
            _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("node-b", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL)), cancellationToken);

            var vote = await service.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);

            _ = await Assert.That(vote.Granted).IsTrue();
            _ = await Assert.That(vote.CurrentTerm).IsEqualTo(2UL);
        }

        await using var reopened = await OpenAsync(dir, GroupId, cancellationToken);
        var status = await Assert.That(await new ReplicaFollower(reopened, Members).GetStatusAsync(GroupId, cancellationToken)).IsNotNull();
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo("node-b");
    }

    /// <summary>One candidate wins a term: a rival in the same term is refused, and a retry by the winner is granted without a rewrite.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteGrantIsExclusivePerTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-vote-exclusive");
        await using (var registry = await OpenAsync(dir, GroupId, cancellationToken))
        {
            var first = new ReplicaFollower(registry, Members);
            _ = await first.AppendAsync(GroupId, Fingerprint, 1UL, Batch("node-b", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL)), cancellationToken);
            var grant = await first.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);
            _ = await Assert.That(grant.Granted).IsTrue();
        }

        // The grant reply does not wait for the mirror copy of the vote record; the dispose does, so the file is settled after a reopen.
        await using var reopened = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(reopened, Members);
        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);

        var rival = await service.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("node-c", 2UL, 1UL, 1UL), cancellationToken);
        var retry = await service.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("node-b", 2UL, 1UL, 1UL), cancellationToken);

        _ = await Assert.That(rival.Granted).IsFalse();
        _ = await Assert.That(rival.RefusalCode).IsEqualTo(FollowerLogRefusal.AlreadyVoted);
        _ = await Assert.That(rival.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(retry.Granted).IsTrue();
        _ = await Assert.That(retry.CurrentTerm).IsEqualTo(2UL);
        var after = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>A vote from a candidate with another topology is refused before the log is reached and changes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteTopologyMismatchIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-vote-topology");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry, Members);
        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);

        var vote = await service.RequestVoteAsync(GroupId, new byte[] { 4 }, 1UL, new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);
        var probe = await service.PreVoteAsync(GroupId, Fingerprint, 0UL, new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That(vote.Granted).IsFalse();
        _ = await Assert.That(vote.RefusalCode).IsEqualTo(FollowerLogRefusal.TopologyMismatch);
        _ = await Assert.That(vote.CurrentTerm).IsEqualTo(0UL);
        _ = await Assert.That(probe.Granted).IsFalse();
        _ = await Assert.That(probe.RefusalCode).IsEqualTo(FollowerLogRefusal.TopologyMismatch);
        var after = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    private static FollowerBatch Batch(string leader, ulong term, ulong prevIndex, ulong prevTerm, ulong commit, params ReplicaLogRecord[] records) =>
        new(Array.ConvertAll(records, static record => FollowerLogEntry.Of(in record)), leader, term, prevIndex, prevTerm, commit);

    private static async Task<ReplicaGroupRegistry> OpenAsync(TempDirectory dir, string groupId, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, [groupId], cancellationToken);
        var registry = new ReplicaGroupRegistry(dir, [groupId], 1, Fingerprint, 1UL, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        await registry.OpenAsync(cancellationToken);
        return registry;
    }

    private static ReplicaLogRecord Record(ulong index, ulong term) => new(
        index,
        term,
        $"op-{index}",
        "scope",
        new[] { Convert.ToByte(index) },
        "UserMutation",
        "cache",
        new[] { Convert.ToByte(index + 1) },
        "set",
        new[] { Convert.ToByte(index + 2) },
        new[] { Convert.ToByte(index + 3) },
        0,
        0,
        0,
        0);
}
