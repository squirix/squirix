using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Every request kind to a copy that is not complete is refused with the code of the copy state, and nothing on disk changes.</summary>
[Immutable]
public sealed class ReplicaFollowerNotCompleteTests : ServerUnitTestBase
{
    private const string GroupId = "n1";
    private const string LeaderId = "n2";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>Appends with entries and without, commit advances, installs, pre-votes and votes are refused with the code of the copy state, even when an offer is held.</summary>
    /// <param name="incomplete">Whether the copy is planted incomplete; otherwise it is unseeded.</param>
    /// <param name="offered">Whether the planted record holds an offer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true, false)]
    [Arguments(true, true)]
    [Arguments(false, false)]
    public async Task EveryRequestIsRefusedByState(bool incomplete, bool offered, CancellationToken cancellationToken)
    {
        var state = incomplete ? GroupMemberState.Incomplete : GroupMemberState.Unseeded;
        var code = incomplete ? FollowerLogRefusal.Incomplete : FollowerLogRefusal.Unseeded;
        using var dir = new TempDirectory("squirix-follower-not-complete");
        using var sourceDir = new TempDirectory("squirix-follower-not-complete-source");
        await using var registry = await OpenRegistryAsync(dir, state, offered, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var upload = await PublishUploadAsync(sourceDir, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        var ballot = new ElectionVoteRequest("n3", 5UL, 0UL, 0UL);

        var append = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId, true), cancellationToken);
        var probe = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId, false), cancellationToken);
        var commit = await follower.AdvanceCommitAsync(GroupId, LeaderId, Fingerprint, 1UL, 0UL, 3UL, cancellationToken);
        var install = await follower.InstallSnapshotUploadAsync(GroupId, LeaderId, Fingerprint, 1UL, upload, 3UL, cancellationToken);
        var preVote = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);

        _ = await Assert.That((append.Success, append.RefusalCode, append.CurrentTerm, append.LastLogIndex)).IsEqualTo((false, code, 0UL, 0UL));
        _ = await Assert.That((probe.Success, probe.RefusalCode, probe.CurrentTerm, probe.LastLogIndex)).IsEqualTo((false, code, 0UL, 0UL));
        _ = await Assert.That((commit.Success, commit.RefusalCode)).IsEqualTo((false, code));
        _ = await Assert.That((install.Success, install.Refusal)).IsEqualTo((false, code));
        _ = await Assert.That((preVote.Granted, preVote.RefusalCode, preVote.CurrentTerm)).IsEqualTo((false, code, 0UL));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, code, 0UL));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetSnapshotPath(dir, GroupId))).IsFalse();
        _ = await Assert.That((await follower.GetStatusAsync(GroupId, cancellationToken))?.MemberState).IsEqualTo(state);
    }

    /// <summary>An offer to an incomplete copy is refused, reports the incomplete state, and changes nothing on disk.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferToIncompleteCopyIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-incomplete-offer");
        await using var registry = await OpenRegistryAsync(dir, GroupMemberState.Incomplete, true, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);

        var offer = await follower.AcceptOfferAsync(GroupId, GroupId, Fingerprint, 1UL, cancellationToken);

        _ = await Assert.That((offer.Accepted, offer.MemberState, offer.RefusalCode)).IsEqualTo((false, GroupMemberState.Incomplete, FollowerLogRefusal.NotReady));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    private static async Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, GroupMemberState state, bool offered, CancellationToken cancellationToken)
    {
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var meta = new GroupLogMetadata(GroupId, Fingerprint, 1UL, 0UL, string.Empty, 0UL, 0UL, state, offered);
        await GroupMetadataFile.WriteAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), in meta, cancellationToken);
        var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, Fingerprint, 1UL, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    /// <summary>Publishes a snapshot on a source node and reads it back as the upload a leader would send.</summary>
    /// <param name="sourceDir">The data directory of the source node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A well-formed upload of the snapshot file.</returns>
    private static async Task<ReplicaSnapshotUpload> PublishUploadAsync(string sourceDir, CancellationToken cancellationToken)
    {
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, [], DateTime.UnixEpoch);
        var path = GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId);
        _ = Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        await new GroupSnapshotStore(sourceDir, GroupId).PublishAsync(snapshot, cancellationToken);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        return new ReplicaSnapshotUpload(bytes, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - 4)), snapshot.LastIncludedIndex, snapshot.LastIncludedTerm);
    }

    private static FollowerBatch Batch(string leaderNodeId, bool withEntry)
    {
        if (!withEntry)
            return new FollowerBatch([], leaderNodeId, 1, 0, 0, 0);

        var record = new ReplicaLogRecord(
            1,
            1,
            "op-1",
            "cache",
            new byte[] { 1 },
            "UserMutation",
            "cache",
            Encoding.UTF8.GetBytes("k"),
            "Set",
            Encoding.UTF8.GetBytes("v"),
            ReadOnlyMemory<byte>.Empty,
            0,
            0,
            0,
            0);
        return new FollowerBatch([record], leaderNodeId, 1, 0, 0, 0);
    }
}
