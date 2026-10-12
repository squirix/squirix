using System;
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

/// <summary>A copy that is not complete refuses appends with the code of its state; ballots are refused by an unseeded copy and answered by an incomplete one.</summary>
[Immutable]
public sealed class ReplicaFollowerNotCompleteTests : ServerUnitTestBase
{
    private const string GroupId = "n1";
    private const string LeaderId = "n2";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>Appends with entries and without are refused with the code of the copy state, even when an offer is held, and nothing on disk changes.</summary>
    /// <param name="incomplete">Whether the copy is planted incomplete; otherwise it is unseeded.</param>
    /// <param name="offered">Whether the planted record holds an offer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true, false)]
    [Arguments(true, true)]
    [Arguments(false, false)]
    public async Task AppendsAreRefusedByState(bool incomplete, bool offered, CancellationToken cancellationToken)
    {
        var state = incomplete ? GroupMemberState.Incomplete : GroupMemberState.Unseeded;
        var code = incomplete ? FollowerLogRefusal.Incomplete : FollowerLogRefusal.Unseeded;
        using var dir = new TempDirectory("squirix-follower-not-complete");
        await using var registry = await OpenRegistryAsync(dir, state, offered, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);

        var append = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId, true), cancellationToken);
        var probe = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId, false), cancellationToken);

        _ = await Assert.That((append.Success, append.RefusalCode, append.CurrentTerm, append.LastLogIndex)).IsEqualTo((false, code, 0UL, 0UL));
        _ = await Assert.That((probe.Success, probe.RefusalCode, probe.CurrentTerm, probe.LastLogIndex)).IsEqualTo((false, code, 0UL, 0UL));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That((await follower.GetStatusAsync(GroupId, cancellationToken))?.MemberState).IsEqualTo(state);
    }

    /// <summary>An unseeded copy refuses a pre-vote and a vote with its code and term zero, and nothing on disk changes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededCopyRefusesBallots(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-unseeded-ballots");
        await using var registry = await OpenRegistryAsync(dir, GroupMemberState.Unseeded, false, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        var ballot = new ElectionVoteRequest("n3", 5UL, 0UL, 0UL);

        var preVote = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode, preVote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>An incomplete copy keeps its term, its vote and its log, so it grants a pre-vote and a vote and records the vote while it stays incomplete.</summary>
    /// <param name="offered">Whether the planted record holds an offer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IncompleteCopyAnswersBallots(bool offered, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-incomplete-ballots");
        await using var registry = await OpenRegistryAsync(dir, GroupMemberState.Incomplete, offered, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var ballot = new ElectionVoteRequest("n3", 5UL, 0UL, 0UL);

        var preVote = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode)).IsEqualTo((true, string.Empty));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((true, string.Empty, 5UL));
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.MemberState, status?.CurrentTerm, status?.VotedFor)).IsEqualTo((GroupMemberState.Incomplete, 5UL, "n3"));
        var durable = await GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        _ = await Assert.That((durable.MemberState, durable.CurrentTerm, durable.VotedFor)).IsEqualTo((GroupMemberState.Incomplete, 5UL, "n3"));
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
