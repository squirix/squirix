using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
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

/// <summary>What the follower RPC paths do with a copy that is not complete: refuse it, post nothing, and complete it only on the call of a member after an offer.</summary>
[Immutable]
public sealed class ReplicaFollowerBootstrapTests : ServerUnitTestBase
{
    private const string GroupId = "n1";
    private const string LeaderId = "n2";
    private const string OutsiderId = "n9";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>An append of a member leader to a copy that holds an offer completes it and is accepted in the same call.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendCompletesOfferedCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-offered");
        await using var registry = await OpenRegistryAsync(dir, true, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId), cancellationToken);

        _ = await Assert.That((result.Success, result.CurrentTerm, result.LastLogIndex)).IsEqualTo((true, 1UL, 1UL));
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That(status?.MemberState).IsEqualTo(GroupMemberState.Complete);
        _ = await Assert.That(registry.StateFor(GroupId).ReadRoute().Known).IsEqualTo(new LeaderRoute(LeaderId, 1UL));
    }

    /// <summary>An append to an unseeded copy without an offer is refused with term zero, changes nothing and records no leader contact.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendWithoutOfferIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-no-offer");
        await using var registry = await OpenRegistryAsync(dir, false, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId), cancellationToken);

        _ = await Assert.That((result.Success, result.RefusalCode, result.CurrentTerm, result.LastLogIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL, 0UL));
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.MemberState, status?.CurrentTerm, status?.LastLogIndex)).IsEqualTo((GroupMemberState.Unseeded, 0UL, 0UL));
        _ = await Assert.That(registry.StateFor(GroupId).ReadRoute().HasLeader).IsFalse();
    }

    /// <summary>A pre-vote and a vote request to an unseeded copy are refused with term zero and post nothing to the election state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VotesToUnseededCopyPostNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-votes");
        await using var registry = await OpenRegistryAsync(dir, false, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var state = registry.StateFor(GroupId);
        var reset = state.ElectionResetTimestamp();
        var ballot = new ElectionVoteRequest("n3", 5UL, 0UL, 0UL);

        var preVote = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode, preVote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        _ = await Assert.That(state.HighestObservedTerm).IsEqualTo(0UL);
        _ = await Assert.That(state.ElectionResetTimestamp()).IsEqualTo(reset);
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.CurrentTerm, status?.VotedFor)).IsEqualTo((0UL, string.Empty));
    }

    /// <summary>A pre-vote from a member completes an offered copy, and the pre-vote is answered by the completed log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteCompletesOfferedCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-prevote");
        await using var registry = await OpenRegistryAsync(dir, true, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        var result = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((result.Granted, result.RefusalCode)).IsEqualTo((true, string.Empty));
        _ = await Assert.That((await follower.GetStatusAsync(GroupId, cancellationToken))?.MemberState).IsEqualTo(GroupMemberState.Complete);
    }

    /// <summary>A vote request from a member completes an offered copy, and the completed log grants and records the vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteCompletesOfferedCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-vote");
        await using var registry = await OpenRegistryAsync(dir, true, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        var result = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((result.Granted, result.RefusalCode, result.CurrentTerm)).IsEqualTo((true, string.Empty, 5UL));
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.MemberState, status?.CurrentTerm, status?.VotedFor)).IsEqualTo((GroupMemberState.Complete, 5UL, "n3"));
    }

    /// <summary>A vote and a pre-vote from a node outside the group are refused as not a member and do not complete an offered copy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonMemberBallotsDoNotComplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-ballot-outsider");
        await using var registry = await OpenRegistryAsync(dir, true, cancellationToken);
        var follower = new ReplicaFollower(registry, new ReplicaMembership(new ReplicaGroupLocator(new PhysicalNodeRing(["n1", "n2", "n3"]), 3), registry.GroupIds));
        var ballot = new ElectionVoteRequest(OutsiderId, 5UL, 0UL, 0UL);

        var preVote = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode, preVote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.NotMember, 0UL));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.NotMember, 0UL));
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.MemberState, status?.CurrentTerm, status?.VotedFor)).IsEqualTo((GroupMemberState.Unseeded, 0UL, string.Empty));
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken)).BootstrapOffered).IsTrue();
    }

    /// <summary>An append, a pre-vote and a vote under another topology are refused with the code, term and last index the topology check gives a complete copy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TopologyMismatchKeepsTermAndLastIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-mismatch");
        await SeededLogs.SeedGroupsAsync(dir, [GroupId], cancellationToken);
        await using var registry = await OpenRegistryAsync(dir, false, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var other = new byte[] { 4 };
        var ballot = new ElectionVoteRequest("n3", 5UL, 0UL, 0UL);
        _ = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId), cancellationToken);

        var append = await follower.AppendAsync(GroupId, other, 1UL, Batch(LeaderId), cancellationToken);
        var preVote = await follower.PreVoteAsync(GroupId, other, 1UL, ballot, cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, other, 1UL, ballot, cancellationToken);

        _ = await Assert.That((append.Success, append.RefusalCode, append.CurrentTerm, append.LastLogIndex)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch, 1UL, 1UL));
        _ = await Assert.That((preVote.Granted, preVote.RefusalCode, preVote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch, 0UL));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch, 0UL));
    }

    /// <summary>An append, a pre-vote and a vote to a log that is not ready are refused by the log, with its durable term and last index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotReadyLogKeepsTermAndLastIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-not-ready");
        await SeededLogs.SeedGroupsAsync(dir, [GroupId], cancellationToken);
        var armed = false;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFrameWritten();
        _ = faults.Setups.OnFramesScheduling();
        _ = faults.Setups.OnFlushed();
        _ = faults.Setups.OnCommitAdvanced();
        _ = faults.Setups.OnBeforeMemoryApply();
        _ = faults.Setups.OnMetaPublishing().Callback(() =>
        {
            if (Volatile.Read(ref armed))
                throw new IOException("simulated failure at a metadata publish.");
        });

        await using var registry = await OpenRegistryAsync(dir, faults.Instance(), cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        _ = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId), cancellationToken);
        Volatile.Write(ref armed, true);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), cancellationToken));

        var append = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(LeaderId), cancellationToken);
        var preVote = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 6UL, 0UL, 0UL), cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 6UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((append.Success, append.RefusalCode, append.CurrentTerm, append.LastLogIndex)).IsEqualTo((false, FollowerLogRefusal.NotReady, 1UL, 1UL));
        _ = await Assert.That((preVote.Granted, preVote.RefusalCode, preVote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.NotReady, 1UL));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.NotReady, 1UL));
    }

    /// <summary>A call whose leader is not a member of the group is refused as not a member and does not complete an offered copy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonMemberLeaderDoesNotComplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bootstrap-outsider");
        await using var registry = await OpenRegistryAsync(dir, true, cancellationToken);
        var follower = new ReplicaFollower(registry, new ReplicaMembership(new ReplicaGroupLocator(new PhysicalNodeRing(["n1", "n2", "n3"]), 3), registry.GroupIds));

        var append = await follower.AppendAsync(GroupId, Fingerprint, 1UL, Batch(OutsiderId), cancellationToken);

        _ = await Assert.That((append.Success, append.RefusalCode, append.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.NotMember, 0UL));
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.MemberState, status?.CurrentTerm)).IsEqualTo((GroupMemberState.Unseeded, 0UL));
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken)).BootstrapOffered).IsTrue();
    }

    /// <summary>A batch is stored as the leader sent it: the bytes read back from the log equal the bytes of the batch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendStoresLeaderBytesAsReceived(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-bytes");
        await using var registry = await OpenRegistryAsync(dir, true, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        FollowerLogEntry[] sent = [Entry(1), Entry(2)];

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch(sent, LeaderId, 1, 0, 0, 0), cancellationToken);

        _ = await Assert.That((result.Success, result.LastLogIndex)).IsEqualTo((true, 2UL));
        _ = registry.TryGetLog(GroupId, out var log);
        var stored = await log!.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(stored.Count).IsEqualTo(2);
        for (var i = 0; i < sent.Length; i++)
        {
            _ = await Assert.That((stored[i].LogIndex, stored[i].Term)).IsEqualTo((sent[i].LogIndex, sent[i].Term));
            _ = await Assert.That(stored[i].Payload.Span.SequenceEqual(sent[i].Payload.Span)).IsTrue();
        }
    }

    /// <summary>A batch with one entry that is not a canonical record at its position is refused whole, before the offer is completed, a term adopted or a leader observed.</summary>
    /// <param name="malformation">The damage to the second entry: 0 garbage bytes, 1 a trailing byte, 2 the bytes of another position.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task MalformedEntryRefusesTheBatch(int malformation, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-malformed");
        await using var registry = await OpenRegistryAsync(dir, true, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var damaged = Damaged(malformation);
        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch([Entry(1), damaged], LeaderId, 1, 0, 0, 0), cancellationToken);

        _ = await Assert.That((result.Success, result.RefusalCode, result.CurrentTerm, result.LastLogIndex)).IsEqualTo((false, RefusalCodes.ChecksumMismatch, 0UL, 0UL));
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.MemberState, status?.CurrentTerm, status?.LastLogIndex)).IsEqualTo((GroupMemberState.Unseeded, 0UL, 0UL));
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(metaPath, cancellationToken)).BootstrapOffered).IsTrue();
        var after = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(registry.StateFor(GroupId).ReadRoute().HasLeader).IsFalse();
    }

    private static Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, bool offered, CancellationToken cancellationToken) =>
        OpenRegistryAsync(dir, (new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, offered), cancellationToken);

    private static Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, IFollowerLogFaultHooks faults, CancellationToken cancellationToken) =>
        OpenRegistryAsync(dir, (new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System, FaultHooks = faults }, false), cancellationToken);

    private static async Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, (FollowerLogOptions Options, bool Offered) setup, CancellationToken cancellationToken)
    {
        var offered = setup.Offered;
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, Fingerprint, 1UL, NullLoggerFactory.Instance, setup.Options) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
            if (offered && registry.TryGetLog(GroupId, out var log))
                _ = await log.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    private static FollowerBatch Batch(string leaderNodeId) => new([Entry(1)], leaderNodeId, 1, 0, 0, 0);

    private static FollowerLogEntry Damaged(int malformation) => malformation switch
    {
        0 => new FollowerLogEntry(2, 1, new byte[] { 1, 2, 3 }),
        1 => new FollowerLogEntry(2, 1, new ReadOnlyMemory<byte>([.. Entry(2).Payload.Span, 0])),
        _ => new FollowerLogEntry(2, 1, Entry(3).Payload),
    };

    private static FollowerLogEntry Entry(ulong logIndex)
    {
        var record = new ReplicaLogRecord(
            logIndex,
            1,
            $"op-{logIndex}",
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
        return FollowerLogEntry.Of(in record);
    }
}
