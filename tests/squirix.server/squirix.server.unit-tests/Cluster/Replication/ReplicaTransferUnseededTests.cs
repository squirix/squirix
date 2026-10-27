using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaTransferHarness;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// A state transfer at the receiver into a copy that lost its disk (#1126): taken only from a leader that checked the term ceiling, with the copy
/// unseeded until the end, which records the leader's term and a vote for the leader.
/// </summary>
[Immutable]
public sealed class ReplicaTransferUnseededTests : ServerUnitTestBase
{
    private const string Transfer = "transfer-unseeded";
    private const ulong LeaderTerm = 2UL;

    /// <summary>
    /// The whole transfer runs with the copy unseeded in both metadata slots, through the wipe and the journal barrier; the end makes it complete at
    /// the transferred index with the leader's term and a vote for the leader.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CeilingTransferRepairsUnseededCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-unseeded");
        await PlantUnseededAsync(dir, cancellationToken);
        await using var harness = await OpenAsync(dir, cancellationToken);
        harness.Sink.Keys = 1;
        var entries = new[] { Entry("k-1", 1) };
        var totals = RecordingStateSink.TotalsOf(entries);
        var header = HeaderOf(Transfer, LeaderTerm);
        var image = TransferFixtures.Image();

        var begin = await harness.Receiver.BeginAsync(header, BaselineOf(image) with { CeilingChecked = true }, cancellationToken);
        _ = await harness.Receiver.SnapshotAsync(in header, 1, 0, image, cancellationToken);
        var sequence = 2;
        ReplicaTransferReply wipe;
        do
            wipe = await harness.Receiver.WipeAsync(in header, sequence++, cancellationToken);
        while (wipe.More);

        _ = await harness.Receiver.EntriesAsync(in header, sequence++, entries, cancellationToken);
        var end = await harness.Receiver.EndAsync(in header, sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That((begin.Success, wipe.Success, end.Success)).IsEqualTo((true, true, true));
        _ = await Assert.That(harness.Sink.SlotsAtFirstWipe).IsEqualTo((GroupMemberState.Unseeded, GroupMemberState.Unseeded));
        _ = await Assert.That(harness.Sink.SlotsAtSeal).IsEqualTo((GroupMemberState.Unseeded, GroupMemberState.Unseeded));
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        var status = await log!.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.VotedFor, status.CommitIndex))
                        .IsEqualTo((GroupMemberState.Complete, LeaderTerm, LeaderId, TransferFixtures.SnapshotIndex));
    }

    /// <summary>A begin without the ceiling flag is refused as unseeded and writes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginWithoutCeilingIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-unseeded-refused");
        await PlantUnseededAsync(dir, cancellationToken);
        await using var harness = await OpenAsync(dir, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);

        var reply = await harness.Receiver.BeginAsync(HeaderOf(Transfer, LeaderTerm), BaselineOf(TransferFixtures.Image()), cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.Unseeded));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(harness.Sink.Calls.Count).IsEqualTo(0);
    }

    private static async Task PlantUnseededAsync(TempDirectory dir, CancellationToken cancellationToken)
    {
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var unseeded = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, GroupMemberState.Unseeded, false);
        await GroupMetadataFile.WriteAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), in unseeded, cancellationToken);
    }
}
