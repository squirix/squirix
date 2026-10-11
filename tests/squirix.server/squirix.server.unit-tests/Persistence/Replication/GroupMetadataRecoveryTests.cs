using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>What a follower log reads back from its two-slot metadata file: term, vote, and the refusal of files it cannot trust.</summary>
[Immutable]
public sealed class GroupMetadataRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "meta-recovery";

    /// <summary>A term step and a vote grant are read back by a reopen.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TermAndVoteSurviveReopen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-reopen");
        await GrantVoteAfterTermStepAsync(dir, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((4UL, "node-b"));
    }

    /// <summary>
    /// With the newest slot corrupted after a vote grant, a reopen reads the previous record, which holds the term step but not the vote. That
    /// matches a crash during the grant's publish: the grant is reported only after the slot is written and flushed, so a slot that did not
    /// survive belongs to a grant nobody was told about, and the vote can safely be cast again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CorruptNewestSlotReadsPreviousRecord(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-corrupt");
        await GrantVoteAfterTermStepAsync(dir, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var newest = GroupMetadataStore.Read(path);
        await FollowerLogTestKit.CorruptByteAsync(path, (newest.Slot * GroupMetadataSlots.SlotSize) + 20, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((4UL, string.Empty));
        var other = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-c", 4UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(other.Granted).IsTrue();
    }

    /// <summary>With both slots corrupt the log refuses to open and leaves the file untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BothSlotsCorruptFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-both");
        await GrantVoteAfterTermStepAsync(dir, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        await FollowerLogTestKit.CorruptByteAsync(path, 20, cancellationToken);
        await FollowerLogTestKit.CorruptByteAsync(path, GroupMetadataSlots.SlotSize + 20, cancellationToken);
        var before = await File.ReadAllBytesAsync(path, cancellationToken);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("corrupt", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>A metadata file in the layout of an earlier build fails readiness with a message naming the cause, and is not modified.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierLayoutFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-earlier");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var meta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 2UL, string.Empty, 0UL, 0UL);
        var earlier = new byte[GroupLogCodec.ComputeMetaEncodedLength(in meta)];
        GroupLogCodec.EncodeMeta(in meta, earlier);
        await File.WriteAllBytesAsync(path, earlier, cancellationToken);

        await using var log = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(log.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("earlier build", StringComparison.Ordinal);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(earlier, after);
    }

    /// <summary>Observes term three, then grants a vote in term four, which publishes the term step and the grant separately, and closes the log.</summary>
    /// <param name="dir">The persistence root.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the log is closed.</returns>
    private static async Task GrantVoteAfterTermStepAsync(TempDirectory dir, CancellationToken cancellationToken)
    {
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 4UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(granted.Granted).IsTrue();
    }

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
}
