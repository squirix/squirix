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

/// <summary>The install gate of a group: one transfer at a time, a higher term takes over, stale messages change nothing, and a silent transfer expires.</summary>
[Immutable]
public sealed class ReplicaTransferGateTests : ServerUnitTestBase
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(5);

    /// <summary>A second begin of the same term while a transfer runs is refused as busy and changes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondBeginIsBusy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-busy");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync("first", 1UL, cancellationToken);

        var second = await harness.Receiver.BeginAsync(HeaderOf("second", 1UL), BaselineOf(TransferFixtures.Image()), cancellationToken);
        var next = await harness.Receiver.WipeAsync(GroupId, "first", sequence, cancellationToken);

        _ = await Assert.That((second.Success, second.RefusalCode)).IsEqualTo((false, RefusalCodes.TransferBusy));
        _ = await Assert.That(next.Success).IsTrue().Because("The held transfer goes on.");
    }

    /// <summary>A begin of a higher term takes the group over at once; the old transfer's later messages are stale and it cannot finish over the new one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermTakesOverAndOldIsStale(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-takeover");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var oldSequence = await harness.BeginAndUploadAsync("old", 1UL, cancellationToken);
        oldSequence = await harness.WipeAllAsync("old", oldSequence, cancellationToken);
        var empty = RecordingStateSink.TotalsOf();

        var newSequence = await harness.BeginAndUploadAsync("new", 2UL, cancellationToken);
        var oldWipe = await harness.Receiver.WipeAsync(GroupId, "old", oldSequence, cancellationToken);
        var oldEnd = await harness.Receiver.EndAsync(GroupId, "old", oldSequence, empty.EntryCount, empty.Crc, cancellationToken);

        _ = await Assert.That((oldWipe.Success, oldWipe.RefusalCode)).IsEqualTo((false, RefusalCodes.TransferStale));
        _ = await Assert.That((oldEnd.Success, oldEnd.RefusalCode)).IsEqualTo((false, RefusalCodes.TransferStale));
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsFalse().Because("The old transfer's wipe made the copy incomplete, and the new one has not finished.");
        newSequence = await harness.WipeAllAsync("new", newSequence, cancellationToken);
        var end = await harness.Receiver.EndAsync(GroupId, "new", newSequence, empty.EntryCount, empty.Crc, cancellationToken);
        _ = await Assert.That(end.Success).IsTrue();
        _ = await Assert.That((await harness.Receiver.AbortAsync(GroupId, "old", cancellationToken)).Success).IsTrue().Because("Nothing is held any more.");
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(2UL);
    }

    /// <summary>A message with another transfer id or another sequence number is refused as stale and changes nothing: the expected message is still accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WrongIdOrSequenceChangesNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-stale");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var image = TransferFixtures.Image();
        _ = await harness.Receiver.BeginAsync(HeaderOf("t", 1UL), BaselineOf(image), cancellationToken);

        var otherId = await harness.Receiver.SnapshotAsync(GroupId, "other", 1, 0, image, cancellationToken);
        var skipped = await harness.Receiver.SnapshotAsync(GroupId, "t", 2, 0, image, cancellationToken);
        var repeated = await harness.Receiver.SnapshotAsync(GroupId, "t", 0, 0, image, cancellationToken);
        var wrongOffset = await harness.Receiver.SnapshotAsync(GroupId, "t", 1, 5, image, cancellationToken);
        var early = await harness.Receiver.WipeAsync(GroupId, "t", 1, cancellationToken);
        var accepted = await harness.Receiver.SnapshotAsync(GroupId, "t", 1, 0, image, cancellationToken);

        _ = await Assert.That((otherId.RefusalCode, skipped.RefusalCode, repeated.RefusalCode, wrongOffset.RefusalCode, early.RefusalCode))
                        .IsEqualTo((RefusalCodes.TransferStale, RefusalCodes.TransferStale, RefusalCodes.TransferStale, RefusalCodes.TransferStale, RefusalCodes.TransferStale));
        _ = await Assert.That(accepted.Success).IsTrue();
        await SequenceAssert.EqualAsync(["enter"], harness.Sink.Calls, StringComparer.Ordinal);
    }

    /// <summary>A transfer silent for the idle timeout is dropped by the timer: the group is released and a new begin succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleTransferExpiresAndReleasesGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-idle");
        await using var harness = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { IdleTimeout = TimeSpan.FromSeconds(30) });
        var sequence = await harness.BeginAndUploadAsync("silent", 1UL, cancellationToken);
        _ = await Assert.That(harness.Sink.Held).IsTrue();

        harness.Clock.Advance(TimeSpan.FromSeconds(29));
        _ = await Assert.That((await harness.Receiver.WipeAsync(GroupId, "silent", sequence, cancellationToken)).Success).IsTrue().Because("A message refreshes the activity.");
        harness.Clock.Advance(TimeSpan.FromSeconds(29));
        _ = await Assert.That(harness.Registry.TransferHoldFor(GroupId).IsHeld).IsTrue();
        harness.Clock.Advance(TimeSpan.FromSeconds(2));

        _ = await Assert.That(harness.Registry.TransferHoldFor(GroupId).IsHeld).IsFalse();
        _ = await Assert.That(harness.Sink.Held).IsFalse();
        var stale = await harness.Receiver.WipeAsync(GroupId, "silent", sequence + 1, cancellationToken);
        _ = await Assert.That(stale.RefusalCode).IsEqualTo(RefusalCodes.TransferStale);
        var again = await harness.Receiver.BeginAsync(HeaderOf("again", 1UL), BaselineOf(TransferFixtures.Image()), cancellationToken);
        _ = await Assert.That(again.Success).IsTrue();
    }

    /// <summary>Every accepted message is a contact of the leader for the election state; a refused message is not.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedMessagesAreLeaderContact(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-contact");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var election = harness.Registry.StateFor(GroupId);
        var image = TransferFixtures.Image();
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsFalse();

        var refused = await harness.Receiver.BeginAsync(HeaderOf("t", 0UL), BaselineOf(image), cancellationToken);
        _ = await Assert.That((refused.Success, refused.RefusalCode)).IsEqualTo((false, RefusalCodes.StaleTerm));
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsFalse();

        _ = await harness.Receiver.BeginAsync(HeaderOf("t", 1UL), BaselineOf(image), cancellationToken);
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsTrue();
        harness.Clock.Advance(Window + Window);
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsFalse();
        var stale = await harness.Receiver.SnapshotAsync(GroupId, "t", 7, 0, image, cancellationToken);
        _ = await Assert.That(stale.Success).IsFalse();
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsFalse();
        var chunk = await harness.Receiver.SnapshotAsync(GroupId, "t", 1, 0, image, cancellationToken);
        _ = await Assert.That(chunk.Success).IsTrue();
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsTrue();
        harness.Clock.Advance(Window + Window);
        var wipe = await harness.Receiver.WipeAsync(GroupId, "t", 2, cancellationToken);
        _ = await Assert.That(wipe.Success).IsTrue();
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsTrue();
    }

    /// <summary>An abort drops the transfer and releases the group; the copy stays as it is, and another transfer's abort is stale.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbortReleasesGroupAndLeavesCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-abort");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync("t", 1UL, cancellationToken);
        _ = await harness.WipeAllAsync("t", sequence, cancellationToken);

        var other = await harness.Receiver.AbortAsync(GroupId, "other", cancellationToken);
        var abort = await harness.Receiver.AbortAsync(GroupId, "t", cancellationToken);

        _ = await Assert.That((other.Success, other.RefusalCode)).IsEqualTo((false, RefusalCodes.TransferStale));
        _ = await Assert.That(abort.Success).IsTrue();
        _ = await Assert.That(harness.Registry.TransferHoldFor(GroupId).IsHeld).IsFalse();
        _ = await Assert.That(harness.Sink.Held).IsFalse();
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That((await log!.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete).Because("The wipe had marked the copy incomplete; an abort does not complete it.");
    }

    /// <summary>A begin the copy cannot take is refused with its code and writes nothing durable, holds no group and does not reach the sink.</summary>
    /// <param name="case">The refusal under test.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("topology-mismatch")]
    [Arguments("stale-term")]
    [Arguments("not-ready")]
    [Arguments("not-member")]
    public async Task BeginRefusalsWriteNothing(string @case, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-refusals");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var baseline = BaselineOf(TransferFixtures.Image());
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);

        var reply = @case switch
        {
            "topology-mismatch" => await harness.Receiver.BeginAsync(HeaderOf("t", 1UL), baseline with { Fingerprint = new byte[] { 1 } }, cancellationToken),
            "stale-term" => await harness.Receiver.BeginAsync(HeaderOf("t", 0UL), baseline, cancellationToken),
            "not-ready" => await harness.Receiver.BeginAsync(HeaderOf("t", 1UL), baseline with { Index = 1UL }, cancellationToken),
            _ => await harness.Receiver.BeginAsync(new ReplicaTransferHeader("unknown-group", "t", LeaderId, 1UL), baseline, cancellationToken),
        };

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, @case));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(harness.Sink.Calls.Count).IsEqualTo(0);
        _ = await Assert.That(harness.Registry.TransferHoldFor(GroupId).IsHeld).IsFalse();
    }

    /// <summary>An unseeded copy is not repaired by a transfer: the begin is refused as unseeded and writes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededCopyIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-unseeded");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var unseeded = new GroupLogMetadata(GroupId, TransferFixtures.Fingerprint, 1UL, 0UL, string.Empty, 0UL, 0UL, GroupMemberState.Unseeded, false);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        await GroupMetadataFile.WriteAsync(path, in unseeded, cancellationToken);
        await using var harness = await OpenAsync(dir, cancellationToken);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);

        var reply = await harness.Receiver.BeginAsync(HeaderOf("t", 1UL), BaselineOf(TransferFixtures.Image()), cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.Unseeded));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(harness.Sink.Calls.Count).IsEqualTo(0);
    }

    /// <summary>A node that leads the group refuses the begin as not ready, whether the election state or the sink says so.</summary>
    /// <param name="byElectionState">Whether the election state leads; otherwise the sink reports that the apply driver is taken.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task LeaderRefusesBegin(bool byElectionState, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-leads");
        await using var harness = await OpenAsync(dir, cancellationToken);
        if (byElectionState)
        {
            harness.Registry.StateFor(GroupId).SetElectionDriven(true);
            _ = await Assert.That(harness.Registry.StateFor(GroupId).BecomeLeader(2UL)).IsTrue();
        }
        else
        {
            harness.Sink.Enters = false;
        }

        var reply = await harness.Receiver.BeginAsync(HeaderOf("t", 2UL), BaselineOf(TransferFixtures.Image()), cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.NotReady));
        _ = await Assert.That(harness.Registry.TransferHoldFor(GroupId).IsHeld).IsFalse();
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsTrue();
    }

    /// <summary>While a group holds a transfer, an offer of a first start is refused as busy, a member call finds an incomplete copy, and the copy is not completed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstStartIsExcludedWhileTransferRuns(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-gate-first-start");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var follower = new ReplicaFollower(harness.Registry, RocksDoubles.CreateReplicaMembers());
        var sequence = await harness.BeginAndUploadAsync("t", 1UL, cancellationToken);
        _ = await harness.Receiver.WipeAsync(GroupId, "t", sequence, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);

        var offer = await follower.AcceptOfferAsync(GroupId, GroupId, TransferFixtures.Fingerprint, 1UL, cancellationToken);
        var append = await follower.AppendAsync(GroupId, TransferFixtures.Fingerprint, 1UL, new FollowerBatch([], LeaderId, 1, 0, 0, 0), cancellationToken);

        _ = await Assert.That((offer.Accepted, offer.RefusalCode)).IsEqualTo((false, RefusalCodes.TransferBusy));
        _ = await Assert.That((append.Success, append.RefusalCode)).IsEqualTo((false, RefusalCodes.Incomplete));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That((await follower.GetStatusAsync(GroupId, cancellationToken))?.MemberState).IsEqualTo(GroupMemberState.Incomplete);
    }
}
