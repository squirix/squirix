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

/// <summary>The offer, the completion on a member call and the completion of the owner: what each persists, refuses and survives.</summary>
[Immutable]
public sealed class GroupBootstrapTests : ServerUnitTestBase
{
    private const string GroupId = "group-bootstrap";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>An unseeded copy accepts an offer, holds it in both slots, and accepts it again, which holds it in both slots again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferIsAcceptedAndIdempotent(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-offer");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);

        var first = await log.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var written = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        var second = await log.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);

        var repeated = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        _ = await Assert.That(first).IsNull();
        _ = await Assert.That(second).IsNull();
        await AssertBothSlotsAsync(written, GroupMemberState.Unseeded, true);
        await AssertBothSlotsAsync(repeated, GroupMemberState.Unseeded, true);
    }

    /// <summary>An offer that reached only one metadata slot before a crash is published to both slots by the next offer, so damage to either slot afterwards keeps it.</summary>
    /// <param name="damagedSlot">The slot that is damaged after the second offer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task RepeatedOfferFillsBothSlots(int damagedSlot, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-half-offer");
        var publishes = 0;
        var failAt = int.MaxValue;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFrameWritten();
        _ = faults.Setups.OnFlushed();
        _ = faults.Setups.OnCommitAdvanced();
        _ = faults.Setups.OnBeforeMemoryApply();
        _ = faults.Setups.OnMetaPublishing().Callback(() =>
        {
            if (++publishes == failAt)
                throw new IOException("simulated failure at the second slot write of the offer.");
        });

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults.Instance(), TimeProvider.System))
        {
            await log.OpenAsync(cancellationToken);
            failAt = publishes + 2;

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken));
        }

        await using (var reopened = OpenLog(dir))
        {
            await reopened.OpenAsync(cancellationToken);
            var halfWritten = await ReadOfferedAsync(dir, cancellationToken);

            var again = await reopened.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);

            _ = await Assert.That(halfWritten).IsTrue();
            _ = await Assert.That(again).IsNull();
        }

        await FollowerLogTestKit.CorruptByteAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), (damagedSlot * GroupMetadataSlots.SlotSize) + 20, cancellationToken);

        await using var damaged = OpenLog(dir);
        await damaged.OpenAsync(cancellationToken);
        var status = await damaged.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.Readiness)).IsEqualTo((GroupMemberState.Unseeded, FollowerLogReadiness.Ready));
        _ = await Assert.That(await ReadOfferedAsync(dir, cancellationToken)).IsTrue();
    }

    /// <summary>A complete copy refuses an offer as not ready, and an offer of another topology is refused without being recorded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferRefusals(CancellationToken cancellationToken)
    {
        using var completeDir = new TempDirectory("squirix-bootstrap-offer-complete");
        using var mismatchDir = new TempDirectory("squirix-bootstrap-offer-mismatch");
        await using var complete = OpenLog(completeDir);
        await complete.OpenSeededAsync(cancellationToken);
        await using var mismatched = OpenLog(mismatchDir);
        await mismatched.OpenAsync(cancellationToken);
        await mismatched.AdoptTopologyAsync(Fingerprint, 1UL, cancellationToken);

        var onComplete = await complete.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);
        var otherFingerprint = await mismatched.AcceptBootstrapOfferAsync(new byte[] { 4 }, 1UL, cancellationToken);
        var olderGeneration = await mismatched.AcceptBootstrapOfferAsync(Fingerprint, 0UL, cancellationToken);

        _ = await Assert.That(onComplete).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(otherFingerprint).IsEqualTo(FollowerLogRefusal.TopologyMismatch);
        _ = await Assert.That(olderGeneration).IsEqualTo(FollowerLogRefusal.TopologyMismatch);
        _ = await Assert.That(await ReadOfferedAsync(mismatchDir, cancellationToken)).IsFalse();
    }

    /// <summary>A copy without an offer is not completed by a member call and stays as it was.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletionNeedsAnOffer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-no-offer");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);

        var refusal = await log.CompleteIfOfferedAsync(Fingerprint, 1UL, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(FollowerLogRefusal.Unseeded);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, await ReadOfferedAsync(dir, cancellationToken))).IsEqualTo((GroupMemberState.Unseeded, false));
    }

    /// <summary>An offered copy completes on a member call, keeps its topology and term, and holds the complete record in both slots.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferedCopyCompletes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-complete");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        await log.AdoptTopologyAsync(Fingerprint, 2UL, cancellationToken);
        _ = await log.AcceptBootstrapOfferAsync(Fingerprint, 2UL, cancellationToken);

        var refusal = await log.CompleteIfOfferedAsync(Fingerprint, 2UL, cancellationToken);
        var again = await log.CompleteIfOfferedAsync(Fingerprint, 2UL, cancellationToken);

        _ = await Assert.That(refusal).IsNull();
        _ = await Assert.That(again).IsNull();
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.ConfigurationGeneration, status.CurrentTerm)).IsEqualTo((GroupMemberState.Complete, 2UL, 0UL));
        await SequenceAssert.EqualAsync(Fingerprint, status.TopologyFingerprint.ToArray());
        await AssertBothSlotsAsync(await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken), GroupMemberState.Complete, true);
    }

    /// <summary>A completion of another topology is refused and leaves the copy unseeded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletionChecksTopology(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-topology");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        await log.AdoptTopologyAsync(Fingerprint, 1UL, cancellationToken);
        _ = await log.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);

        var mismatch = await log.CompleteIfOfferedAsync(new byte[] { 4 }, 1UL, cancellationToken);

        _ = await Assert.That(mismatch).IsEqualTo(FollowerLogRefusal.TopologyMismatch);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>An incomplete copy refuses the offer as not ready and the completion and appends as incomplete, and nothing completes it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IncompleteCopyRefusesEverything(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-incomplete");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var meta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, GroupMemberState.Incomplete, true);
        await GroupMetadataFile.WriteAsync(path, in meta, cancellationToken);
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);

        var offer = await log.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);
        var completion = await log.CompleteIfOfferedAsync(Fingerprint, 1UL, cancellationToken);
        var append = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(log.CompleteBootstrapAsync(cancellationToken));

        _ = await Assert.That((offer, completion, append.RefusalCode)).IsEqualTo((FollowerLogRefusal.NotReady, FollowerLogRefusal.Incomplete, FollowerLogRefusal.Incomplete));
        _ = await Assert.That(ex.Message).Contains(GroupId, StringComparison.Ordinal);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
    }

    /// <summary>The completion of the owner completes an unseeded copy, which then takes entries, and refuses a copy that is complete already.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerCompletionCompletesCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-owner");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);

        await log.CompleteBootstrapAsync(cancellationToken);
        await AssertBothSlotsAsync(await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken), GroupMemberState.Complete, false);
        var appended = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(log.CompleteBootstrapAsync(cancellationToken));

        _ = await Assert.That(appended.Success).IsTrue();
    }

    /// <summary>The complete flag read without the gate follows the copy: unset while fresh or offered, set by either completion and by reopening a complete copy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompleteFlagFollowsTheCopy(CancellationToken cancellationToken)
    {
        using var memberDir = new TempDirectory("squirix-bootstrap-flag-member");
        using var ownerDir = new TempDirectory("squirix-bootstrap-flag-owner");
        bool fresh;
        bool offered;
        bool completed;
        await using (var member = OpenLog(memberDir))
        {
            await member.OpenAsync(cancellationToken);
            fresh = member.IsComplete;
            _ = await member.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);
            offered = member.IsComplete;
            _ = await member.CompleteIfOfferedAsync(Fingerprint, 1UL, cancellationToken);
            completed = member.IsComplete;
        }

        await using var reopened = OpenLog(memberDir);
        await reopened.OpenAsync(cancellationToken);
        await using var owner = OpenLog(ownerDir);
        await owner.OpenAsync(cancellationToken);
        await owner.CompleteBootstrapAsync(cancellationToken);

        _ = await Assert.That((fresh, offered, completed)).IsEqualTo((false, false, true));
        _ = await Assert.That((reopened.IsComplete, owner.IsComplete)).IsEqualTo((true, true));
    }

    /// <summary>A fault on the first or the second slot write of the completion fails readiness, a reopen reads a legal state, and a later member call ends complete.</summary>
    /// <param name="failedWrite">The completion write that fails: the first or the second slot.</param>
    /// <param name="reopenedComplete">Whether a reopen reads the complete record, rather than the offered one.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, false)]
    [Arguments(2, true)]
    public async Task CompletionFaultLeavesLegalState(int failedWrite, bool reopenedComplete, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-fault");
        var publishes = 0;
        var failAt = int.MaxValue;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFrameWritten();
        _ = faults.Setups.OnFlushed();
        _ = faults.Setups.OnCommitAdvanced();
        _ = faults.Setups.OnBeforeMemoryApply();
        _ = faults.Setups.OnMetaPublishing().Callback(() =>
        {
            if (++publishes == failAt)
                throw new IOException("simulated failure at a completion publish.");
        });

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults.Instance(), TimeProvider.System))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AcceptBootstrapOfferAsync(Fingerprint, 1UL, cancellationToken);
            failAt = publishes + failedWrite;

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompleteIfOfferedAsync(Fingerprint, 1UL, cancellationToken));

            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var state = await reopened.GetStatusAsync(cancellationToken);
        var offered = await ReadOfferedAsync(dir, cancellationToken);
        var completion = await reopened.CompleteIfOfferedAsync(Fingerprint, 1UL, cancellationToken);

        _ = await Assert.That((state.MemberState == GroupMemberState.Complete, offered)).IsEqualTo((reopenedComplete, true));
        _ = await Assert.That(completion).IsNull();
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
    }

    private static async Task AssertBothSlotsAsync(byte[] bytes, GroupMemberState state, bool offered)
    {
        for (var slot = 0; slot < 2; slot++)
        {
            var alone = new byte[bytes.Length];
            bytes.CopyTo(alone, 0);
            Array.Clear(alone, (1 - slot) * GroupMetadataSlots.SlotSize, GroupMetadataSlots.SlotSize);

            var read = GroupMetadataSlots.Select(alone);

            _ = await Assert.That((read.Status, read.Metadata.MemberState, read.Metadata.BootstrapOffered)).IsEqualTo((GroupMetadataReadStatus.Valid, state, offered));
        }
    }

    private static async Task<bool> ReadOfferedAsync(TempDirectory dir, CancellationToken cancellationToken) =>
        (await GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken)).BootstrapOffered;

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
}
