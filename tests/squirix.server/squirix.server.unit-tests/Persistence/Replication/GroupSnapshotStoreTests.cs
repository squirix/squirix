using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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

/// <summary>The two-slot snapshot store: mirrored in-place publishes, the slot rule on read, and the faults around each slot write.</summary>
[Immutable]
public sealed class GroupSnapshotStoreTests : ServerUnitTestBase
{
    private const string GroupId = "snapshot-store";

    /// <summary>Every publish writes the same image into both slots, the slot that is not current first, and the sequence counts each write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishMirrorsImageIntoBothSlots(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-mirror");
        var store = NewStore(dir, new StateInstallFaultHooks());

        await store.PublishAsync(Snapshot(4UL), cancellationToken);
        var (firstA, firstB) = await ReadSlotsAsync(dir, cancellationToken);
        var snapshot = Snapshot(5UL) with { CapturedUtc = DateTime.UnixEpoch.AddTicks(12345) };
        IFollowerLogSnapshotStore publisher = store;
        var verified = await publisher.PublishVerifiedAsync(snapshot, cancellationToken);
        var (secondA, secondB) = await ReadSlotsAsync(dir, cancellationToken);

        _ = await Assert.That(verified?.CapturedUtc).IsEqualTo(DateTime.UnixEpoch.AddMilliseconds(1));
        await SequenceAssert.EqualAsync(firstA, firstB);
        await SequenceAssert.EqualAsync(firstB, secondB);
        _ = await Assert.That(secondA.AsSpan().SequenceEqual(firstA)).IsFalse();
        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(5UL);
    }

    /// <summary>Publishing creates the two slot files and nothing else: no temp file is left in the group directory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishLeavesNoOtherFiles(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-files");
        var store = NewStore(dir, new StateInstallFaultHooks());

        await store.PublishAsync(Snapshot(4UL), cancellationToken);
        await store.PublishAsync(Snapshot(5UL), cancellationToken);

        var names = Directory.GetFiles(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        Array.Sort(names, StringComparer.Ordinal);
        _ = await Assert.That(names.Length).IsEqualTo(3);
        _ = await Assert.That(File.Exists(GroupSnapshotFile.PointerPath(dir, GroupId))).IsTrue();
    }

    /// <summary>A failure before the first slot write changes no byte: the previous snapshot stays, and the next publish succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstSlotWriteFaultKeepsPreviousSnapshot(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-first-fault");
        var faults = new StateInstallFaultHooks();
        var store = NewStore(dir, faults);
        await store.PublishAsync(Snapshot(4UL), cancellationToken);
        var (beforeA, beforeB) = await ReadSlotsAsync(dir, cancellationToken);
        faults.FailSnapshotSlotWrite(1);

        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(store.PublishAsync(Snapshot(5UL), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        var (afterA, afterB) = await ReadSlotsAsync(dir, cancellationToken);
        await SequenceAssert.EqualAsync(beforeA, afterA);
        await SequenceAssert.EqualAsync(beforeB, afterB);
        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(4UL);
        await store.PublishAsync(Snapshot(6UL), cancellationToken);
        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(6UL);
    }

    /// <summary>A failure at the second slot write leaves the new snapshot published, although the caller sees the exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondSlotWriteFaultPublishesNewSnapshot(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-second-fault");
        var faults = new StateInstallFaultHooks();
        var store = NewStore(dir, faults);
        await store.PublishAsync(Snapshot(4UL), cancellationToken);
        faults.FailSnapshotSlotWrite(2);

        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(store.PublishAsync(Snapshot(5UL), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(await ReadCommitAsync(NewStore(dir, new StateInstallFaultHooks()), cancellationToken)).IsEqualTo(5UL);
        await store.PublishAsync(Snapshot(6UL), cancellationToken);
        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(6UL);
    }

    /// <summary>A torn slot is ignored on read and overwritten by the next publish, which leaves two valid slots again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishRecoversFromTornSlot(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-torn");
        var store = NewStore(dir, new StateInstallFaultHooks());
        await store.PublishAsync(Snapshot(4UL), cancellationToken);
        await store.PublishAsync(Snapshot(5UL), cancellationToken);
        await GroupSnapshotFile.TearAsync(dir, GroupId, 1, 30, cancellationToken);

        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(5UL);
        await store.PublishAsync(Snapshot(6UL), cancellationToken);

        var (slotA, slotB) = await ReadSlotsAsync(dir, cancellationToken);
        await SequenceAssert.EqualAsync(slotA, slotB);
        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(6UL);
    }

    /// <summary>A first publish that stopped half way, with only a torn slot 0, reads as no snapshot and is completed by the next publish.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishOverInterruptedFirstPublish(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-interrupted");
        var store = NewStore(dir, new StateInstallFaultHooks());
        await File.WriteAllBytesAsync(GroupSnapshotFile.PointerPath(dir, GroupId), new byte[10], cancellationToken);

        _ = await Assert.That(await store.ReadPublishedAsync(cancellationToken) == null).IsTrue();
        await store.PublishAsync(Snapshot(4UL), cancellationToken);

        var (slotA, slotB) = await ReadSlotsAsync(dir, cancellationToken);
        await SequenceAssert.EqualAsync(slotA, slotB);
        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(4UL);
    }

    /// <summary>A reader that holds both slot files open does not block a publish, and the publish is visible to a later read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishSucceedsWhileReaderHoldsSlot(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-reader");
        var store = NewStore(dir, new StateInstallFaultHooks());
        await store.PublishAsync(Snapshot(4UL), cancellationToken);
        const FileShare share = FileShare.ReadWrite | FileShare.Delete;
        using var first = File.OpenHandle(GroupSnapshotFile.SlotPath(dir, GroupId, 0), FileMode.Open, FileAccess.Read, share);
        using var second = File.OpenHandle(GroupSnapshotFile.SlotPath(dir, GroupId, 1), FileMode.Open, FileAccess.Read, share);

        await store.PublishAsync(Snapshot(5UL), cancellationToken);

        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(5UL);
    }

    /// <summary>Once the first slot is flushed, a cancellation does not stop the mirror write, and a reopen selects the new snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelAfterFirstSlotKeepsMirror(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-cancel");
        using var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnSnapshotPublishing().Callback(
            Bind.To<CancellationTokenSource, Action>(
                source,
                static cancel =>
                {
                    var writes = 0;
                    return () =>
                    {
                        if (++writes == 2)
                            _ = cancel.CancelAsync();
                    };
                }));
        var store = NewStore(dir, faults.Instance());

        await store.PublishAsync(Snapshot(4UL), source.Token);

        _ = await Assert.That(source.IsCancellationRequested).IsTrue();
        var (slotA, slotB) = await ReadSlotsAsync(dir, cancellationToken);
        await SequenceAssert.EqualAsync(slotA, slotB);
        _ = await Assert.That(await ReadCommitAsync(NewStore(dir, null), cancellationToken)).IsEqualTo(4UL);
    }

    /// <summary>A referenced empty image falls back to its intact mirror.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyImageFallsBackToMirror(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-empty");
        var store = NewStore(dir, new StateInstallFaultHooks());
        await store.PublishAsync(Snapshot(4UL), cancellationToken);
        await GroupSnapshotFile.TearAsync(dir, GroupId, 0, 0, cancellationToken);

        _ = await Assert.That(await ReadCommitAsync(store, cancellationToken)).IsEqualTo(4UL);
    }

    /// <summary>Recovery refuses an oversized referenced snapshot when no usable mirror remains.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryRefusesOversizeImages(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-oversize");
        var store = new GroupSnapshotStore(dir, GroupId, 200);
        await GroupSnapshotFile.WriteImageAsync(dir, GroupId, new byte[300], true, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadPublishedAsync(cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>The single snapshot file of an earlier build is refused on read and on publish, and is left unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LegacySnapshotFileIsRefused(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-legacy");
        var legacyPath = GroupStoragePaths.GetLegacySnapshotPath(dir, GroupId);
        byte[] legacy = [1, 2, 3, 4];
        await File.WriteAllBytesAsync(legacyPath, legacy, cancellationToken);

        var store = NewStore(dir, new StateInstallFaultHooks());

        var read = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadPublishedAsync(cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.PublishAsync(Snapshot(4UL), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(read.Message).Contains("previous replica snapshot format is unsupported", StringComparison.Ordinal);
        _ = await Assert.That(store.SnapshotExists).IsTrue();
        var after = await File.ReadAllBytesAsync(legacyPath, cancellationToken);
        await SequenceAssert.EqualAsync(legacy, after);
        _ = await Assert.That(GroupSnapshotFile.Exists(dir, GroupId)).IsFalse();
    }

    /// <summary>A group with no slot files, or no group directory yet, has no published snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GroupWithoutSlotFilesHasNoSnapshot(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-absent");
        using var missingDir = new TempDirectory("squirix-snapshot-store-no-directory");

        _ = await Assert.That(await NewStore(dir, new StateInstallFaultHooks()).ReadPublishedAsync(cancellationToken)).IsNull();
        _ = await Assert.That(await NewStore(missingDir, new StateInstallFaultHooks()).ReadPublishedAsync(cancellationToken)).IsNull();
    }

    /// <summary>A snapshot over the size bound is refused before any file is created.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TooLargeWritesNothing(CancellationToken cancellationToken)
    {
        using var dir = NewDirectory("squirix-snapshot-store-too-large");
        var store = new GroupSnapshotStore(dir, GroupId, 16);

        _ = await NodeAsyncAssert.ThrowsAsync<GroupSnapshotTooLargeException>(store.PublishAsync(Snapshot(4UL), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(GroupSnapshotFile.Exists(dir, GroupId)).IsFalse();
        _ = await Assert.That(Directory.GetFiles(GroupStoragePaths.GetGroupDirectory(dir, GroupId)).Length).IsEqualTo(0);
    }

    private static TempDirectory NewDirectory(string prefix)
    {
        var dir = new TempDirectory(prefix);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        return dir;
    }

    private static GroupSnapshotStore NewStore(string dir, IFollowerLogFaultHooks? faults) => new(dir, GroupId, faults: faults);

    private static async Task<(byte[] First, byte[] Second)> ReadSlotsAsync(string dir, CancellationToken cancellationToken)
    {
        var first = await GroupSnapshotFile.ReadSlotBytesAsync(dir, GroupId, 0, cancellationToken);
        var second = await GroupSnapshotFile.ReadSlotBytesAsync(dir, GroupId, 1, cancellationToken);
        return (first ?? [], second ?? []);
    }

    private static async Task<ulong> ReadCommitAsync(GroupSnapshotStore store, CancellationToken cancellationToken)
    {
        var snapshot = await store.ReadPublishedAsync(cancellationToken);
        return snapshot?.CommitIndex ?? 0UL;
    }

    private static GroupSnapshot Snapshot(ulong commitIndex) => new(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, commitIndex, [], DateTime.UnixEpoch);
}
