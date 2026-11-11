using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Single-copy snapshot publications and fallback through the pointer file.</summary>
public sealed class SnapshotCheckpointTests : ServerUnitTestBase
{
    private const string GroupId = "checkpoint";

    /// <summary>A damaged newest image leaves the preceding snapshot selectable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CorruptImageFallsBack(CancellationToken cancellationToken)
    {
        using var directory = new TempDirectory("snapshot-checkpoint-fallback");
        IFollowerLogSnapshotStore store = new GroupSnapshotStore(directory, GroupId);
        _ = await store.PublishVerifiedAsync(Snapshot(1UL), cancellationToken);
        _ = await store.PublishVerifiedAsync(Snapshot(2UL), cancellationToken);
        var folder = GroupStoragePaths.GetGroupDirectory(directory, GroupId);
        var pointers = GroupSnapshotPointers.Read(store.SnapshotPath);
        var path = Path.Join(folder, $"group.checkpoint-{pointers[0].Generation:N}.snapshot");
        await File.WriteAllBytesAsync(path, [0], cancellationToken);
        var recovered = await new GroupSnapshotStore(directory, GroupId).ReadPublishedAsync(cancellationToken);
        _ = await Assert.That(recovered?.LastIncludedIndex).IsEqualTo(1UL);
    }

    /// <summary>A retry after a torn initial pointer preserves full-WAL recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryRestoresFullWalMarker(CancellationToken cancellationToken)
    {
        using var directory = new TempDirectory("snapshot-checkpoint-initial-retry");
        IFollowerLogSnapshotStore store = new GroupSnapshotStore(directory, GroupId);
        var folder = GroupStoragePaths.GetGroupDirectory(directory, GroupId);
        _ = Directory.CreateDirectory(folder);
        await File.WriteAllBytesAsync(store.SnapshotPath, [0], cancellationToken);
        var orphan = Path.Join(folder, $"group.checkpoint-{Guid.NewGuid():N}.snapshot");
        await File.WriteAllBytesAsync(orphan, [0], cancellationToken);

        _ = await store.PublishVerifiedAsync(Snapshot(1UL), cancellationToken, 0UL);
        _ = await Assert.That(File.Exists(orphan)).IsFalse();
        var pointers = GroupSnapshotPointers.Read(store.SnapshotPath);
        _ = await Assert.That(pointers[1].Generation).IsEqualTo(Guid.Empty);
        _ = await Assert.That(pointers[1].Sequence).IsEqualTo(1UL);

        var path = Path.Join(folder, $"group.checkpoint-{pointers[0].Generation:N}.snapshot");
        await File.WriteAllBytesAsync(path, [0], cancellationToken);
        _ = await Assert.That(await store.ReadPublishedAsync(cancellationToken)).IsNull();
    }

    /// <summary>A torn pointer update leaves the preceding pointer intact.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornPointerFallsBack(CancellationToken cancellationToken)
    {
        using var directory = new TempDirectory("snapshot-checkpoint-pointer");
        IFollowerLogSnapshotStore store = new GroupSnapshotStore(directory, GroupId);
        _ = await store.PublishVerifiedAsync(Snapshot(1UL), cancellationToken);
        _ = await store.PublishVerifiedAsync(Snapshot(2UL), cancellationToken);
        var pointers = GroupSnapshotPointers.Read(store.SnapshotPath);
        using (var handle = File.OpenHandle(store.SnapshotPath, FileMode.Open, FileAccess.Write, FileShare.Read))
        {
            byte[] corrupt = [0];
            await RandomAccess.WriteAsync(handle, corrupt, pointers[0].Slot * GroupSnapshotPointers.SlotSize, cancellationToken);
        }

        var recovered = await new GroupSnapshotStore(directory, GroupId).ReadPublishedAsync(cancellationToken);
        _ = await Assert.That(recovered?.LastIncludedIndex).IsEqualTo(1UL);
    }

    /// <summary>Repeated publications retain exactly the two generations referenced by the pointer slots.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OldGenerationsAreReclaimed(CancellationToken cancellationToken)
    {
        using var directory = new TempDirectory("snapshot-checkpoint-cleanup");
        IFollowerLogSnapshotStore store = new GroupSnapshotStore(directory, GroupId);
        for (var index = 1UL; index <= 4UL; index++)
            _ = await store.PublishVerifiedAsync(Snapshot(index), cancellationToken);

        var folder = GroupStoragePaths.GetGroupDirectory(directory, GroupId);
        _ = await Assert.That(Directory.GetFiles(folder, "group.checkpoint-*.snapshot").Length).IsEqualTo(2);
        var recovered = await new GroupSnapshotStore(directory, GroupId).ReadPublishedAsync(cancellationToken);
        _ = await Assert.That(recovered?.LastIncludedIndex).IsEqualTo(4UL);
    }

    /// <summary>A state installation leaves two independently readable copies at the new boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallationMirrorsNewBoundary(CancellationToken cancellationToken)
    {
        using var directory = new TempDirectory("snapshot-checkpoint-install");
        IFollowerLogSnapshotStore store = new GroupSnapshotStore(directory, GroupId);
        await store.PublishAsync(Snapshot(7UL), cancellationToken);
        var pointers = GroupSnapshotPointers.Read(store.SnapshotPath);
        var folder = GroupStoragePaths.GetGroupDirectory(directory, GroupId);
        var path = Path.Join(folder, $"group.checkpoint-{pointers[0].Generation:N}.snapshot");
        await File.WriteAllBytesAsync(path, [0], cancellationToken);
        var recovered = await new GroupSnapshotStore(directory, GroupId).ReadPublishedAsync(cancellationToken);
        _ = await Assert.That(recovered?.LastIncludedIndex).IsEqualTo(7UL);
    }

    private static GroupSnapshot Snapshot(ulong index) =>
        new(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, index, index, [], DateTime.UnixEpoch);
}
