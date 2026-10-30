using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests owner-only permissions of journal, manifest, snapshot and replica storage files.</summary>
[Immutable]
public sealed class StorageFilePermissionsTests : ServerUnitTestBase
{
    private const string GroupId = "permissions";

    /// <summary>A journal segment is created owner-only.</summary>
    [Test]
    public async Task JournalSegmentIsOwnerOnly()
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-private-segment");
        var path = Path.Join(root, "jrn-000001.jsqx");
        using var writer = JournalSegmentWriterFactory.Create();

        writer.OpenSegment(path, false);

        _ = await Assert.That(File.GetUnixFileMode(path)).IsEqualTo(FileEx.PrivateFileMode);
    }

    /// <summary>A manifest slot file is created owner-only.</summary>
    [Test]
    public async Task ManifestSlotIsOwnerOnly()
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-private-manifest");
        var path = Path.Join(root, "man-current");

        using (var writer = new SlotWriter(path))
            _ = writer.Publish(new State { CurrentJournal = 1 });

        _ = await Assert.That(File.GetUnixFileMode(path)).IsEqualTo(FileEx.PrivateFileMode);
    }

    /// <summary>The snapshot final file, the group metadata store and the group snapshot store create owner-only files.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoresCreateOwnerOnlyFiles(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-private-stores");
        var snapshotPath = await new SnapshotWriter(root).WriteSingleAsync(1, CacheKey.Default("k"), new NodeCacheEntry<object?> { Value = "v", Version = 1 }, cancellationToken);

        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(root, GroupId));
        using var metadata = new GroupMetadataStore(
            GroupStoragePaths.GetMetadataPath(root, GroupId),
            GroupStoragePaths.GetMetadataTempPath(root, GroupId),
            GroupId,
            new CountingFollowerLogFaultHooks(),
            NullLogger.Instance);
        metadata.Publish(new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, string.Empty, 0UL, 0UL, GroupMemberState.Complete, false));

        await new GroupSnapshotStore(root, GroupId).PublishAsync(
            new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, [], DateTime.UnixEpoch),
            cancellationToken);

        _ = await Assert.That(File.GetUnixFileMode(snapshotPath)).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(GroupStoragePaths.GetMetadataPath(root, GroupId))).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(GroupStoragePaths.GetSnapshotSlotPath(root, GroupId, 0))).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(GroupStoragePaths.GetSnapshotSlotPath(root, GroupId, 1))).IsEqualTo(FileEx.PrivateFileMode);
    }
}
