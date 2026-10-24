using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
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

namespace Squirix.Server.UnitTests.Storage;

/// <summary>Tests that persistence files are created owner-only and that existing looser entries are restricted at startup.</summary>
[Immutable]
public sealed class DataDirectoryPermissionsTests : ServerUnitTestBase
{
    private const string GroupId = "permissions";

    private const UnixFileMode LooseFile = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead;

    private const UnixFileMode LooseDirectory = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                                                UnixFileMode.GroupRead | UnixFileMode.GroupExecute | UnixFileMode.OtherRead | UnixFileMode.OtherExecute;

    /// <summary>Acquiring a missing directory creates every created level with 0700 and the lock file with 0600.</summary>
    [Test]
    public async Task AcquireCreatesOwnerOnlyEntries()
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-private-lock");
        var parent = Path.Join(root, "nested");
        var dataDir = Path.Join(parent, "data");

        using var held = DataDirectoryLock.Acquire(dataDir);

        _ = await Assert.That(File.GetUnixFileMode(Path.Join(dataDir, DataDirectoryLock.LockFileName))).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(dataDir)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
        _ = await Assert.That(File.GetUnixFileMode(parent)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
    }

    /// <summary>A group directory created through <see cref="DirectoryEx" /> is owner-only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CreateDirectoryIsOwnerOnly(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-private-dir");
        var groupDir = await DirectoryEx.CreateDirectoryAsync(Path.Join(root, "groups", "g1"), cancellationToken: cancellationToken);

        _ = await Assert.That(File.GetUnixFileMode(groupDir)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
    }

    /// <summary>A file opened for creation, and one that already existed, ends up owner-only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OpenPrivateRestrictsNewAndExistingFiles(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-private-file");
        var created = Path.Join(root, "created.bin");
        var existing = Path.Join(root, "existing.bin");
        await File.WriteAllBytesAsync(existing, [1], cancellationToken);
        File.SetUnixFileMode(existing, LooseFile);

        FileEx.OpenPrivate(created, FileMode.CreateNew, FileAccess.Write).Dispose();
        FileEx.OpenPrivate(existing, FileMode.OpenOrCreate, FileAccess.Write).Dispose();

        _ = await Assert.That(File.GetUnixFileMode(created)).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(existing)).IsEqualTo(FileEx.PrivateFileMode);
    }

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

    /// <summary>The snapshot final file, the group metadata store, the group snapshot store and the stamp store create owner-only files.</summary>
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
        await new ActivatedTopologyStampStore(root).PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 }, cancellationToken);

        _ = await Assert.That(File.GetUnixFileMode(snapshotPath)).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(GroupStoragePaths.GetMetadataPath(root, GroupId))).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(GroupStoragePaths.GetSnapshotSlotPath(root, GroupId, 0))).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(GroupStoragePaths.GetSnapshotSlotPath(root, GroupId, 1))).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(Path.Join(root, ActivatedTopologyStampStore.StampFileName))).IsEqualTo(FileEx.PrivateFileMode);
    }

    /// <summary>Tightening restricts the node's own loose entries, counts them, and a second run changes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TightenExistingRestrictsOwnEntries(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-tighten");
        var dataDir = Path.Join(root, "data");
        var groupDir = GroupStoragePaths.GetGroupDirectory(dataDir, GroupId);
        var replicationDir = GroupStoragePaths.GetRoot(dataDir);
        _ = Directory.CreateDirectory(groupDir);
        var segment = Path.Join(dataDir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        var snapshotTemp = Path.Join(dataDir, $"{FilePrefixes.Snapshot}000001{FileExtensions.Temp}");
        var groupFile = GroupStoragePaths.GetLogSlotPath(dataDir, GroupId, 0);
        var alreadyPrivate = Path.Join(dataDir, DataDirectoryLock.LockFileName);
        await File.WriteAllBytesAsync(segment, [1], cancellationToken);
        await File.WriteAllBytesAsync(snapshotTemp, [1], cancellationToken);
        await File.WriteAllBytesAsync(groupFile, [1], cancellationToken);
        await File.WriteAllBytesAsync(alreadyPrivate, [1], cancellationToken);
        File.SetUnixFileMode(segment, LooseFile);
        File.SetUnixFileMode(snapshotTemp, LooseFile);
        File.SetUnixFileMode(groupFile, LooseFile);
        File.SetUnixFileMode(alreadyPrivate, FileEx.PrivateFileMode);
        File.SetUnixFileMode(groupDir, LooseDirectory);
        File.SetUnixFileMode(replicationDir, LooseDirectory);
        File.SetUnixFileMode(dataDir, LooseDirectory);

        var outcome = DataDirectoryPermissions.TightenExisting(dataDir);

        _ = await Assert.That(outcome).IsEqualTo(new DataDirectoryPermissions.Outcome(6, false));
        _ = await Assert.That(File.GetUnixFileMode(segment)).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(snapshotTemp)).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(groupFile)).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(groupDir)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
        _ = await Assert.That(File.GetUnixFileMode(replicationDir)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
        _ = await Assert.That(File.GetUnixFileMode(dataDir)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
        _ = await Assert.That(DataDirectoryPermissions.TightenExisting(dataDir)).IsEqualTo(default(DataDirectoryPermissions.Outcome));
    }

    /// <summary>Tightening never opens, lists or changes entries the node did not create, and a symbolic link under a node name is not followed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TightenExistingLeavesForeignEntriesAlone(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-tighten-foreign");
        var dataDir = Path.Join(root, "data");
        var foreignFile = Path.Join(dataDir, "notes.txt");
        var foreignDir = Path.Join(dataDir, "operator");
        var foreignNested = Path.Join(foreignDir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        var unlistable = Path.Join(dataDir, "lost+found");
        var outside = Path.Join(root, "outside.bin");
        var link = Path.Join(dataDir, $"{FilePrefixes.Snapshot}000002{FileExtensions.Snapshot}");
        _ = Directory.CreateDirectory(foreignDir);
        _ = Directory.CreateDirectory(unlistable);
        await File.WriteAllBytesAsync(foreignFile, [1], cancellationToken);
        await File.WriteAllBytesAsync(foreignNested, [1], cancellationToken);
        await File.WriteAllBytesAsync(outside, [1], cancellationToken);
        File.SetUnixFileMode(foreignFile, LooseFile);
        File.SetUnixFileMode(foreignNested, LooseFile);
        File.SetUnixFileMode(outside, LooseFile);
        File.SetUnixFileMode(foreignDir, LooseDirectory);
        File.SetUnixFileMode(dataDir, DirectoryEx.PrivateDirectoryMode);
        _ = File.CreateSymbolicLink(link, outside);
        File.SetUnixFileMode(unlistable, UnixFileMode.None);
        try
        {
            var outcome = DataDirectoryPermissions.TightenExisting(dataDir);

            _ = await Assert.That(outcome).IsEqualTo(default(DataDirectoryPermissions.Outcome));
            _ = await Assert.That(File.GetUnixFileMode(foreignFile)).IsEqualTo(LooseFile);
            _ = await Assert.That(File.GetUnixFileMode(foreignNested)).IsEqualTo(LooseFile);
            _ = await Assert.That(File.GetUnixFileMode(foreignDir)).IsEqualTo(LooseDirectory);
            _ = await Assert.That(File.GetUnixFileMode(outside)).IsEqualTo(LooseFile);
            _ = await Assert.That(File.GetUnixFileMode(unlistable)).IsEqualTo(UnixFileMode.None);
        }
        finally
        {
            File.SetUnixFileMode(unlistable, DirectoryEx.PrivateDirectoryMode);
        }
    }

    /// <summary>Windows has no Unix modes, so tightening changes nothing and reports zero.</summary>
    [Test]
    public async Task TightenExistingIsNoOpOnWindows()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-tighten-windows");

        _ = await Assert.That(DataDirectoryPermissions.TightenExisting(root)).IsEqualTo(default(DataDirectoryPermissions.Outcome));
    }
}
