using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Tests startup permission tightening of the node data directory.</summary>
[Immutable]
public sealed class DataDirectoryPermissionsTests : ServerUnitTestBase
{
    private const string GroupId = "permissions";

    private const UnixFileMode LooseDirectory = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                                                UnixFileMode.GroupRead | UnixFileMode.GroupExecute | UnixFileMode.OtherRead | UnixFileMode.OtherExecute;

    private const UnixFileMode LooseFile = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead;

    /// <summary>Windows has no Unix modes, so tightening changes nothing and reports zero.</summary>
    [Test]
    public async Task TightenExistingIsNoOpOnWindows()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-tighten-windows");

        _ = await Assert.That(DataDirectoryPermissions.TightenExisting(root)).IsEqualTo(default);
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

            _ = await Assert.That(outcome).IsEqualTo(default);
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
        _ = await Assert.That(DataDirectoryPermissions.TightenExisting(dataDir)).IsEqualTo(default);
    }
}
