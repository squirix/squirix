using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
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

    /// <summary>Tightening restricts loose files and directories, counts them, and leaves private entries and symbolic links alone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TightenExistingRestrictsLooseEntries(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-tighten");
        var dataDir = Path.Join(root, "data");
        var groupDir = Path.Join(dataDir, "group-1");
        var outside = Path.Join(root, "outside.bin");
        _ = Directory.CreateDirectory(groupDir);
        var loose = Path.Join(groupDir, "group.log.1");
        var alreadyPrivate = Path.Join(dataDir, "private.bin");
        var link = Path.Join(dataDir, "link.bin");
        await File.WriteAllBytesAsync(loose, [1], cancellationToken);
        await File.WriteAllBytesAsync(alreadyPrivate, [2], cancellationToken);
        await File.WriteAllBytesAsync(outside, [3], cancellationToken);
        File.SetUnixFileMode(loose, LooseFile);
        File.SetUnixFileMode(alreadyPrivate, FileEx.PrivateFileMode);
        File.SetUnixFileMode(outside, LooseFile);
        File.SetUnixFileMode(groupDir, LooseDirectory);
        File.SetUnixFileMode(dataDir, LooseDirectory);
        _ = File.CreateSymbolicLink(link, outside);

        var tightened = DataDirectoryPermissions.TightenExisting(dataDir);

        _ = await Assert.That(tightened).IsEqualTo(3);
        _ = await Assert.That(File.GetUnixFileMode(loose)).IsEqualTo(FileEx.PrivateFileMode);
        _ = await Assert.That(File.GetUnixFileMode(groupDir)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
        _ = await Assert.That(File.GetUnixFileMode(dataDir)).IsEqualTo(DirectoryEx.PrivateDirectoryMode);
        _ = await Assert.That(File.GetUnixFileMode(outside)).IsEqualTo(LooseFile);
        _ = await Assert.That(DataDirectoryPermissions.TightenExisting(dataDir)).IsEqualTo(0);
    }

    /// <summary>Windows has no Unix modes, so tightening changes nothing and reports zero.</summary>
    [Test]
    public async Task TightenExistingIsNoOpOnWindows()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-tighten-windows");

        _ = await Assert.That(DataDirectoryPermissions.TightenExisting(root)).IsEqualTo(0);
    }
}
