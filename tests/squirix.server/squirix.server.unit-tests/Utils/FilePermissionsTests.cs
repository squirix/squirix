using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Tests owner-only permissions of generic file and directory helpers.</summary>
[Immutable]
public sealed class FilePermissionsTests : ServerUnitTestBase
{
    private const UnixFileMode LooseFile = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead;

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
}
