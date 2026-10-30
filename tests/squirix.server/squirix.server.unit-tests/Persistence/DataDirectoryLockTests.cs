using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Unit tests for <see cref="DataDirectoryLock" />.</summary>
[Immutable]
public sealed class DataDirectoryLockTests : ServerUnitTestBase
{
    /// <summary>Acquiring a missing directory creates it together with the lock file.</summary>
    [Test]
    public async Task AcquireCreatesMissingDirectory()
    {
        using var root = new TempDirectory("squirix-data-dir-lock-create");
        var dataDir = Path.Join(root, "nested", "data");

        using var held = DataDirectoryLock.Acquire(dataDir);

        _ = await Assert.That(File.Exists(Path.Join(dataDir, DataDirectoryLock.LockFileName))).IsTrue();
    }

    /// <summary>The operating system enforces the lock on the supported test hosts.</summary>
    [Test]
    public async Task AcquireReportsEnforcedLock()
    {
        using var root = new TempDirectory("squirix-data-dir-lock-enforced");

        using var held = DataDirectoryLock.Acquire(root);

        _ = await Assert.That(held.IsEnforced).IsTrue();
    }

    /// <summary>A second acquire on a held directory fails and names the directory.</summary>
    [Test]
    public async Task SecondAcquireFailsWhileHeld()
    {
        using var root = new TempDirectory("squirix-data-dir-lock-held");
        using var first = DataDirectoryLock.Acquire(root);

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(root.ToString(), static dir => DataDirectoryLock.Acquire(dir).Dispose());

        _ = await Assert.That(ex.Message).Contains("in use by another node");
        _ = await Assert.That(ex.Message).Contains(new DirectoryInfo(root).Name);
    }

    /// <summary>Disposing the holder releases the directory, and the lock file stays in place.</summary>
    [Test]
    public async Task AcquireSucceedsAfterDispose()
    {
        using var root = new TempDirectory("squirix-data-dir-lock-release");
        DataDirectoryLock.Acquire(root).Dispose();

        using var second = DataDirectoryLock.Acquire(root);

        _ = await Assert.That(File.Exists(Path.Join(root, DataDirectoryLock.LockFileName))).IsTrue();
    }

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
}
