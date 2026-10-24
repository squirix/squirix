using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Storage;

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
}
