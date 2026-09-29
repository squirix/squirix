using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Exceptions;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers symlink/junction guards used by directory creation.</summary>
[Immutable]
public sealed class DirectorySymlinkGuardTests : IsolatedStorageTestBase
{
    /// <summary>Ordinary directories pass the regular-directory check.</summary>
    [Test]
    public async Task EnsureRegularAcceptsOrdinaryDir()
    {
        string path = Dir;
        DirectorySymlinkGuard.EnsureRegularDirectory(path, false, true);
        _ = await Assert.That(Directory.Exists(path)).IsTrue();
    }

    /// <summary>Created and existing symlink targets are rejected when forbidSymlinks is true.</summary>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public async Task EnsureRegularRejectsSymlinkTarget()
    {
        var real = Path.Join(Dir, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(Dir, "link");
        if (!TryCreateDirectoryLink(link, real))
            throw new SkipTestException("Directory symlink/junction creation is not available in this environment.");

        var createdEx = NodeExceptionAssert.For<IOException>().Throws(link, static path => DirectorySymlinkGuard.EnsureRegularDirectory(path, true, true));
        _ = await Assert.That(createdEx.Message).Contains("Created directory resolved to a symlink", StringComparison.OrdinalIgnoreCase);

        var existingEx = NodeExceptionAssert.For<IOException>().Throws(link, static path => DirectorySymlinkGuard.EnsureRegularDirectory(path, false, true));
        _ = await Assert.That(existingEx.Message).Contains("Target directory is a symlink", StringComparison.OrdinalIgnoreCase);
        _ = await Assert.That(DirectorySymlinkGuard.IsSymlink(new DirectoryInfo(link))).IsTrue();
    }

    /// <summary>When forbidSymlinks is false, regular-directory checks are skipped.</summary>
    [Test]
    public async Task EnsureRegularSkipsWhenAllowed()
    {
        string path = Dir;
        DirectorySymlinkGuard.EnsureRegularDirectory(path, true, false);
        _ = await Assert.That(Directory.Exists(path)).IsTrue();
    }

    /// <summary>EnsureNoSymlinksInChain accepts paths with no existing intermediate links.</summary>
    [Test]
    public async Task GuardFlagsMissingChainSegments()
    {
        string basePath = Dir;
        var target = Path.Join(basePath, "missing", "child");
        DirectorySymlinkGuard.EnsureNoSymlinksInChain(target, basePath);
        _ = await Assert.That(Directory.Exists(target)).IsFalse();
    }

    /// <summary>EnsureNoSymlinksInChain is a no-op when relative remainder is empty.</summary>
    [Test]
    public async Task GuardPassesWhenRelativeIsEmpty()
    {
        var dir = Path.GetPathRoot(Path.GetTempPath())!;
        DirectorySymlinkGuard.EnsureNoSymlinksInChain(dir, dir);
        _ = await Assert.That(Path.IsPathRooted(dir)).IsTrue();
    }

    /// <summary>EnsureNoSymlinksInChain rejects an intermediate symlink under the base.</summary>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public void GuardRejectsIntermediateSymlink()
    {
        string basePath = Dir;
        var real = Path.Join(basePath, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(basePath, "link");
        if (!TryCreateDirectoryLink(link, real))
            throw new SkipTestException("Directory symlink/junction creation is not available in this environment.");

        var target = Path.Join(link, "child");
        _ = NodeExceptionAssert.For<IOException>().Throws(target, basePath, static (path, rootPath) => DirectorySymlinkGuard.EnsureNoSymlinksInChain(path, rootPath));
    }

    /// <summary>IsSymlink returns false for ordinary directories.</summary>
    [Test]
    public async Task IsSymlinkFalseForOrdinaryDir() => _ = await Assert.That(DirectorySymlinkGuard.IsSymlink(new DirectoryInfo(Dir))).IsFalse();

    /// <summary>An unreadable link target with readable attributes lets the attributes decide.</summary>
    [Test]
    public async Task LinkTargetIoFallsBackToAttributes()
    {
        var info = new DirectoryInfo(Dir);
        _ = await Assert.That(DirectorySymlinkGuard.IsSymlink(info, static _ => throw new IOException("probe"), static _ => FileAttributes.Directory)).IsFalse();
        _ = await Assert.That(DirectorySymlinkGuard.IsSymlink(info, static _ => throw new IOException("probe"), static _ => FileAttributes.Directory | FileAttributes.ReparsePoint)).IsTrue();
    }

    /// <summary>An unsupported link target with readable attributes lets the attributes decide.</summary>
    [Test]
    public async Task LinkTargetUnsupportedUsesAttributes()
    {
        var info = new DirectoryInfo(Dir);
        _ = await Assert.That(DirectorySymlinkGuard.IsSymlink(info, static _ => throw new NotSupportedException("probe"), static _ => FileAttributes.Directory)).IsFalse();
        _ = await Assert.That(DirectorySymlinkGuard.IsSymlink(info, static _ => throw new NotSupportedException("probe"), static _ => FileAttributes.ReparsePoint)).IsTrue();
    }

    /// <summary>Unreadable attributes reported as IOException fail closed.</summary>
    [Test]
    public async Task AttributesIoFailsClosed()
    {
        var info = new DirectoryInfo(Dir);
        var ex = NodeExceptionAssert.For<IOException>().Throws(info, static entry => DirectorySymlinkGuard.IsSymlink(entry, static _ => null, static _ => throw new IOException("probe")));
        _ = await Assert.That(ex.Message).Contains(info.FullName, StringComparison.Ordinal);
    }

    /// <summary>Unreadable attributes reported as UnauthorizedAccessException fail closed.</summary>
    [Test]
    public async Task AttributesUnauthorizedFailsClosed()
    {
        var info = new DirectoryInfo(Dir);
        var ex = NodeExceptionAssert.For<IOException>().Throws(
            info,
            static entry => DirectorySymlinkGuard.IsSymlink(entry, static _ => throw new IOException("probe"), static _ => throw new UnauthorizedAccessException("probe")));
        _ = await Assert.That(ex.Message).Contains(info.FullName, StringComparison.Ordinal);
    }

    /// <summary>The chain walk rejects when the link status of an existing segment cannot be determined.</summary>
    [Test]
    public void GuardRejectsUndeterminableSegment()
    {
        string basePath = Dir;
        var target = Path.Join(basePath, "child", "leaf");
        _ = Directory.CreateDirectory(Path.Join(basePath, "child"));
        _ = NodeExceptionAssert.For<IOException>().Throws(
            target,
            basePath,
            static (path, rootPath) => DirectorySymlinkGuard.EnsureNoSymlinksInChain(path, rootPath, static _ => throw new IOException("undeterminable")));
    }

    private static bool TryCreateDirectoryLink(string linkPath, string targetPath)
    {
        try
        {
            _ = Directory.CreateSymbolicLink(linkPath, targetPath);
            return Directory.Exists(linkPath);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            // Symlink privilege may be missing; treat as unavailable.
            return false;
        }
    }
}
