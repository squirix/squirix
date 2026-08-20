using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers symlink/junction guards used by directory creation.</summary>
[Immutable]
public sealed class DirectorySymlinkGuardTests : IsolatedStorageTestBase
{
    /// <summary>EnsureNoSymlinksInChain rejects an intermediate symlink under the base.</summary>
    [Fact]
    public void EnsureNoSymlinksInChainRejectsIntermediateSymlink()
    {
        var basePath = Dir.Path;
        var real = Path.Join(basePath, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(basePath, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        var target = Path.Join(link, "child");
        _ = NodeExceptionAssert.For<IOException>().Throws(target, basePath, static (path, rootPath) => DirectorySymlinkGuard.EnsureNoSymlinksInChain(path, rootPath));
    }

    /// <summary>EnsureNoSymlinksInChain is a no-op when relative remainder is empty.</summary>
    [Fact]
    public void EnsureNoSymlinksInChainReturnsWhenRelativeIsEmpty()
    {
        var dir = Path.GetPathRoot(Path.GetTempPath())!;
        DirectorySymlinkGuard.EnsureNoSymlinksInChain(dir, dir);
        Assert.True(Path.IsPathRooted(dir));
    }

    /// <summary>Ordinary directories pass the regular-directory check.</summary>
    [Fact]
    public void EnsureRegularDirectoryAcceptsOrdinaryDirectory()
    {
        var path = Dir.Path;
        DirectorySymlinkGuard.EnsureRegularDirectory(path, false, true);
        Assert.True(Directory.Exists(path));
    }

    /// <summary>Created and existing symlink targets are rejected when forbidSymlinks is true.</summary>
    [Fact]
    public void EnsureRegularDirectoryRejectsSymlinkTarget()
    {
        var real = Path.Join(Dir.Path, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(Dir.Path, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        var createdEx = NodeExceptionAssert.For<IOException>().Throws(link, static path => DirectorySymlinkGuard.EnsureRegularDirectory(path, true, true));
        Assert.Contains("Created directory resolved to a symlink", createdEx.Message, StringComparison.OrdinalIgnoreCase);

        var existingEx = NodeExceptionAssert.For<IOException>().Throws(link, static path => DirectorySymlinkGuard.EnsureRegularDirectory(path, false, true));
        Assert.Contains("Target directory is a symlink", existingEx.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(DirectorySymlinkGuard.IsSymlink(new DirectoryInfo(link)));
    }

    /// <summary>When forbidSymlinks is false, regular-directory checks are skipped.</summary>
    [Fact]
    public void EnsureRegularDirectorySkipsWhenNotForbidden()
    {
        var path = Dir.Path;
        DirectorySymlinkGuard.EnsureRegularDirectory(path, true, false);
        Assert.True(Directory.Exists(path));
    }

    /// <summary>EnsureNoSymlinksInChain accepts paths with no existing intermediate links.</summary>
    [Fact]
    public void EnsureSymlinksChainMissingIntermediateSegments()
    {
        var basePath = Dir.Path;
        var target = Path.Join(basePath, "missing", "child");
        DirectorySymlinkGuard.EnsureNoSymlinksInChain(target, basePath);
        Assert.False(Directory.Exists(target));
    }

    /// <summary>IsSymlink returns false for ordinary directories.</summary>
    [Fact]
    public void IsSymlinkReturnsFalseForOrdinaryDirectory() => Assert.False(DirectorySymlinkGuard.IsSymlink(new DirectoryInfo(Dir.Path)));

    private static bool TryCreateDirectoryLink(string linkPath, string targetPath)
    {
        try
        {
            _ = Directory.CreateSymbolicLink(linkPath, targetPath);
            return Directory.Exists(linkPath);
        }
        catch (IOException)
        {
            // Symlink privilege may be missing; treat as unavailable.
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            // Symlink privilege may be missing; treat as unavailable.
            return false;
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }
    }
}
