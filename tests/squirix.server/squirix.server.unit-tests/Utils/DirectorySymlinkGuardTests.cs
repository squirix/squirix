using System;
using System.IO;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers symlink/junction guards used by directory creation.</summary>
public sealed class DirectorySymlinkGuardTests : ServerUnitTestBase
{
    /// <summary>When forbidSymlinks is false, regular-directory checks are skipped.</summary>
    [Fact]
    public void EnsureRegularDirectorySkipsWhenNotForbidden()
    {
        using var root = new TempDirectory("squirix-symlink-guard-skip");
        var path = root.Path;
        var exception = Record.Exception(() => DirectorySymlinkGuard.EnsureRegularDirectory(path, true, false));
        Assert.Null(exception);
    }

    /// <summary>Ordinary directories pass the regular-directory check.</summary>
    [Fact]
    public void EnsureRegularDirectoryAcceptsOrdinaryDirectory()
    {
        using var root = new TempDirectory("squirix-symlink-guard-ok");
        var path = root.Path;
        var exception = Record.Exception(() => DirectorySymlinkGuard.EnsureRegularDirectory(path, false, true));
        Assert.Null(exception);
    }

    /// <summary>IsSymlink returns false for ordinary directories.</summary>
    [Fact]
    public void IsSymlinkReturnsFalseForOrdinaryDirectory()
    {
        using var root = new TempDirectory("squirix-symlink-guard-is");
        Assert.False(DirectorySymlinkGuard.IsSymlink(new DirectoryInfo(root.Path)));
    }

    /// <summary>EnsureNoSymlinksInChain accepts paths with no existing intermediate links.</summary>
    [Fact]
    public void EnsureSymlinksChainMissingIntermediateSegments()
    {
        using var root = new TempDirectory("squirix-symlink-guard-chain");
        var basePath = root.Path;
        var target = Path.Join(basePath, "missing", "child");
        var exception = Record.Exception(() => DirectorySymlinkGuard.EnsureNoSymlinksInChain(target, basePath));
        Assert.Null(exception);
    }

    /// <summary>EnsureNoSymlinksInChain is a no-op when relative remainder is empty.</summary>
    [Fact]
    public void EnsureNoSymlinksInChainReturnsWhenRelativeIsEmpty()
    {
        var root = Path.GetPathRoot(Path.GetTempPath())!;
        var exception = Record.Exception(() => DirectorySymlinkGuard.EnsureNoSymlinksInChain(root, root));
        Assert.Null(exception);
    }

    /// <summary>Created and existing symlink targets are rejected when forbidSymlinks is true.</summary>
    [Fact]
    public void EnsureRegularDirectoryRejectsSymlinkTarget()
    {
        using var root = new TempDirectory("squirix-symlink-guard-reject");
        var real = Path.Join(root.Path, "real");
        Directory.CreateDirectory(real);
        var link = Path.Join(root.Path, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        var createdEx = Assert.Throws<IOException>(() => DirectorySymlinkGuard.EnsureRegularDirectory(link, true, true));
        Assert.Contains("Created directory resolved to a symlink", createdEx.Message, StringComparison.OrdinalIgnoreCase);

        var existingEx = Assert.Throws<IOException>(() => DirectorySymlinkGuard.EnsureRegularDirectory(link, false, true));
        Assert.Contains("Target directory is a symlink", existingEx.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(DirectorySymlinkGuard.IsSymlink(new DirectoryInfo(link)));
    }

    /// <summary>EnsureNoSymlinksInChain rejects an intermediate symlink under the base.</summary>
    [Fact]
    public void EnsureNoSymlinksInChainRejectsIntermediateSymlink()
    {
        using var root = new TempDirectory("squirix-symlink-guard-chain-reject");
        var basePath = root.Path;
        var real = Path.Join(basePath, "real");
        Directory.CreateDirectory(real);
        var link = Path.Join(basePath, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        var target = Path.Join(link, "child");
        _ = Assert.Throws<IOException>(() => DirectorySymlinkGuard.EnsureNoSymlinksInChain(target, basePath));
    }

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
