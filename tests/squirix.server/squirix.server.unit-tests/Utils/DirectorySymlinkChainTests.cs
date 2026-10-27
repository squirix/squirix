using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
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

/// <summary>Covers link detection while walking directory path chains.</summary>
[Immutable]
public sealed class DirectorySymlinkChainTests : IsolatedStorageTestBase
{
    /// <summary>Plain nested directories pass the chain walk.</summary>
    [Test]
    public void GuardAcceptsPlainNestedDirs()
    {
        string basePath = Dir;
        var target = Path.Join(basePath, "a", "b", "c");
        _ = Directory.CreateDirectory(target);
        DirectorySymlinkGuard.EnsureNoSymlinksInChain(target, basePath);
        DirectorySymlinkGuard.EnsureNoSymlinksInChain(target, null);
    }

    /// <summary>A link at the top of the walked chain is rejected.</summary>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public void GuardRejectsTopSymlink()
    {
        string basePath = Dir;
        var real = Path.Join(basePath, "real");
        _ = Directory.CreateDirectory(Path.Join(real, "x", "y"));
        var link = Path.Join(basePath, "link");
        if (!DirectorySymlinkGuardTests.TryCreateDirectoryLink(link, real))
            throw new SkipTestException("Directory symlink/junction creation is not available in this environment.");

        _ = NodeExceptionAssert.For<IOException>().Throws(
            Path.Join(link, "x", "y"),
            basePath,
            static (path, rootPath) => DirectorySymlinkGuard.EnsureNoSymlinksInChain(path, rootPath));
    }

    /// <summary>A link in the middle of the walked chain is rejected.</summary>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public void GuardRejectsMiddleSymlink()
    {
        string basePath = Dir;
        var real = Path.Join(basePath, "real");
        _ = Directory.CreateDirectory(Path.Join(real, "z"));
        var parent = Path.Join(basePath, "a", "b");
        _ = Directory.CreateDirectory(parent);
        var link = Path.Join(parent, "link");
        if (!DirectorySymlinkGuardTests.TryCreateDirectoryLink(link, real))
            throw new SkipTestException("Directory symlink/junction creation is not available in this environment.");

        _ = NodeExceptionAssert.For<IOException>().Throws(
            Path.Join(link, "z"),
            basePath,
            static (path, rootPath) => DirectorySymlinkGuard.EnsureNoSymlinksInChain(path, rootPath));
    }

    /// <summary>A link at the leaf of the walked chain is rejected.</summary>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public void GuardRejectsLeafSymlink()
    {
        string basePath = Dir;
        var real = Path.Join(basePath, "real");
        _ = Directory.CreateDirectory(real);
        var parent = Path.Join(basePath, "a", "b");
        _ = Directory.CreateDirectory(parent);
        var link = Path.Join(parent, "leaf");
        if (!DirectorySymlinkGuardTests.TryCreateDirectoryLink(link, real))
            throw new SkipTestException("Directory symlink/junction creation is not available in this environment.");

        _ = NodeExceptionAssert.For<IOException>().Throws(link, basePath, static (path, rootPath) => DirectorySymlinkGuard.EnsureNoSymlinksInChain(path, rootPath));
    }

    /// <summary>A Windows junction is rejected by the chain walk.</summary>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public async Task GuardRejectsJunction()
    {
        if (!OperatingSystem.IsWindows())
            throw new SkipTestException("Junctions exist only on Windows.");

        string basePath = Dir;
        var real = Path.Join(basePath, "real");
        _ = Directory.CreateDirectory(Path.Join(real, "child"));
        var junction = Path.Join(basePath, "junction");
        if (!await TryCreateJunctionAsync(junction, real))
            throw new SkipTestException("Junction creation is not available in this environment.");

        var ex = NodeExceptionAssert.For<IOException>().Throws(
            Path.Join(junction, "child"),
            basePath,
            static (path, rootPath) => DirectorySymlinkGuard.EnsureNoSymlinksInChain(path, rootPath));
        _ = await Assert.That(ex.Message).Contains("Symlink/junction detected", StringComparison.Ordinal);
        _ = await Assert.That(DirectorySymlinkGuard.IsSymlink(new DirectoryInfo(junction))).IsTrue();
    }

    /// <summary>The link-target probe is not consulted when the attributes show no reparse point.</summary>
    [Test]
    public async Task LinkTargetSkippedWithoutReparsePoint()
    {
        var info = new DirectoryInfo(Dir);
        var result = DirectorySymlinkGuard.IsSymlink(info, static _ => throw new InvalidOperationException("probe"), static _ => FileAttributes.Directory);
        _ = await Assert.That(result).IsFalse();
    }

    /// <summary>A readable link target proves a link when the attributes are unreadable.</summary>
    [Test]
    public async Task LinkTargetProvesLinkOnBadAttributes()
    {
        var info = new DirectoryInfo(Dir);
        var result = DirectorySymlinkGuard.IsSymlink(info, static _ => "target", static _ => throw new IOException("probe"));
        _ = await Assert.That(result).IsTrue();
    }

    private static async Task<bool> TryCreateJunctionAsync(string linkPath, string targetPath)
    {
        var startInfo = new ProcessStartInfo("cmd.exe")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        startInfo.ArgumentList.Add("/c");
        startInfo.ArgumentList.Add("mklink");
        startInfo.ArgumentList.Add("/J");
        startInfo.ArgumentList.Add(linkPath);
        startInfo.ArgumentList.Add(targetPath);
        using var process = Process.Start(startInfo);
        if (process == null)
            return false;

        var output = process.StandardOutput.ReadToEndAsync(CancellationToken.None);
        var error = process.StandardError.ReadToEndAsync(CancellationToken.None);
        await process.WaitForExitAsync(CancellationToken.None);
        _ = await output;
        _ = await error;
        return process.ExitCode == 0 && Directory.Exists(linkPath);
    }
}
