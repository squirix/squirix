using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Exceptions;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="DirectoryEx" /> path validation and creation behavior.</summary>
[Immutable]
public sealed class DirectoryExTests : ServerUnitTestBase
{
    /// <summary>On non-Apple hosts, <see cref="MacOsCompatibilitySymlink.TryFollow(DirectoryInfo, out string)" /> returns false for a normal directory.</summary>
    [Test]
    public async Task CompatSymlinkTryReturnsFalseOffApple()
    {
        if (OperatingSystem.IsMacOS() || OperatingSystem.IsMacCatalyst())
            return;

        using var dir = new TempDirectory("squirix-directoryex-macos-follow");
        var info = new DirectoryInfo(dir);
        _ = await Assert.That(MacOsCompatibilitySymlink.TryFollow(info, out var resolved)).IsFalse();
        _ = await Assert.That(resolved).IsEqualTo(string.Empty);
    }

    /// <summary>On macOS, <c language="csharp">/tmp</c> may be used as a base despite being a Darwin compatibility symlink.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CreateDirAcceptsMacOsTmpBase(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsMacOS() && !OperatingSystem.IsMacCatalyst())
            return;

        var name = NodeInvariantIndexStrings.FormatPrefixedGuidN("squirix-directoryex-macos-tmp-");
        string? created = null;
        try
        {
            created = await DirectoryEx.CreateDirectoryAsync(name, "/tmp", cancellationToken: cancellationToken);
            _ = await Assert.That(Directory.Exists(created)).IsTrue();
            _ = await Assert.That(created.StartsWith("/private/tmp", StringComparison.Ordinal) || created.StartsWith("/tmp", StringComparison.Ordinal)).IsTrue();
        }
        finally
        {
            if (created != null && Directory.Exists(created))
                Directory.Delete(created, true);
        }
    }

    /// <summary>Throws when a regular file already exists at the target path.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CreateDirRejectsFileAtTargetPath(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-directoryex-file");
        var target = Path.Join(dir, "blocked");
        await File.WriteAllTextAsync(target, "x", cancellationToken);

        var ex = NodeExceptionAssert.For<IOException>().Throws(dir, static basePath => DirectoryEx.CreateDirectory("blocked", basePath));
        _ = await Assert.That(ex.Message).Contains("file already exists", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>When <c language="csharp">forbidSymlinks</c> is true, rejects a directory symlink or junction in the path chain.</summary>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public void CreateDirRejectsSymlinkInChain()
    {
        using var dir = new TempDirectory("squirix-directoryex-symlink");
        var real = Path.Join(dir, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(dir, "link");
        if (!TryCreateDirectoryLink(link, real))
            throw new SkipTestException("Directory symlink/junction creation is not available in this environment.");

        _ = NodeExceptionAssert.For<IOException>().Throws(dir, static basePath => DirectoryEx.CreateDirectory(Path.Join("link", "child"), basePath));
    }

    /// <summary>On Windows, reserved device names such as CON are rejected.</summary>
    [Test]
    public void CreateDirRejectsWindowsReservedName()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var dir = new TempDirectory("squirix-directoryex-reserved");
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(dir, static basePath => DirectoryEx.CreateDirectory("CON", basePath));
    }

    /// <summary>Creates under a temp path and returns an absolute existing directory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CreateDirReturnsAbsoluteExistingPath(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-directoryex-create");
        var created = await DirectoryEx.CreateDirectoryAsync("child", dir, cancellationToken: cancellationToken);

        _ = await Assert.That(Path.IsPathRooted(created)).IsTrue();
        _ = await Assert.That(Directory.Exists(created)).IsTrue();
        _ = await Assert.That(created).StartsWith(dir, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects a target that escapes the base directory.</summary>
    [Test]
    public async Task CreateDirectoryRejectsBaseEscape()
    {
        using var dir = new TempDirectory("squirix-directoryex-escape");
        var parent = Directory.GetParent(dir);
        _ = await Assert.That(parent).IsNotNull();
        var outside = Path.Join(parent.FullName, NodeInvariantIndexStrings.FormatPrefixedGuidN("squirix-directoryex-outside-"));
        _ = NodeExceptionAssert.For<UnauthorizedAccessException>().Throws(outside, dir, static (path, basePath) => DirectoryEx.CreateDirectory(path, basePath));
    }

    /// <summary>Rejects empty and whitespace paths.</summary>
    /// <param name="path">Invalid path input.</param>
    [Test]
    [Arguments("")]
    [Arguments("   ")]
    public void CreateDirectoryRejectsEmptyOrWhitespace(string path)
    {
        using var dir = new TempDirectory("squirix-directoryex-empty");
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(path, dir, static (value, basePath) => DirectoryEx.CreateDirectory(value, basePath));
    }

    /// <summary>Rejects wildcard characters in the path.</summary>
    /// <param name="path">Path containing wildcards.</param>
    [Test]
    [Arguments("a*b")]
    [Arguments("a?b")]
    public void CreateDirectoryRejectsWildcards(string path)
    {
        using var dir = new TempDirectory("squirix-directoryex-wildcards");
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(path, dir, static (value, basePath) => DirectoryEx.CreateDirectory(value, basePath));
    }

    /// <summary>When <c language="csharp">forbidSymlinks</c> is true, async create also rejects a symlink or junction in the path chain.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public void EnsureEmptyRejectsSymlinkInChain(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-directoryex-symlink-async");
        var ct = cancellationToken;
        var real = Path.Join(dir, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(dir, "link");
        if (!TryCreateDirectoryLink(link, real))
            throw new SkipTestException("Directory symlink/junction creation is not available in this environment.");

        // CreateDirectoryAsync validates the path synchronously before returning a Task.
        _ = NodeExceptionAssert.For<IOException>().Throws(
            dir,
            ct,
            static (basePath, token) => _ = DirectoryEx.CreateDirectoryAsync(Path.Join("link", "child"), basePath, cancellationToken: token));
    }

    /// <summary>When <c language="csharp">ensureEmpty</c> is true, existing child files are removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnsureEmptyRemovesChildrenAsync(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-directoryex-empty-children");
        var ct = cancellationToken;
        var child = await DirectoryEx.CreateDirectoryAsync("nest", dir, cancellationToken: ct);
        var leftover = Path.Join(child, "leftover.txt");
        await File.WriteAllTextAsync(leftover, "keep-me-not", ct);
        _ = await Assert.That(File.Exists(leftover)).IsTrue();

        var ready = await DirectoryEx.CreateDirectoryAsync("nest", dir, true, cancellationToken: ct);

        _ = await Assert.That(ready).IsEqualTo(child);
        _ = await Assert.That(File.Exists(leftover)).IsFalse();
        _ = await Assert.That(Directory.Exists(ready)).IsTrue();
    }

    private static bool TryCreateDirectoryLink(string linkPath, string targetPath)
    {
        try
        {
            _ = Directory.CreateSymbolicLink(linkPath, targetPath);
            return Directory.Exists(linkPath);
        }
        catch (IOException ex)
        {
            TestLog.Suppressed($"Symlink creation failed for '{linkPath}' -> '{targetPath}'; falling back to junction.", ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            TestLog.Suppressed($"Symlink creation denied for '{linkPath}' -> '{targetPath}'; falling back to junction.", ex);
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }

        return OperatingSystem.IsWindows() && TryCreateWindowsJunction(linkPath, targetPath);
    }

    private static bool TryCreateWindowsJunction(string linkPath, string targetPath)
    {
        try
        {
            var processStartInfo = new ProcessStartInfo
            {
                FileName = "cmd.exe",
                Arguments = NodeInvariantIndexStrings.FormatMklinkJunctionArguments(linkPath, targetPath),
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            using var process = Process.Start(processStartInfo);
            if (process == null)
                return false;

            if (process.WaitForExit(10_000))
                return process.ExitCode == 0 && Directory.Exists(linkPath);
            try
            {
                process.Kill(true);
            }
            catch (InvalidOperationException ex)
            {
                TestLog.Suppressed("Junction process exited between wait timeout and Kill; treating as not created.", ex);
            }

            return false;
        }
        catch (Exception ex) when (ex is IOException or Win32Exception)
        {
            return false;
        }
    }
}
