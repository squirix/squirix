using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="DirectoryEx" /> path validation and creation behavior.</summary>
[Immutable]
public sealed class DirectoryExTests : ServerUnitTestBase
{
    /// <summary>Rejects empty and whitespace paths.</summary>
    /// <param name="path">Invalid path input.</param>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public static void CreateDirectoryRejectsEmptyOrWhitespace(string path)
    {
        using var root = new TempDirectory("squirix-directoryex-empty");
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(path, root.Path, static (value, basePath) => DirectoryEx.CreateDirectory(value, basePath));
    }

    /// <summary>Rejects wildcard characters in the path.</summary>
    /// <param name="path">Path containing wildcards.</param>
    [Theory]
    [InlineData("a*b")]
    [InlineData("a?b")]
    public static void CreateDirectoryRejectsWildcards(string path)
    {
        using var root = new TempDirectory("squirix-directoryex-wildcards");
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(path, root.Path, static (value, basePath) => DirectoryEx.CreateDirectory(value, basePath));
    }

    /// <summary>On macOS, <c language="csharp">/tmp</c> may be used as a base despite being a Darwin compatibility symlink.</summary>
    [Fact]
    public void CreateDirAcceptsMacOsTmpBase()
    {
        if (!OperatingSystem.IsMacOS() && !OperatingSystem.IsMacCatalyst())
            return;

        var name = NodeInvariantIndexStrings.FormatPrefixedGuidN("squirix-directoryex-macos-tmp-");
        string? created = null;
        try
        {
            created = DirectoryEx.CreateDirectory(name, "/tmp");
            Assert.True(Directory.Exists(created));
            Assert.True(created.StartsWith("/private/tmp", StringComparison.Ordinal) || created.StartsWith("/tmp", StringComparison.Ordinal));
        }
        finally
        {
            if (created != null && Directory.Exists(created))
                Directory.Delete(created, true);
        }
    }

    /// <summary>When <c language="csharp">ensureEmpty</c> is true, existing child files are removed.</summary>
    [Fact]
    public async Task EnsureEmptyRemovesChildrenAsync()
    {
        using var root = new TempDirectory("squirix-directoryex-empty-children");
        var ct = DefaultCancellationToken;
        var child = await DirectoryEx.CreateDirectoryAsync("nest", root.Path, cancellationToken: ct);
        var leftover = Path.Join(child, "leftover.txt");
        await File.WriteAllTextAsync(leftover, "keep-me-not", ct);
        Assert.True(File.Exists(leftover));

        var ready = await DirectoryEx.CreateDirectoryAsync("nest", root.Path, true, cancellationToken: ct);

        Assert.Equal(child, ready);
        Assert.False(File.Exists(leftover));
        Assert.True(Directory.Exists(ready));
    }

    /// <summary>When <c language="csharp">forbidSymlinks</c> is true, async create also rejects a symlink or junction in the path chain.</summary>
    [Fact]
    public void EnsureEmptyRejectsSymlinkInChain()
    {
        using var root = new TempDirectory("squirix-directoryex-symlink-async");
        var ct = DefaultCancellationToken;
        var real = Path.Join(root.Path, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(root.Path, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        // CreateDirectoryAsync validates the path synchronously before returning a Task.
        _ = NodeExceptionAssert.For<IOException>().Throws(
            root.Path,
            ct,
            static (basePath, token) => _ = DirectoryEx.CreateDirectoryAsync(Path.Join("link", "child"), basePath, cancellationToken: token));
    }

    /// <summary>Rejects a target that escapes the base directory.</summary>
    [Fact]
    public void CreateDirectoryRejectsBaseEscape()
    {
        using var root = new TempDirectory("squirix-directoryex-escape");
        var parent = Directory.GetParent(root.Path);
        Assert.NotNull(parent);
        var outside = Path.Join(parent.FullName, NodeInvariantIndexStrings.FormatPrefixedGuidN("squirix-directoryex-outside-"));
        _ = NodeExceptionAssert.For<UnauthorizedAccessException>().Throws(outside, root.Path, static (path, basePath) => DirectoryEx.CreateDirectory(path, basePath));
    }

    /// <summary>Throws when a regular file already exists at the target path.</summary>
    [Fact]
    public void CreateDirRejectsFileAtTargetPath()
    {
        using var root = new TempDirectory("squirix-directoryex-file");
        var target = Path.Join(root.Path, "blocked");
        File.WriteAllText(target, "x");

        var ex = NodeExceptionAssert.For<IOException>().Throws(root.Path, static basePath => DirectoryEx.CreateDirectory("blocked", basePath));
        Assert.Contains("file already exists", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>When <c language="csharp">forbidSymlinks</c> is true, rejects a directory symlink or junction in the path chain.</summary>
    [Fact]
    public void CreateDirRejectsSymlinkInChain()
    {
        using var root = new TempDirectory("squirix-directoryex-symlink");
        var real = Path.Join(root.Path, "real");
        _ = Directory.CreateDirectory(real);
        var link = Path.Join(root.Path, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        _ = NodeExceptionAssert.For<IOException>().Throws(root.Path, static basePath => DirectoryEx.CreateDirectory(Path.Join("link", "child"), basePath));
    }

    /// <summary>On Windows, reserved device names such as CON are rejected.</summary>
    [Fact]
    public void CreateDirRejectsWindowsReservedName()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-directoryex-reserved");
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(root.Path, static basePath => DirectoryEx.CreateDirectory("CON", basePath));
    }

    /// <summary>Creates under a temp path and returns an absolute existing directory.</summary>
    [Fact]
    public void CreateDirReturnsAbsoluteExistingPath()
    {
        using var root = new TempDirectory("squirix-directoryex-create");
        var created = DirectoryEx.CreateDirectory("child", root.Path);

        Assert.True(Path.IsPathRooted(created));
        Assert.True(Directory.Exists(created));
        Assert.StartsWith(root.Path, created, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>On non-Apple hosts, <see cref="MacOsCompatibilitySymlink.TryFollow(DirectoryInfo, out string)" /> returns false for a normal directory.</summary>
    [Fact]
    public void CompatSymlinkTryReturnsFalseOffApple()
    {
        if (OperatingSystem.IsMacOS() || OperatingSystem.IsMacCatalyst())
            return;

        using var root = new TempDirectory("squirix-directoryex-macos-follow");
        var info = new DirectoryInfo(root.Path);
        Assert.False(MacOsCompatibilitySymlink.TryFollow(info, out var resolved));
        Assert.Equal(string.Empty, resolved);
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
