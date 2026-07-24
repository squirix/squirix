using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="DirectoryEx" /> path validation and creation behavior.</summary>
public sealed class DirectoryExTests
{
    /// <summary>Creates under a temp path and returns an absolute existing directory.</summary>
    [Fact]
    public void CreateDirectoryReturnsAbsoluteExistingDirectory()
    {
        using var root = new TempDirectory("squirix-directoryex-create");
        var created = DirectoryEx.CreateDirectory("child", root.Path);

        Assert.True(Path.IsPathRooted(created));
        Assert.True(Directory.Exists(created));
        Assert.StartsWith(root.Path, created, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects empty and whitespace paths.</summary>
    /// <param name="path">Invalid path input.</param>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void CreateDirectoryRejectsEmptyOrWhitespace(string path)
    {
        using var root = new TempDirectory("squirix-directoryex-empty");
        _ = Assert.Throws<ArgumentException>(() => DirectoryEx.CreateDirectory(path, root.Path));
    }

    /// <summary>Rejects wildcard characters in the path.</summary>
    /// <param name="path">Path containing wildcards.</param>
    [Theory]
    [InlineData("a*b")]
    [InlineData("a?b")]
    public void CreateDirectoryRejectsWildcards(string path)
    {
        using var root = new TempDirectory("squirix-directoryex-wildcards");
        _ = Assert.Throws<ArgumentException>(() => DirectoryEx.CreateDirectory(path, root.Path));
    }

    /// <summary>Rejects a target that escapes the base directory.</summary>
    [Fact]
    public void CreateDirectoryRejectsBaseEscape()
    {
        using var root = new TempDirectory("squirix-directoryex-escape");
        var parent = Directory.GetParent(root.Path);
        Assert.NotNull(parent);
        var outside = Path.Join(parent.FullName, "squirix-directoryex-outside-" + Guid.NewGuid().ToString("N"));
        _ = Assert.Throws<UnauthorizedAccessException>(() => DirectoryEx.CreateDirectory(outside, root.Path));
    }

    /// <summary>Throws when a regular file already exists at the target path.</summary>
    [Fact]
    public void CreateDirectoryRejectsExistingFileAtTarget()
    {
        using var root = new TempDirectory("squirix-directoryex-file");
        var target = Path.Join(root.Path, "blocked");
        File.WriteAllText(target, "x");

        var ex = Assert.Throws<IOException>(() => DirectoryEx.CreateDirectory("blocked", root.Path));
        Assert.Contains(target, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>When <c>ensureEmpty</c> is true, existing child files are removed.</summary>
    [Fact]
    public async Task CreateDirectoryAsyncEnsureEmptyRemovesChildren()
    {
        using var root = new TempDirectory("squirix-directoryex-empty-children");
        var ct = TestContext.Current.CancellationToken;
        var child = await DirectoryEx.CreateDirectoryAsync("nest", root.Path, cancellationToken: ct).ConfigureAwait(true);
        var leftover = Path.Join(child, "leftover.txt");
        await File.WriteAllTextAsync(leftover, "keep-me-not", ct).ConfigureAwait(true);
        Assert.True(File.Exists(leftover));

        var ready = await DirectoryEx.CreateDirectoryAsync("nest", root.Path, true, cancellationToken: ct).ConfigureAwait(true);

        Assert.Equal(child, ready);
        Assert.False(File.Exists(leftover));
        Assert.True(Directory.Exists(ready));
    }

    /// <summary>When <c>forbidSymlinks</c> is true, rejects a directory symlink or junction in the path chain.</summary>
    [Fact]
    public void CreateDirectoryRejectsSymlinkOrJunctionInChain()
    {
        using var root = new TempDirectory("squirix-directoryex-symlink");
        var real = Path.Join(root.Path, "real");
        Directory.CreateDirectory(real);
        var link = Path.Join(root.Path, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        _ = Assert.Throws<IOException>(() => DirectoryEx.CreateDirectory(Path.Join("link", "child"), root.Path));
    }

    /// <summary>When <c>forbidSymlinks</c> is true, async create also rejects a symlink or junction in the path chain.</summary>
    [Fact]
    public async Task CreateDirectoryAsyncRejectsSymlinkJunctionInChain()
    {
        using var root = new TempDirectory("squirix-directoryex-symlink-async");
        var ct = TestContext.Current.CancellationToken;
        var real = Path.Join(root.Path, "real");
        Directory.CreateDirectory(real);
        var link = Path.Join(root.Path, "link");
        if (!TryCreateDirectoryLink(link, real))
            Assert.Skip("Directory symlink/junction creation is not available in this environment.");

        _ = await Assert.ThrowsAsync<IOException>(() => DirectoryEx.CreateDirectoryAsync(Path.Join("link", "child"), root.Path, cancellationToken: ct)).ConfigureAwait(true);
    }

    /// <summary>On macOS, <c>/tmp</c> may be used as a base despite being a Darwin compatibility symlink.</summary>
    [Fact]
    public void CreateDirectoryAcceptsMacOsTmpCompatibilityBase()
    {
        if (!OperatingSystem.IsMacOS() && !OperatingSystem.IsMacCatalyst())
            return;

        var name = "squirix-directoryex-macos-tmp-" + Guid.NewGuid().ToString("N");
        string? created = null;
        try
        {
            created = DirectoryEx.CreateDirectory(name, "/tmp");
            Assert.True(Directory.Exists(created));
            Assert.True(created.StartsWith("/private/tmp", StringComparison.Ordinal) || created.StartsWith("/tmp", StringComparison.Ordinal));
        }
        finally
        {
            if (created is not null && Directory.Exists(created))
                Directory.Delete(created, true);
        }
    }

    /// <summary>On non-Apple hosts, <see cref="MacOsCompatibilitySymlink.TryFollow(DirectoryInfo, out string)" /> returns false for a normal directory.</summary>
    [Fact]
    public void MacOsCompatibilitySymlinkTryReturnsFalseOffApple()
    {
        if (OperatingSystem.IsMacOS() || OperatingSystem.IsMacCatalyst())
            return;

        using var root = new TempDirectory("squirix-directoryex-macos-follow");
        var info = new DirectoryInfo(root.Path);
        Assert.False(MacOsCompatibilitySymlink.TryFollow(info, out var resolved));
        Assert.Equal(string.Empty, resolved);
    }

    /// <summary>On Windows, reserved device names such as CON are rejected.</summary>
    [Fact]
    public void CreateDirectoryRejectsWindowsReservedName()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-directoryex-reserved");
        _ = Assert.Throws<ArgumentException>(() => DirectoryEx.CreateDirectory("CON", root.Path));
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
            // Fall through to a Windows junction attempt when symlink privilege is missing.
        }
        catch (UnauthorizedAccessException)
        {
            // Fall through to a Windows junction attempt when symlink privilege is missing.
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
                Arguments = "/c mklink /J \"" + linkPath + "\" \"" + targetPath + "\"",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            using var process = Process.Start(processStartInfo);
            if (process is null)
                return false;

            if (process.WaitForExit(10_000))
                return process.ExitCode is 0 && Directory.Exists(linkPath);
            try
            {
                process.Kill(true);
            }
            catch (InvalidOperationException)
            {
                // Process exited between the wait timeout and Kill.
            }

            return false;
        }
        catch (IOException)
        {
            return false;
        }
        catch (Win32Exception)
        {
            return false;
        }
    }
}
