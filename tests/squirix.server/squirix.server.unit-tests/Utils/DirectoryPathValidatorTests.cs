using System;
using System.IO;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers directory path resolution and segment parsing.</summary>
public sealed class DirectoryPathValidatorTests : ServerUnitTestBase
{
    /// <summary>Resolves a relative path under a base directory.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathAcceptsRelativeUnderBase()
    {
        using var root = new TempDirectory("squirix-dirpath-rel");
        var full = DirectoryPathValidator.ResolveValidatedDirectoryPath("child", root.Path, true);
        Assert.True(Path.IsPathRooted(full));
        Assert.StartsWith(root.Path, full, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects empty paths.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathRejectsEmpty() =>
        _ = Assert.Throws<ArgumentException>(static () => DirectoryPathValidator.ResolveValidatedDirectoryPath("  ", null, false));

    /// <summary>Rejects targets that escape the base directory.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathRejectsBaseEscape()
    {
        using var root = new TempDirectory("squirix-dirpath-escape");
        var parent = Directory.GetParent(root.Path);
        Assert.NotNull(parent);
        var outside = Path.Join(parent.FullName, "squirix-dirpath-outside-" + Guid.NewGuid().ToString("N"));
        _ = Assert.Throws<UnauthorizedAccessException>(() => DirectoryPathValidator.ResolveValidatedDirectoryPath(outside, root.Path, true));
    }

    /// <summary>Rejects when a regular file already exists at the target.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathRejectsExistingFile()
    {
        using var root = new TempDirectory("squirix-dirpath-file");
        var target = Path.Join(root.Path, "blocked");
        File.WriteAllText(target, "x");
        _ = Assert.Throws<IOException>(() => DirectoryPathValidator.ResolveValidatedDirectoryPath("blocked", root.Path, true));
    }

    /// <summary>Creates a missing base directory when provided.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathCreatesMissingBase()
    {
        using var root = new TempDirectory("squirix-dirpath-base-create");
        var baseDir = Path.Join(root.Path, "missing-base");
        var full = DirectoryPathValidator.ResolveValidatedDirectoryPath("child", baseDir, false);
        Assert.True(Directory.Exists(baseDir));
        Assert.StartsWith(baseDir, full, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>TryReadNextSegment skips separators and returns segments.</summary>
    [Fact]
    public void TryReadNextSegmentReadsSegments()
    {
        var path = "/a//b/".AsSpan();
        Assert.True(DirectoryPathValidator.TryReadNextSegment(ref path, out var first));
        Assert.True(first.SequenceEqual("a".AsSpan()));
        Assert.True(DirectoryPathValidator.TryReadNextSegment(ref path, out var second));
        Assert.True(second.SequenceEqual("b".AsSpan()));
        Assert.False(DirectoryPathValidator.TryReadNextSegment(ref path, out _));
    }

    /// <summary>IsDirectorySeparator recognizes both separators.</summary>
    [Fact]
    public void IsDirectorySeparatorRecognizesBoth()
    {
        Assert.True(DirectoryPathValidator.IsDirectorySeparator(Path.DirectorySeparatorChar));
        Assert.True(DirectoryPathValidator.IsDirectorySeparator(Path.AltDirectorySeparatorChar));
        Assert.False(DirectoryPathValidator.IsDirectorySeparator('x'));
    }
}
