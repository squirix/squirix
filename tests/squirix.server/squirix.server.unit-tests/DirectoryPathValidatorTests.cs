using System;
using System.IO;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers directory path resolution and segment parsing.</summary>
public sealed class DirectoryPathValidatorTests : ServerUnitTestBase
{
    /// <summary>IsDirectorySeparator recognizes both separators.</summary>
    [Fact]
    public void IsDirectorySeparatorRecognizesBoth()
    {
        Assert.True(DirectoryPathValidator.IsDirectorySeparator(Path.DirectorySeparatorChar));
        Assert.True(DirectoryPathValidator.IsDirectorySeparator(Path.AltDirectorySeparatorChar));
        Assert.False(DirectoryPathValidator.IsDirectorySeparator('x'));
    }

    /// <summary>Resolves a relative path under a base directory.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathAcceptsRelativeBase()
    {
        using var root = new TempDirectory("squirix-dirpath-rel");
        var full = DirectoryPathValidator.ResolveValidatedDirectoryPath("child", root.Path, true);
        Assert.True(Path.IsPathRooted(full));
        Assert.StartsWith(root.Path, full, StringComparison.OrdinalIgnoreCase);
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

    /// <summary>Rejects targets that escape the base directory.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathRejectsBaseEscape()
    {
        using var root = new TempDirectory("squirix-dirpath-escape");
        var parent = Directory.GetParent(root.Path);
        Assert.NotNull(parent);
        var outside = Path.Join(parent.FullName, "squirix-dirpath-outside-" + Guid.NewGuid().ToString("N"));
        _ = NodeExceptionAssert.For<UnauthorizedAccessException>().Throws(
            outside,
            root.Path,
            static (path, basePath) => DirectoryPathValidator.ResolveValidatedDirectoryPath(path, basePath, true));
    }

    /// <summary>Rejects empty paths.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathRejectsEmpty() =>
        _ = NodeExceptionAssert.For<ArgumentException>().Throws("  ", static value => DirectoryPathValidator.ResolveValidatedDirectoryPath(value, null, false));

    /// <summary>Rejects when a regular file already exists at the target.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathRejectsExistingFile()
    {
        using var root = new TempDirectory("squirix-dirpath-file");
        var target = Path.Join(root.Path, "blocked");
        File.WriteAllText(target, "x");
        _ = NodeExceptionAssert.For<IOException>().Throws(root.Path, static basePath => DirectoryPathValidator.ResolveValidatedDirectoryPath("blocked", basePath, true));
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
}
