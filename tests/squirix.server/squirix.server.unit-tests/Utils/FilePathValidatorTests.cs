using System;
using System.IO;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers operator path validation used before file I/O.</summary>
public sealed class FilePathValidatorTests : ServerUnitTestBase
{
    /// <summary>Accepts a simple relative file path and returns an absolute path.</summary>
    [Fact]
    public void ResolveValidatedFilePathAcceptsRelativeFileName()
    {
        var full = FilePathValidator.ResolveValidatedFilePath("Squirix.settings.json");
        Assert.True(Path.IsPathRooted(full));
        Assert.EndsWith("Squirix.settings.json", full, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Accepts an absolute directory path without traversal segments.</summary>
    [Fact]
    public void ResolveValidatedDirectoryPathAcceptsAbsolutePath()
    {
        var input = Path.Join(Path.GetTempPath(), "squirix-path-validator");
        var full = FilePathValidator.ResolveValidatedDirectoryPath(input);
        Assert.Equal(Path.GetFullPath(input), full);
    }

    /// <summary>Rejects parent-directory segments in operator paths.</summary>
    /// <param name="path">Path containing <c>.</c> or <c>..</c> segments.</param>
    [Theory]
    [InlineData("..")]
    [InlineData("../Squirix.settings.json")]
    [InlineData("foo/../bar.json")]
    [InlineData("foo/./bar.json")]
    public void ResolveValidatedFilePathRejectsDotSegments(string path)
    {
        var ex = Assert.Throws<ArgumentException>(() => FilePathValidator.ResolveValidatedFilePath(path));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Rejects wildcards in operator paths.</summary>
    /// <param name="path">Path containing wildcards.</param>
    [Theory]
    [InlineData("*.json")]
    [InlineData("settings?.json")]
    public void ResolveValidatedFilePathRejectsWildcards(string path)
    {
        var ex = Assert.Throws<ArgumentException>(() => FilePathValidator.ResolveValidatedFilePath(path));
        Assert.Contains("wildcard", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects empty and whitespace paths.</summary>
    /// <param name="path">Empty or whitespace path.</param>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ResolveValidatedFilePathRejectsEmpty(string? path) => _ = Assert.Throws<ArgumentException>(() => FilePathValidator.ResolveValidatedFilePath(path!));

    /// <summary>PathEx relative joins reject parent-directory segments.</summary>
    [Fact]
    public void PathExCombineRejectsParentSegments()
    {
        var root = Path.GetTempPath();
        var ex = Assert.Throws<ArgumentException>(() => PathEx.Combine(root, "foo/../bar"));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>PathEx multi-segment combine keeps results under the root.</summary>
    [Fact]
    public void PathExCombineAcceptsMultipleRelativeSegments()
    {
        using var root = new TempDirectory("squirix-path-ex-multi");
        var combined = PathEx.Combine(root.Path, "a", "b");
        Assert.Equal(Path.GetFullPath(Path.Join(root.Path, "a", "b")), combined);

        var triple = PathEx.Combine(root.Path, "a", "b", "c");
        Assert.Equal(Path.GetFullPath(Path.Join(root.Path, "a", "b", "c")), triple);
    }

    /// <summary>FileEx.TryDeleteFile treats traversal paths as skipped successes.</summary>
    [Fact]
    public void FileExTryDeleteFileSkipsTraversalPaths() => Assert.True(FileEx.TryDeleteFile("../nope.txt"));
}
