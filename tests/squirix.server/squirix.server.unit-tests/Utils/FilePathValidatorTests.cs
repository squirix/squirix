using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers operator path validation used before file I/O.</summary>
[Immutable]
public sealed class FilePathValidatorTests : IsolatedStorageTestBase
{
    /// <summary>Rejects parent-directory segments in operator paths.</summary>
    /// <param name="path">Path containing <c>.</c> or <c>..</c> segments.</param>
    [Theory]
    [InlineData("..")]
    [InlineData("../Squirix.settings.json")]
    [InlineData("foo/../bar.json")]
    [InlineData("foo/./bar.json")]
    public static void ResolveFileRejectsDotSegments(string path)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => FilePathValidator.ResolveValidatedFilePath(value));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Rejects empty and whitespace paths.</summary>
    /// <param name="path">Empty or whitespace path.</param>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public static void ResolveValidatedFilePathRejectsEmpty(string? path) =>
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => FilePathValidator.ResolveValidatedFilePath(value!));

    /// <summary>Rejects wildcards in operator paths.</summary>
    /// <param name="path">Path containing wildcards.</param>
    [Theory]
    [InlineData("*.json")]
    [InlineData("settings?.json")]
    public static void ResolveValidatedFilePathRejectsWildcards(string path)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => FilePathValidator.ResolveValidatedFilePath(value));
        Assert.Contains("wildcard", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>FileEx.TryDeleteFile treats traversal paths as skipped successes.</summary>
    [Fact]
    public void FileExTryDeleteFileSkipsTraversalPaths() => Assert.True(FileEx.TryDeleteFile("../nope.txt"));

    /// <summary>PathEx multi-segment combine keeps results under the Dir.</summary>
    [Fact]
    public void CombineAcceptsMultipleSegments()
    {
        var combined = PathEx.Combine(Dir.Path, "a", "b");
        Assert.Equal(Path.GetFullPath(Path.Join(Dir.Path, "a", "b")), combined);

        var triple = PathEx.Combine(Dir.Path, "a", "b", "c");
        Assert.Equal(Path.GetFullPath(Path.Join(Dir.Path, "a", "b", "c")), triple);
    }

    /// <summary>PathEx relative joins reject parent-directory segments.</summary>
    [Fact]
    public void PathExCombineRejectsParentSegments()
    {
        var root = Path.GetTempPath();
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(root, static value => PathEx.Combine(value, "foo/../bar"));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Accepts an absolute directory path without traversal segments.</summary>
    [Fact]
    public void ResolveDirAcceptsAbsolutePath()
    {
        var input = Path.Join(Path.GetTempPath(), "squirix-path-validator");
        var full = FilePathValidator.ResolveValidatedDirectoryPath(input);
        Assert.Equal(Path.GetFullPath(input), full);
    }

    /// <summary>Accepts a simple relative file path and returns an absolute path.</summary>
    [Fact]
    public void ResolveFileAcceptsRelativeName()
    {
        var full = FilePathValidator.ResolveValidatedFilePath("Squirix.settings.json");
        Assert.True(Path.IsPathRooted(full));
        Assert.EndsWith("Squirix.settings.json", full, StringComparison.OrdinalIgnoreCase);
    }
}
