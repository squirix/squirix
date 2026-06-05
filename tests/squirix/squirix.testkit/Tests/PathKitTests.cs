using System;
using System.IO;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.TestKit.Tests;

/// <summary>
/// Unit tests for <see cref="PathKit" />.
/// </summary>
public sealed class PathKitTests
{
    /// <summary>
    /// Combine ignores empty segments and returns an empty string when nothing usable is provided.
    /// </summary>
    [Fact]
    public void CombineIgnoresEmptySegments()
    {
        var path = PathKit.Combine(true, string.Empty, "   ");
        Assert.Equal(string.Empty, path);
    }

    /// <summary>
    /// Combine preserves rooted prefixes while sanitizing nested segments.
    /// </summary>
    [Fact]
    public void CombinePreservesRootedPrefix()
    {
        var invalidLeaf = Path.GetFileName("bad:name");
        var rooted = Path.Combine(Path.GetTempPath(), invalidLeaf);
        var path = PathKit.Combine(true, rooted, "child?.txt");

        Assert.StartsWith(Path.GetPathRoot(path), path, StringComparison.OrdinalIgnoreCase);
        Assert.EndsWith($"bad_name{Path.DirectorySeparatorChar}child_.txt", path, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Combine preserves the original segments when sanitization is disabled.
    /// </summary>
    [Fact]
    public void CombinePreservesSegmentsWhenSanitizeIsDisabled()
    {
        var path = PathKit.Combine(false, "root", "bad:name?.txt");

        Assert.Equal(Path.Combine("root", "bad:name?.txt"), path);
    }

    /// <summary>
    /// Combine sanitizes invalid file-name characters in non-root segments by default.
    /// </summary>
    [Fact]
    public void CombineSanitizesSegmentsByDefault()
    {
        var path = PathKit.Combine(true, "root", "bad:name?.txt");

        Assert.Equal(Path.Combine("root", "bad_name_.txt"), path);
    }

    /// <summary>
    /// Process temp path includes the temp root, sanitized framework segment, and process id.
    /// </summary>
    [Fact]
    public void GetProcTempPathBuildsProcessScopedPath()
    {
        var path = PathKit.GetProcTempPath("tests");

        Assert.StartsWith(Path.Combine(Path.GetTempPath(), "tests"), path, StringComparison.OrdinalIgnoreCase);
        Assert.Contains($"pid{Environment.ProcessId}", path, StringComparison.Ordinal);
        Assert.Contains("-start", path, StringComparison.Ordinal);
    }

    /// <summary>
    /// Path name helpers return the expected directory and file components.
    /// </summary>
    [Fact]
    public void PathNameHelpersReturnExpectedComponents()
    {
        var path = Path.Combine("root", "child", "file.txt");

        Assert.Equal(Path.Combine("root", "child"), PathKit.GetDirectoryName(path));
        Assert.Equal("file.txt", PathKit.GetFileName(path));
        Assert.Equal("file", PathKit.GetFileNameWithoutExtension(path));
    }
}
