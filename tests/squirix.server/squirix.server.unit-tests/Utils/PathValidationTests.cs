using System;
using System.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers shared path character and Windows reserved-name validation.</summary>
public sealed class PathValidationTests : ServerUnitTestBase
{
    /// <summary>Rejects wildcards in paths.</summary>
    /// <param name="path">Path containing a wildcard.</param>
    [Theory]
    [InlineData("a*b")]
    [InlineData("a?b")]
    public void ValidateNoInvalidCharsRejectsWildcards(string path)
    {
        var ex = Assert.Throws<ArgumentException>(() => PathValidation.ValidateNoInvalidChars(path, nameof(path)));
        Assert.Contains("wildcard", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects platform-invalid path characters when present.</summary>
    [Fact]
    public void ValidateNoInvalidCharsRejectsInvalidPathCharacters()
    {
        var invalid = Path.GetInvalidPathChars();
        if (invalid.Length is 0)
            return;

        var path = $"ok{invalid[0]}name";
        var ex = Assert.Throws<ArgumentException>(() => PathValidation.ValidateNoInvalidChars(path, nameof(path)));
        Assert.Contains("invalid characters", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects empty path segments.</summary>
    [Fact]
    public void ValidateSegmentRejectsEmptySegment()
    {
        var ex = Assert.Throws<ArgumentException>(static () => PathValidation.ValidateSegment([], "a//b", "path", false));
        Assert.Contains("Empty segment", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Rejects <c>.</c> and <c>..</c> when requested.</summary>
    /// <param name="segment">Dot segment text.</param>
    [Theory]
    [InlineData(".")]
    [InlineData("..")]
    public void ValidateSegmentRejectsDotOrDotDotWhenRequested(string segment)
    {
        var ex = Assert.Throws<ArgumentException>(() => PathValidation.ValidateSegment(segment.AsSpan(), segment, "path", true));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Allows ordinary relative segments.</summary>
    [Fact]
    public void ValidateSegmentAllowsOrdinaryName() => PathValidation.ValidateSegment("data".AsSpan(), "data", "path", false);

    /// <summary>IsDotOrDotDot recognizes both tokens.</summary>
    /// <param name="segment">Candidate segment.</param>
    /// <param name="expected">Expected result.</param>
    [Theory]
    [InlineData(".", true)]
    [InlineData("..", true)]
    [InlineData("...", false)]
    [InlineData("a", false)]
    public void IsDotOrDotDotMatchesExpected(string segment, bool expected) =>
        Assert.Equal(expected, PathValidation.IsDotOrDotDot(segment.AsSpan()));

    /// <summary>On Windows (or when Windows rules are forced), reserved device names such as CON are rejected.</summary>
    /// <param name="segment">Reserved Windows name.</param>
    [Theory]
    [InlineData("CON")]
    [InlineData("con")]
    [InlineData("PRN")]
    [InlineData("AUX")]
    [InlineData("NUL")]
    [InlineData("COM1")]
    [InlineData("LPT9")]
    [InlineData("COM1.txt")]
    public void ValidateSegmentRejectsWindowsReservedNames(string segment)
    {
        var ex = Assert.Throws<ArgumentException>(() => PathValidation.ValidateSegment(segment.AsSpan(), segment, "path", false, true));
        Assert.Contains("reserved Windows name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Non-reserved COM/LPT-like names are accepted under Windows rules.</summary>
    /// <param name="segment">Non-reserved name.</param>
    [Theory]
    [InlineData("COM")]
    [InlineData("COM10")]
    [InlineData("LPT")]
    [InlineData("normal")]
    public void ValidateSegmentAcceptsNonReservedWindowsNames(string segment) =>
        PathValidation.ValidateSegment(segment.AsSpan(), segment, "path", false, true);

    /// <summary>Trailing spaces and dots are rejected under Windows rules.</summary>
    /// <param name="segment">Illegal Windows segment.</param>
    [Theory]
    [InlineData("name ")]
    [InlineData("name.")]
    public void ValidateSegmentRejectsWindowsTrailingSpaceOrDot(string segment)
    {
        var ex = Assert.Throws<ArgumentException>(() => PathValidation.ValidateSegment(segment.AsSpan(), segment, "path", false, true));
        Assert.Contains("space or dot", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Accepts ordinary relative paths with no invalid characters.</summary>
    [Fact]
    public void ValidateNoInvalidCharsAcceptsOrdinaryRelativePath() => PathValidation.ValidateNoInvalidChars("data/subdir", "path");
}
