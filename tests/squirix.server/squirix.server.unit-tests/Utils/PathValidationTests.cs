using System;
using System.IO;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers shared path character and Windows reserved-name validation.</summary>
public sealed class PathValidationTests : ServerUnitTestBase
{
    /// <summary>Non-reserved COM/LPT-like names are accepted under Windows rules.</summary>
    /// <param name="segment">Non-reserved name.</param>
    [Theory]
    [InlineData("COM")]
    [InlineData("COM10")]
    [InlineData("LPT")]
    [InlineData("normal")]
    public static void ValidateSegmentAcceptsNonReservedWindowsNames(string segment) => PathValidation.ValidateSegment(segment.AsSpan(), "path", false, true);

    /// <summary>Allows ordinary relative segments.</summary>
    [Fact]
    public static void ValidateSegmentAllowsOrdinaryName() => PathValidation.ValidateSegment("data".AsSpan(), "path", false);

    /// <summary>Rejects <c>.</c> and <c>..</c> when requested.</summary>
    /// <param name="segment">Dot segment text.</param>
    [Theory]
    [InlineData(".")]
    [InlineData("..")]
    public static void ValidateSegmentRejectsDotOrDotDotWhenRequested(string segment)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(segment, static value => PathValidation.ValidateSegment(value.AsSpan(), "path", true));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Rejects empty path segments.</summary>
    [Fact]
    public static void ValidateSegmentRejectsEmptySegment()
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws("a//b", static _ => PathValidation.ValidateSegment([], "path", false));
        Assert.Contains("Empty segment", ex.Message, StringComparison.Ordinal);
    }

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
    public static void ValidateSegmentRejectsWindowsReservedNames(string segment)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(segment, static value => PathValidation.ValidateSegment(value.AsSpan(), "path", false, true));
        Assert.Contains("reserved Windows name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Trailing spaces and dots are rejected under Windows rules.</summary>
    /// <param name="segment">Illegal Windows segment.</param>
    [Theory]
    [InlineData("name ")]
    [InlineData("name.")]
    public static void ValidateSegmentRejectsWindowsTrailingSpaceOrDot(string segment)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(segment, static value => PathValidation.ValidateSegment(value.AsSpan(), "path", false, true));
        Assert.Contains("space or dot", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>IsDotOrDotDot recognizes both tokens.</summary>
    /// <param name="segment">Candidate segment.</param>
    /// <param name="expected">Expected result.</param>
    [Theory]
    [InlineData(".", true)]
    [InlineData("..", true)]
    [InlineData("...", false)]
    [InlineData("a", false)]
    public static void IsDotOrDotDotMatchesExpected(string segment, bool expected) => Assert.Equal(expected, PathValidation.IsDotOrDotDot(segment.AsSpan()));

    /// <summary>Accepts ordinary relative paths with no invalid characters.</summary>
    [Fact]
    public static void ValidateNoInvalidCharsAcceptsOrdinaryRelativePath() => PathValidation.ValidateNoInvalidChars("data/subdir", "path");

    /// <summary>Rejects platform-invalid path characters when present.</summary>
    [Fact]
    public static void ValidateNoInvalidCharsRejectsInvalidPathCharacters()
    {
        var invalid = Path.GetInvalidPathChars();
        if (invalid.Length is 0)
            return;

        var path = $"ok{invalid[0]}name";
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => PathValidation.ValidateNoInvalidChars(value, nameof(value)));
        Assert.Contains("invalid characters", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects wildcards in paths.</summary>
    /// <param name="path">Path containing a wildcard.</param>
    [Theory]
    [InlineData("a*b")]
    [InlineData("a?b")]
    public static void ValidateNoInvalidCharsRejectsWildcards(string path)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => PathValidation.ValidateNoInvalidChars(value, nameof(value)));
        Assert.Contains("wildcard", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
