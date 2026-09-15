using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers shared path character and Windows reserved-name validation.</summary>
[UsedImplicitly]
[Immutable]
public sealed class PathValidationTests : ServerUnitTestBase
{
    /// <summary>IsDotOrDotDot recognizes both tokens.</summary>
    /// <param name="segment">Candidate segment.</param>
    /// <param name="expected">Expected result.</param>
    [Test]
    [Arguments(".", true)]
    [Arguments("..", true)]
    [Arguments("...", false)]
    [Arguments("a", false)]
    public async Task IsDotOrDotDotMatchesExpected(string segment, bool expected) => _ = await Assert.That(PathValidation.IsDotOrDotDot(segment.AsSpan())).IsEqualTo(expected);

    /// <summary>Accepts ordinary relative paths with no invalid characters.</summary>
    [Test]
    public void NoInvalidCharsAcceptsRelativePath() => PathValidation.ValidateNoInvalidChars("data/subdir", "path");

    /// <summary>Rejects platform-invalid path characters when present.</summary>
    [Test]
    public async Task NoInvalidCharsRejectsBadCharacters()
    {
        var invalid = Path.GetInvalidPathChars();
        if (invalid.Length == 0)
            return;

        var path = $"ok{invalid[0]}name";
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => PathValidation.ValidateNoInvalidChars(value, nameof(value)));
        _ = await Assert.That(ex.Message).Contains("invalid characters", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Non-reserved COM/LPT-like names are accepted under Windows rules.</summary>
    /// <param name="segment">Non-reserved name.</param>
    [Test]
    [Arguments("COM")]
    [Arguments("COM10")]
    [Arguments("LPT")]
    [Arguments("normal")]
    public void SegmentAcceptsNonReservedNames(string segment) => PathValidation.ValidateSegment(segment.AsSpan(), "path", false, true);

    /// <summary>Windows reserved names are accepted when Windows rules are forced off.</summary>
    [Test]
    public void SegmentAllowsReservedWithoutRules() => PathValidation.ValidateSegment("CON".AsSpan(), "path", false, false);

    /// <summary>Rejects <c language="csharp">.</c> and <c language="csharp">..</c> when requested.</summary>
    /// <param name="segment">Dot segment text.</param>
    [Test]
    [Arguments(".")]
    [Arguments("..")]
    public async Task SegmentRejectsDotSegmentsOnDemand(string segment)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(segment, static value => PathValidation.ValidateSegment(value.AsSpan(), "path", true));
        _ = await Assert.That(ex.Message).Contains("'.' or '..'", StringComparison.Ordinal);
    }

    /// <summary>Rejects invalid file-name characters in a segment.</summary>
    [Test]
    public async Task SegmentRejectsInvalidCharacters()
    {
        var invalid = Path.GetInvalidFileNameChars();
        if (invalid.Length == 0)
            return;

        char? candidate = null;
        for (var i = 0; i < invalid.Length; i++)
        {
            var ch = invalid[i];
            if (ch == '/' || ch == '\\' || ch == '\0')
                continue;

            candidate = ch;
            break;
        }

        if (candidate == null)
            return;

        var segment = $"na{candidate.Value}me";
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(segment, static value => PathValidation.ValidateSegment(value.AsSpan(), "path", false, false));
        _ = await Assert.That(ex.Message).Contains("invalid characters", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Trailing spaces and dots are rejected under Windows rules.</summary>
    /// <param name="segment">Illegal Windows segment.</param>
    [Test]
    [Arguments("name ")]
    [Arguments("name.")]
    public async Task SegmentRejectsTrailingSpaceOrDot(string segment)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(segment, static value => PathValidation.ValidateSegment(value.AsSpan(), "path", false, true));
        _ = await Assert.That(ex.Message).Contains("space or dot", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>On Windows (or when Windows rules are forced), reserved device names such as CON are rejected.</summary>
    /// <param name="segment">Reserved Windows name.</param>
    [Test]
    [Arguments("CON")]
    [Arguments("con")]
    [Arguments("PRN")]
    [Arguments("AUX")]
    [Arguments("NUL")]
    [Arguments("COM1")]
    [Arguments("LPT9")]
    [Arguments("COM1.txt")]
    public async Task SegmentRejectsWindowsReservedNames(string segment)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(segment, static value => PathValidation.ValidateSegment(value.AsSpan(), "path", false, true));
        _ = await Assert.That(ex.Message).Contains("reserved Windows name", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects wildcards in paths.</summary>
    /// <param name="path">Path containing a wildcard.</param>
    [Test]
    [Arguments("a*b")]
    [Arguments("a?b")]
    public async Task ValidateNoInvalidCharsRejectsWildcards(string path)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => PathValidation.ValidateNoInvalidChars(value, nameof(value)));
        _ = await Assert.That(ex.Message).Contains("wildcard", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Allows ordinary relative segments.</summary>
    [Test]
    public void ValidateSegmentAllowsOrdinaryName() => PathValidation.ValidateSegment("data".AsSpan(), "path", false);

    /// <summary>Rejects empty path segments.</summary>
    [Test]
    public async Task ValidateSegmentRejectsEmptySegment()
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws("a//b", static _ => PathValidation.ValidateSegment([], "path", false));
        _ = await Assert.That(ex.Message).Contains("Empty segment", StringComparison.Ordinal);
    }
}
