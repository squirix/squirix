using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using Squirix.Server.Errors;
using Squirix.Server.Limits;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Limits;

/// <summary>Unit tests for <see cref="EntryTagsGuard" />.</summary>
public sealed class EntryTagsGuardTests : UnitTestBase
{
    /// <summary>Null or empty tags are allowed.</summary>
    [Fact]
    public void NullOrEmptyTagsDoNotThrow()
    {
        Assert.Null(Record.Exception(static () => EntryTagsGuard.EnsureWithinLimits(null)));
        Assert.Null(Record.Exception(static () => EntryTagsGuard.EnsureWithinLimits(FrozenDictionary<string, string>.Empty)));
    }

    /// <summary>Tag count above the limit is rejected.</summary>
    [Fact]
    public void TagCountAboveLimitThrowsInvalidEntryTags()
    {
        var tags = CreateTags(SquirixEntryLimits.MaxEntryTagCount + 1);

        var ex = Assert.Throws<SquirixException>(() => EntryTagsGuard.EnsureWithinLimits(tags));

        Assert.Equal(SquirixErrorCode.InvalidEntryTags, ex.Code);
        Assert.Contains(SquirixEntryLimits.MaxEntryTagCount.ToString(CultureInfo.InvariantCulture), ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Oversized tag keys are rejected by UTF-8 byte length.</summary>
    [Fact]
    public void TagKeyAboveLimitThrowsInvalidEntryTags()
    {
        var key = new string('k', SquirixEntryLimits.MaxEntryTagKeyUtf8Bytes + 1);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { [key] = "v" }.ToFrozenDictionary(StringComparer.Ordinal);

        var ex = Assert.Throws<SquirixException>(() => EntryTagsGuard.EnsureWithinLimits(tags));

        Assert.Equal(SquirixErrorCode.InvalidEntryTags, ex.Code);
        Assert.Contains(SquirixEntryLimits.MaxEntryTagKeyUtf8Bytes.ToString(CultureInfo.InvariantCulture), ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Oversized tag values are rejected by UTF-8 byte length.</summary>
    [Fact]
    public void TagValueAboveLimitThrowsInvalidEntryTags()
    {
        var value = new string('v', SquirixEntryLimits.MaxEntryTagValueUtf8Bytes + 1);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["k"] = value }.ToFrozenDictionary(StringComparer.Ordinal);

        var ex = Assert.Throws<SquirixException>(() => EntryTagsGuard.EnsureWithinLimits(tags));

        Assert.Equal(SquirixErrorCode.InvalidEntryTags, ex.Code);
        Assert.Contains(SquirixEntryLimits.MaxEntryTagValueUtf8Bytes.ToString(CultureInfo.InvariantCulture), ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Tags within limits pass validation.</summary>
    [Fact]
    public void TagsWithinLimitsDoNotThrow()
    {
        var tags = CreateTags(SquirixEntryLimits.MaxEntryTagCount);

        Assert.Null(Record.Exception(() => EntryTagsGuard.EnsureWithinLimits(tags)));
    }

    private static FrozenDictionary<string, string> CreateTags(int count)
    {
        var tags = new Dictionary<string, string>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
            tags[$"k{i}"] = "v";

        return tags.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
