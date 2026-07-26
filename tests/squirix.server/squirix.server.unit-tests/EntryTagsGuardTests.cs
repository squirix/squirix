using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="EntryTagsGuard" />.</summary>
public sealed class EntryTagsGuardTests : ServerUnitTestBase
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
        var tags = CreateTags(EntryLimits.MaxEntryTagCount + 1);

        var ex = NodeExceptionAssert.For<SquirixException>().Throws(tags, static value => EntryTagsGuard.EnsureWithinLimits(value));

        Assert.Equal(SquirixErrorCode.InvalidEntryTags, ex.Code);
        Assert.Contains("32", ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Oversized tag keys are rejected by UTF-8 byte length.</summary>
    [Fact]
    public void TagKeyAboveLimitThrowsInvalidEntryTags()
    {
        var key = new string('k', EntryLimits.MaxEntryTagKeyUtf8Bytes + 1);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { [key] = "v" }.ToFrozenDictionary(StringComparer.Ordinal);

        var ex = NodeExceptionAssert.For<SquirixException>().Throws(tags, static value => EntryTagsGuard.EnsureWithinLimits(value));

        Assert.Equal(SquirixErrorCode.InvalidEntryTags, ex.Code);
        Assert.Contains("256", ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Oversized tag values are rejected by UTF-8 byte length.</summary>
    [Fact]
    public void TagValueAboveLimitThrowsInvalidEntryTags()
    {
        var value = new string('v', EntryLimits.MaxEntryTagValueUtf8Bytes + 1);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["k"] = value }.ToFrozenDictionary(StringComparer.Ordinal);

        var ex = NodeExceptionAssert.For<SquirixException>().Throws(tags, static value => EntryTagsGuard.EnsureWithinLimits(value));

        Assert.Equal(SquirixErrorCode.InvalidEntryTags, ex.Code);
        Assert.Contains("1024", ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Tags within limits pass validation.</summary>
    [Fact]
    public void TagsWithinLimitsDoNotThrow()
    {
        var tags = CreateTags(EntryLimits.MaxEntryTagCount);

        EntryTagsGuard.EnsureWithinLimits(tags);
        Assert.Equal(EntryLimits.MaxEntryTagCount, tags.Count);
    }

    private static FrozenDictionary<string, string> CreateTags(int count)
    {
        var tags = new Dictionary<string, string>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
            tags[InvariantIndexStrings.Format(i)] = "v";

        return tags.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
