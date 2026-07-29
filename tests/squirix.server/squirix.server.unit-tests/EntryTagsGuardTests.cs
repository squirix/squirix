using System;
using System.Collections.Frozen;
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

    /// <summary>Invalid tag shapes are rejected with deterministic contracts.</summary>
    /// <param name="caseName">Named invalid-tag scenario.</param>
    /// <param name="expectedDetailFragment">Expected detail fragment for that scenario.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="caseName" /> is not a known test case.</exception>
    [Theory]
    [InlineData("count", "32")]
    [InlineData("key", "256")]
    [InlineData("value", "1024")]
    public void InvalidTagsThrowInvalidEntryTags(string caseName, string expectedDetailFragment)
    {
        var tags = caseName switch
        {
            "count" => CreateTags(EntryLimits.MaxEntryTagCount + 1),
            "key" => CreateOversizedKeyTags(),
            "value" => CreateOversizedValueTags(),
            _ => throw new ArgumentOutOfRangeException(nameof(caseName), caseName, "Unsupported tag test case."),
        };

        var ex = NodeExceptionAssert.For<SquirixException>().Throws(tags, static value => EntryTagsGuard.EnsureWithinLimits(value));

        Assert.Equal(SquirixErrorCode.InvalidEntryTags, ex.Code);
        Assert.Contains(expectedDetailFragment, ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Tags within limits pass validation.</summary>
    [Fact]
    public void TagsWithinLimitsDoNotThrow()
    {
        var tags = CreateTags(EntryLimits.MaxEntryTagCount);

        EntryTagsGuard.EnsureWithinLimits(tags);
        Assert.Equal(EntryLimits.MaxEntryTagCount, tags.Count);
    }

    private static FrozenDictionary<string, string> CreateOversizedKeyTags()
    {
        var key = new string('k', EntryLimits.MaxEntryTagKeyUtf8Bytes + 1);
        return EntryTagsKit.One(key, "v");
    }

    private static FrozenDictionary<string, string> CreateOversizedValueTags()
    {
        var value = new string('v', EntryLimits.MaxEntryTagValueUtf8Bytes + 1);
        return EntryTagsKit.One("k", value);
    }

    private static FrozenDictionary<string, string> CreateTags(int count) => EntryTagsKit.CreateCount(count);
}
