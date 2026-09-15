using System;
using System.Collections.Frozen;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="EntryTagsGuard" />.</summary>
[Immutable]
public sealed class EntryTagsGuardTests : ServerUnitTestBase
{
    /// <summary>Invalid tag shapes are rejected with deterministic contracts.</summary>
    /// <param name="caseName">Named invalid-tag scenario.</param>
    /// <param name="expectedDetailFragment">Expected detail fragment for that scenario.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="caseName" /> is not a known test case.</exception>
    [Test]
    [Arguments("count", "32")]
    [Arguments("key", "256")]
    [Arguments("value", "1024")]
    public async Task InvalidTagsThrowInvalidEntryTags(string caseName, string expectedDetailFragment)
    {
        var tags = GetTags(caseName);

        var ex = NodeExceptionAssert.For<SquirixException>().Throws(tags, static value => EntryTagsGuard.EnsureWithinLimits(value));

        _ = await Assert.That(ex.Code).IsEqualTo(SquirixErrorCode.InvalidEntryTags);
        _ = await Assert.That(ex.Detail).Contains(expectedDetailFragment, StringComparison.Ordinal);
    }

    /// <summary>Null or empty tags are allowed.</summary>
    [Test]
    public async Task NullOrEmptyTagsDoNotThrow()
    {
        _ = await Assert.That(static () => EntryTagsGuard.EnsureWithinLimits(null)).ThrowsNothing();
        _ = await Assert.That(static () => EntryTagsGuard.EnsureWithinLimits(FrozenDictionary<string, string>.Empty)).ThrowsNothing();
    }

    /// <summary>Tags within limits pass validation.</summary>
    [Test]
    public async Task TagsWithinLimitsDoNotThrow()
    {
        var tags = CreateTags(EntryLimits.MaxEntryTagCount);

        EntryTagsGuard.EnsureWithinLimits(tags);
        _ = await Assert.That(tags.Count).IsEqualTo(EntryLimits.MaxEntryTagCount);
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

    private static FrozenDictionary<string, string> GetTags(string caseName) => caseName switch
    {
        "count" => CreateTags(EntryLimits.MaxEntryTagCount + 1),
        "key" => CreateOversizedKeyTags(),
        "value" => CreateOversizedValueTags(),
        _ => throw new ArgumentOutOfRangeException(nameof(caseName), caseName, "Unsupported tag test case."),
    };
}
