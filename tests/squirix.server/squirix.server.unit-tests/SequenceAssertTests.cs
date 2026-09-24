using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for the literal-friendly <see cref="SequenceAssert" /> overload.</summary>
[Immutable]
public sealed class SequenceAssertTests : ServerUnitTestBase
{
    /// <summary>A differing element fails the assertion.</summary>
    [Test]
    public async Task SpanOverloadFailsOnElementMismatch()
    {
        var actual = new[] { 1, 2, 4 };
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(SequenceAssert.EqualAsync<int>([1, 2, 3], actual));
    }

    /// <summary>A different length fails the assertion.</summary>
    [Test]
    public async Task SpanOverloadFailsOnLengthMismatch()
    {
        var actual = new[] { 1, 2 };
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(SequenceAssert.EqualAsync<int>([1, 2, 3], actual));
    }

    /// <summary>The comparer overload honors the supplied comparer for both matches and mismatches.</summary>
    [Test]
    public async Task SpanOverloadUsesSuppliedComparer()
    {
        var actual = new[] { "A", "b" };
        await SequenceAssert.EqualAsync<string>(["a", "B"], actual, StringComparer.OrdinalIgnoreCase);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(SequenceAssert.EqualAsync<string>(["a", "B"], actual, StringComparer.Ordinal));
    }

    /// <summary>Equal literal and actual items pass.</summary>
    [Test]
    public Task SpanOverloadPassesOnEqualItems()
    {
        var actual = new[] { 1, 2, 3 };
        return SequenceAssert.EqualAsync<int>([1, 2, 3], actual);
    }
}
