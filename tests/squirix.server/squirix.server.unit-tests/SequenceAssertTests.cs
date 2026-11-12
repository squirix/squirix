using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for the literal-friendly <see cref="SequenceAssert" /> overload.</summary>
[Immutable]
public sealed class SequenceAssertTests : ServerUnitTestBase
{
    /// <summary>List and collection overloads preserve empty inputs, order and duplicates.</summary>
    /// <param name="useList">Whether to use the indexed overload.</param>
    /// <param name="count">The number of items.</param>
    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 3)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 3)]
    public Task CollectionOverloadsPassOnEqualItems(bool useList, int count)
    {
        int[] items = [2, 1, 2];
        return CompareAsync(useList, items[..count], items[..count]);
    }

    /// <summary>Equal multisets in different orders are rejected.</summary>
    /// <param name="useList">Whether to use the indexed overload.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CollectionOverloadsPreserveOrder(bool useList, CancellationToken cancellationToken)
    {
        var failure = await NodeAsyncAssert.ThrowsAsync<AssertionException>(CompareAsync(useList, [1, 2, 1], [1, 1, 2]), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(failure.Message).Contains("Sequences differ at index 1.");
    }

    /// <summary>Both overloads reject a shorter or longer sequence, including empty input.</summary>
    /// <param name="useList">Whether to use the indexed overload.</param>
    /// <param name="longer">Whether the actual sequence is longer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task CollectionOverloadsRejectLengthMismatch(bool useList, bool longer, CancellationToken cancellationToken)
    {
        int[] expected = longer ? [] : [1];
        int[] actual = longer ? [1] : [];
        _ = await NodeAsyncAssert.ThrowsAsync<AssertionException>(CompareAsync(useList, expected, actual), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>List and collection overloads report the first differing position.</summary>
    /// <param name="useList">Whether to use the indexed overload.</param>
    /// <param name="index">The first differing position.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    public async Task CollectionOverloadsReportFirstMismatch(bool useList, int index, CancellationToken cancellationToken)
    {
        int[] actual = [1, 2, 3];
        Array.Fill(actual, 9, index, actual.Length - index);
        var failure = await NodeAsyncAssert.ThrowsAsync<AssertionException>(CompareAsync(useList, [1, 2, 3], actual), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(failure.Message).Contains($"Sequences differ at index {index}.");
    }

    /// <summary>The indexed overload uses the supplied comparer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ListOverloadUsesSuppliedComparer(CancellationToken cancellationToken)
    {
        IReadOnlyList<string> expected = new[] { "a", "B" };
        IReadOnlyList<string> actual = new[] { "A", "b" };
        await SequenceAssert.EqualAsync(expected, actual, StringComparer.OrdinalIgnoreCase);
        var failure = await NodeAsyncAssert.ThrowsAsync<AssertionException>(SequenceAssert.EqualAsync(expected, actual, StringComparer.Ordinal), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(failure.Message).Contains("Sequences differ at index 0.");
    }

    /// <summary>A differing element fails the assertion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpanOverloadFailsOnElementMismatch(CancellationToken cancellationToken)
    {
        var actual = new[] { 1, 2, 4 };
        _ = await NodeAsyncAssert.ThrowsAsync<AssertionException>(SequenceAssert.EqualAsync([1, 2, 3], actual), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>A different length fails the assertion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpanOverloadFailsOnLengthMismatch(CancellationToken cancellationToken)
    {
        var actual = new[] { 1, 2 };
        _ = await NodeAsyncAssert.ThrowsAsync<AssertionException>(SequenceAssert.EqualAsync([1, 2, 3], actual), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>Equal literal and actual items pass.</summary>
    [Test]
    public Task SpanOverloadPassesOnEqualItems()
    {
        var actual = new[] { 1, 2, 3 };
        return SequenceAssert.EqualAsync([1, 2, 3], actual);
    }

    /// <summary>The comparer overload honors the supplied comparer for both matches and mismatches.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpanOverloadUsesSuppliedComparer(CancellationToken cancellationToken)
    {
        var actual = new[] { "A", "b" };
        await SequenceAssert.EqualAsync<string>(["a", "B"], actual, StringComparer.OrdinalIgnoreCase);
        _ = await NodeAsyncAssert.ThrowsAsync<AssertionException>(SequenceAssert.EqualAsync<string>(["a", "B"], actual, StringComparer.Ordinal), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    private static Task CompareAsync(bool useList, int[] expected, int[] actual)
    {
        if (!useList)
            return SequenceAssert.EqualAsync(new Queue<int>(expected), new Queue<int>(actual));
        IReadOnlyList<int> expectedItems = expected;
        return SequenceAssert.EqualAsync(expectedItems, actual, EqualityComparer<int>.Default);
    }
}
