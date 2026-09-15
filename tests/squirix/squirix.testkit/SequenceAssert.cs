using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.TestKit;

/// <summary>Ordered sequence assertions without LINQ or reflection (AOT-safe).</summary>
/// <remarks>Client surface stays minimal on purpose: only the comparer overload is used here.
/// For the full set (collections, memory blocks, default comparer) see Squirix.Server.TestKit.</remarks>
public static class SequenceAssert
{
    /// <summary>Asserts two sequences contain equal items in the same order using a custom comparer.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="expected">Expected items in order.</param>
    /// <param name="actual">Actual items in order.</param>
    /// <param name="comparer">Element equality comparer.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    public static Task Equal<T>(IReadOnlyList<T> expected, IReadOnlyList<T> actual, IEqualityComparer<T> comparer)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        ArgumentNullException.ThrowIfNull(comparer);

        return EqualAsync(expected, actual, comparer);
    }

    private static async Task EqualAsync<T>(IReadOnlyList<T> expected, IReadOnlyList<T> actual, IEqualityComparer<T> comparer)
    {
        _ = await Assert.That(actual.Count).IsEqualTo(expected.Count);
        for (var i = 0; i < expected.Count; i++)
            _ = await Assert.That(comparer.Equals(expected[i], actual[i])).IsTrue().Because($"Sequences differ at index {i}.");
    }
}
