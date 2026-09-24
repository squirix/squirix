using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Server.TestKit;

/// <summary>Ordered sequence assertions without LINQ or reflection (AOT-safe).</summary>
public static class SequenceAssert
{
    /// <summary>Asserts two sequences contain equal items in the same order.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="expected">Expected items in order.</param>
    /// <param name="actual">Actual items in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    public static Task EqualAsync<T>(IReadOnlyList<T> expected, IReadOnlyList<T> actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);

        return EqualCoreAsync(expected, actual, EqualityComparer<T>.Default);
    }

    /// <summary>Asserts a literal expected sequence equals the actual items in the same order, without allocating the expected side.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="expected">Expected items in order.</param>
    /// <param name="actual">Actual items in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    public static Task EqualAsync<T>(ReadOnlySpan<T> expected, IReadOnlyList<T> actual) => EqualAsync(expected, actual, EqualityComparer<T>.Default);

    /// <summary>Asserts a literal expected sequence equals the actual items in the same order using a custom comparer, without allocating the expected side.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="expected">Expected items in order.</param>
    /// <param name="actual">Actual items in order.</param>
    /// <param name="comparer">Element equality comparer.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    public static Task EqualAsync<T>(ReadOnlySpan<T> expected, IReadOnlyList<T> actual, IEqualityComparer<T> comparer)
    {
        ArgumentNullException.ThrowIfNull(actual);
        ArgumentNullException.ThrowIfNull(comparer);

        var mismatch = actual.Count == expected.Length ? FirstMismatch(expected, actual, comparer) : -1;
        return EqualSpanCoreAsync(expected.Length, actual.Count, mismatch);
    }

    /// <summary>Asserts two sequences contain equal items in the same order using a custom comparer.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="expected">Expected items in order.</param>
    /// <param name="actual">Actual items in order.</param>
    /// <param name="comparer">Element equality comparer.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    public static Task EqualAsync<T>(IReadOnlyList<T> expected, IReadOnlyList<T> actual, IEqualityComparer<T> comparer)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        ArgumentNullException.ThrowIfNull(comparer);

        return EqualCoreAsync(expected, actual, comparer);
    }

    /// <summary>Asserts two sequences contain equal items in the same order.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="expected">Expected items in order.</param>
    /// <param name="actual">Actual items in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    public static Task EqualAsync<T>(IReadOnlyCollection<T> expected, IReadOnlyCollection<T> actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);

        return EqualCoreAsync(expected, actual);
    }

    /// <summary>Asserts two memory blocks contain equal items in the same order.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="expected">Expected items in order.</param>
    /// <param name="actual">Actual items in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    public static Task EqualMemoryAsync<T>(ReadOnlyMemory<T> expected, ReadOnlyMemory<T> actual) => EqualMemoryAsync(expected, actual, EqualityComparer<T>.Default);

    private static T[] CopyToArray<T>(IReadOnlyCollection<T> source)
    {
        var items = new T[source.Count];
        var index = 0;
        foreach (var item in source)
            items[index++] = item;

        return items;
    }

    private static async Task EqualCoreAsync<T>(IReadOnlyList<T> expected, IReadOnlyList<T> actual, IEqualityComparer<T> comparer)
    {
        _ = await Assert.That(actual.Count).IsEqualTo(expected.Count);
        for (var i = 0; i < expected.Count; i++)
            _ = await Assert.That(comparer.Equals(expected[i], actual[i])).IsTrue().Because($"Sequences differ at index {i}.");
    }

    private static async Task EqualCoreAsync<T>(IReadOnlyCollection<T> expected, IReadOnlyCollection<T> actual)
    {
        var comparer = EqualityComparer<T>.Default;
        _ = await Assert.That(actual.Count).IsEqualTo(expected.Count);
        var expectedItems = CopyToArray(expected);
        var actualItems = CopyToArray(actual);
        for (var i = 0; i < expectedItems.Length; i++)
            _ = await Assert.That(comparer.Equals(expectedItems[i], actualItems[i])).IsTrue().Because($"Sequences differ at index {i}.");
    }

    private static async Task EqualSpanCoreAsync(int expectedCount, int actualCount, int mismatch)
    {
        _ = await Assert.That(actualCount).IsEqualTo(expectedCount);
        _ = await Assert.That(mismatch).IsEqualTo(-1).Because($"Sequences differ at index {mismatch}.");
    }

    private static int FirstMismatch<T>(ReadOnlySpan<T> expected, IReadOnlyList<T> actual, IEqualityComparer<T> comparer)
    {
        for (var i = 0; i < expected.Length; i++)
        {
            if (!comparer.Equals(expected[i], actual[i]))
                return i;
        }

        return -1;
    }

    private static async Task EqualMemoryAsync<T>(ReadOnlyMemory<T> expected, ReadOnlyMemory<T> actual, IEqualityComparer<T> comparer)
    {
        _ = await Assert.That(actual.Length).IsEqualTo(expected.Length);
        var mismatch = FirstMismatch(expected, actual, comparer);
        _ = await Assert.That(mismatch).IsEqualTo(-1).Because($"Memory blocks differ at index {mismatch}.");
    }

    private static int FirstMismatch<T>(ReadOnlyMemory<T> expected, ReadOnlyMemory<T> actual, IEqualityComparer<T> comparer)
    {
        var expectedSpan = expected.Span;
        var actualSpan = actual.Span;
        var length = Math.Min(expectedSpan.Length, actualSpan.Length);
        for (var i = 0; i < length; i++)
        {
            if (!comparer.Equals(expectedSpan[i], actualSpan[i]))
                return i;
        }

        return expectedSpan.Length == actualSpan.Length ? -1 : length;
    }
}
