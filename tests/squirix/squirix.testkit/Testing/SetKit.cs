using System;
using System.Collections.Generic;

namespace Squirix.TestKit.Testing;

/// <summary>String set helpers for golden snapshot and API surface tests.</summary>
public static class SetKit
{
    /// <summary>
    /// Returns items present in <paramref name="left" /> but not in <paramref name="right" />,
    /// compared with <paramref name="membershipComparer" />, sorted with <see cref="StringComparer.Ordinal" />.
    /// </summary>
    /// <param name="left">Source set.</param>
    /// <param name="right">Set to subtract.</param>
    /// <param name="membershipComparer">Equality comparer for membership checks.</param>
    /// <returns>Sorted difference list.</returns>
    public static IReadOnlyList<string> CollectDifference(HashSet<string> left, HashSet<string> right, StringComparer membershipComparer)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        ArgumentNullException.ThrowIfNull(membershipComparer);

        var result = new List<string>();
        foreach (var item in left)
        {
            if (!Contains(right, item, membershipComparer))
                result.Add(item);
        }

        result.Sort(StringComparer.Ordinal);
        return result;
    }

    /// <summary>
    /// Returns whether <paramref name="set" /> contains an element equal to <paramref name="item" />
    /// per <paramref name="comparer" />.
    /// </summary>
    /// <param name="set">Candidates to scan.</param>
    /// <param name="item">Item to locate.</param>
    /// <param name="comparer">Equality comparer.</param>
    /// <returns><see langword="true" /> when a matching element exists.</returns>
    private static bool Contains(IEnumerable<string> set, string item, StringComparer comparer)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(comparer);

        foreach (var candidate in set)
        {
            if (comparer.Equals(candidate, item))
                return true;
        }

        return false;
    }
}
