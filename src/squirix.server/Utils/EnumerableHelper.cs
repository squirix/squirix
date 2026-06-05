using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace Squirix.Server.Utils;

internal static class EnumerableHelper
{
    /// <summary>
    /// Returns distinct, non-whitespace strings in first-seen order.
    /// </summary>
    /// <param name="values">Candidate values.</param>
    /// <returns>Deduplicated values.</returns>
    public static string[] GetDistinct([NoEnumeration] IEnumerable<string> values)
    {
        ArgumentNullException.ThrowIfNull(values);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<string>();

        foreach (var value in values)
        {
            if (string.IsNullOrWhiteSpace(value))
                continue;

            if (seen.Add(value))
                result.Add(value);
        }

        return [.. result];
    }
}
