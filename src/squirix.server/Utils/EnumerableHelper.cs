using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace Squirix.Server.Utils;

internal static class EnumerableHelper
{
    /// <summary>Returns distinct, non-whitespace strings in first-seen order.</summary>
    /// <param name="values">Candidate values.</param>
    /// <returns>Deduplicated values.</returns>
    public static string[] GetDistinct([NoEnumeration] IEnumerable<string> values)
    {
        ArgumentNullException.ThrowIfNull(values);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        string[] buffer = [];
        var writeIndex = 0;

        foreach (var value in values)
        {
            if (string.IsNullOrWhiteSpace(value))
                continue;

            if (!seen.Add(value))
                continue;

            if (writeIndex == buffer.Length)
            {
                var nextLength = buffer.Length is 0 ? 4 : buffer.Length * 2;
                var grown = new string[nextLength];
                buffer.AsSpan(0, writeIndex).CopyTo(grown);
                buffer = grown;
            }

            buffer[writeIndex++] = value;
        }

        if (writeIndex is 0)
            return [];

        if (writeIndex == buffer.Length)
            return buffer;

        var result = new string[writeIndex];
        buffer.AsSpan(0, writeIndex).CopyTo(result);
        return result;
    }
}
