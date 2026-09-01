using System;
using System.Collections.Generic;

namespace Squirix.Server.Cluster;

/// <summary>Shared peer-id filtering used by physical and vnode ring construction.</summary>
internal static class DistinctNodeIds
{
    /// <summary>Returns distinct non-whitespace node IDs in first-seen order.</summary>
    /// <param name="nodeIds">Configured peer node identifiers.</param>
    /// <returns>A trimmed array of distinct ids; empty when none remain.</returns>
    internal static string[] InInsertionOrder(ReadOnlySpan<string> nodeIds)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var buffer = new string[nodeIds.Length];
        var write = 0;
        for (var i = 0; i < nodeIds.Length; i++)
        {
            var value = nodeIds[i]?.Trim();
            if (string.IsNullOrEmpty(value) || !seen.Add(value))
                continue;

            buffer[write++] = value;
        }

        return Trim(buffer, write);
    }

    private static string[] Trim(string[] buffer, int write)
    {
        if (write == 0)
            return [];

        if (write == buffer.Length)
            return buffer;

        var result = new string[write];
        buffer.AsSpan(0, write).CopyTo(result);
        return result;
    }
}
