using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>Label filtering for the public HTTP Prometheus scrape profile.</summary>
internal static class PrometheusScrapeLabelPolicy
{
    private static readonly HashSet<string> ExcludedLabelNames = new(StringComparer.Ordinal)
    {
        "cache",
        "exception_type",
    };

    /// <summary>Builds a Prometheus label set string from sorted tags.</summary>
    /// <param name="tags">Sorted tag list.</param>
    /// <returns>Prometheus label set without outer braces.</returns>
    internal static string BuildLabelKey(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length is 0)
            return string.Empty;

        var sb = new StringBuilder();
        for (var i = 0; i < tags.Length; i++)
        {
            if (i > 0)
                _ = sb.Append(',');
            _ = sb.Append(tags[i].Key);
            _ = sb.Append("=\"");
            _ = sb.Append(Escape(Convert.ToString(tags[i].Value, CultureInfo.InvariantCulture) ?? string.Empty));
            _ = sb.Append('"');
        }

        return sb.ToString();
    }

    /// <summary>Returns tags with identifying labels removed for public HTTP export.</summary>
    /// <param name="tags">Full instrument tags.</param>
    /// <returns>Filtered tag list sorted by key.</returns>
    internal static KeyValuePair<string, object?>[] FilterPublicTags(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length is 0)
            return [];

        var filtered = new KeyValuePair<string, object?>[tags.Length];
        var writeIndex = 0;
        foreach (var tag in tags)
        {
            if (!ExcludedLabelNames.Contains(tag.Key))
                filtered[writeIndex++] = tag;
        }

        if (writeIndex is 0)
            return [];

        if (writeIndex != filtered.Length)
            Array.Resize(ref filtered, writeIndex);

        Array.Sort(filtered, static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        return filtered;
    }

    private static string Escape(string s) => s
                                             .Replace("\\", @"\\", StringComparison.Ordinal).Replace("\n", "\\n", StringComparison.Ordinal)
                                             .Replace("\"", "\\\"", StringComparison.Ordinal);
}
