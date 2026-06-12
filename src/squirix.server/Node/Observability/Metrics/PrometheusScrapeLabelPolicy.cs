using System;
using System.Collections.Generic;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>
/// Label filtering for the public HTTP Prometheus scrape profile.
/// </summary>
internal static class PrometheusScrapeLabelPolicy
{
    private static readonly HashSet<string> ExcludedLabelNames = new(StringComparer.Ordinal)
    {
        "cache",
        "exception_type",
    };

    /// <summary>
    /// Returns tags with identifying labels removed for public HTTP export.
    /// </summary>
    /// <param name="tags">Full instrument tags.</param>
    /// <returns>Filtered tag list sorted by key.</returns>
    internal static KeyValuePair<string, object?>[] FilterPublicTags(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length == 0)
            return [];

        var filtered = new List<KeyValuePair<string, object?>>(tags.Length);
        foreach (var tag in tags)
        {
            if (!ExcludedLabelNames.Contains(tag.Key))
                filtered.Add(tag);
        }

        if (filtered.Count == 0)
            return [];

        filtered.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        return [.. filtered];
    }

    /// <summary>
    /// Builds a Prometheus label set string from sorted tags.
    /// </summary>
    /// <param name="tags">Sorted tag list.</param>
    /// <returns>Prometheus label set without outer braces.</returns>
    internal static string BuildLabelKey(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length == 0)
            return string.Empty;

        var sb = new System.Text.StringBuilder();
        for (var i = 0; i < tags.Length; i++)
        {
            if (i > 0)
                _ = sb.Append(',');
            _ = sb.Append(tags[i].Key);
            _ = sb.Append("=\"");
            _ = sb.Append(Escape(tags[i].Value?.ToString() ?? string.Empty));
            _ = sb.Append('"');
        }

        return sb.ToString();
    }

    private static string Escape(string s) => s.Replace("\\", @"\\").Replace("\n", "\\n").Replace("\"", "\\\"");
}
