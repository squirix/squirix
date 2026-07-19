using System;

namespace Squirix.Server.Utils;

/// <summary>Produces PII-safe representations of cache keys for use in structured logs and transport errors.</summary>
internal static class CacheKeySanitizer
{
    private const int FullDisplayThreshold = 8;
    private const int MaxPrefixLength = 4;

    /// <summary>
    /// Returns a safe representation of <paramref name="key" /> suitable for log messages.
    /// </summary>
    /// <param name="key">The cache key to sanitize.</param>
    /// <returns>A PII-safe hint string.</returns>
    internal static string Sanitize(string? key)
    {
        if (string.IsNullOrEmpty(key))
            return "(empty)";

        if (key.Length <= FullDisplayThreshold)
            return key;

        var len = key.Length;
        return $"{key.AsSpan(0, MaxPrefixLength)}***[len={len}]";
    }
}
