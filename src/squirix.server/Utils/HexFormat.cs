using System;
using System.Globalization;

namespace Squirix.Server.Utils;

/// <summary>UTF-16 hex formatting for binary diagnostics and stable fingerprint strings.</summary>
internal static class HexFormat
{
    /// <summary>
    /// Formats a 32-byte digest as 64 uppercase hexadecimal characters.
    /// Matches <see cref="Convert.ToHexString(System.ReadOnlySpan{byte})" /> — required for stable idempotency fingerprints.
    /// </summary>
    /// <param name="digest">The 32-byte SHA-256 digest bytes.</param>
    /// <returns>A 64-character uppercase hexadecimal string.</returns>
    public static string FormatSha256HexUpper(ReadOnlySpan<byte> digest) =>
        digest.Length is not 32 ? throw new ArgumentException("SHA-256 digest must be exactly 32 bytes.", nameof(digest)) : Convert.ToHexString(digest);

    /// <summary>
    /// Formats a 32-bit value as eight lowercase hexadecimal digits (same convention as <c>{value:x8}</c>).
    /// </summary>
    /// <param name="value">The unsigned value to format.</param>
    /// <returns>Eight lowercase hexadecimal characters.</returns>
    public static string FormatUInt32HexLower(uint value) => value.ToString("x8", CultureInfo.InvariantCulture);
}
