using System;

namespace Squirix.Server.Utils;

/// <summary>
/// UTF-16 hex formatting for binary diagnostics and stable fingerprint strings.
/// </summary>
internal static class HexFormat
{
    /// <summary>
    /// Formats a 32-byte digest as 64 uppercase hexadecimal characters.
    /// Matches <see cref="Convert.ToHexString(System.ReadOnlySpan{byte})" /> — required for stable idempotency fingerprints.
    /// </summary>
    /// <param name="digest">The 32-byte SHA-256 digest bytes.</param>
    /// <returns>A 64-character uppercase hexadecimal string.</returns>
    public static string FormatSha256HexUpper(ReadOnlySpan<byte> digest) =>
        digest.Length != 32 ? throw new ArgumentException("SHA-256 digest must be exactly 32 bytes.", nameof(digest)) : Convert.ToHexString(digest);

    /// <summary>
    /// Formats a 32-bit value as eight lowercase hexadecimal digits (same convention as <c>{value:x8}</c>).
    /// </summary>
    /// <param name="value">The unsigned value to format.</param>
    /// <returns>Eight lowercase hexadecimal characters.</returns>
    public static string FormatUInt32HexLower(uint value) => string.Create(
        8,
        value,
        static (dest, v) =>
        {
            for (var i = 0; i < 8; i++)
            {
                var shift = 28 - (i * 4);
                var nibble = (int)((v >> shift) & 0xF);
                dest[i] = (char)(nibble < 10 ? '0' + nibble : 'a' + (nibble - 10));
            }
        });
}
