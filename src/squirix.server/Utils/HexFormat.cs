using System;

namespace Squirix.Server.Utils;

/// <summary>UTF-16 hex formatting for binary diagnostics and stable fingerprint strings.</summary>
internal static class HexFormat
{
    /// <summary>
    /// Writes a 32-byte digest as 64 uppercase hexadecimal characters into <paramref name="destination" />.
    /// Matches <see cref="Convert.ToHexString(ReadOnlySpan{byte})" /> — required for stable idempotency fingerprints.
    /// </summary>
    /// <param name="destination">Destination span of at least 64 characters.</param>
    /// <param name="digest">The 32-byte SHA-256 digest bytes.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="digest" /> is not exactly 32 bytes or <paramref name="destination" /> is too short.</exception>
    /// <exception cref="InvalidOperationException">Thrown when hexadecimal formatting fails.</exception>
    internal static void WriteSha256HexUpper(Span<char> destination, ReadOnlySpan<byte> digest)
    {
        if (digest.Length != 32)
            throw new ArgumentException("SHA-256 digest must be exactly 32 bytes.", nameof(digest));
        if (destination.Length < 64)
            throw new ArgumentException("Destination must be at least 64 characters.", nameof(destination));

        if (!Convert.TryToHexString(digest, destination, out var written) || written != 64)
            throw new InvalidOperationException("Failed to format SHA-256 digest as uppercase hexadecimal.");
    }
}
