using System;

namespace Squirix.Core;

/// <summary>Validates cache key strings before cache operations.</summary>
internal static class KeyInputValidator
{
    /// <summary>
    /// Maximum allowed length for a cache key (Unicode scalar values; .NET string length).
    /// </summary>
    internal const int MaxLength = 1024;

    /// <summary>
    /// Validates a key, or throws <see cref="ArgumentException" /> when invalid.
    /// </summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="parameterName">The caller parameter name for exceptions.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="key" /> is invalid.</exception>
    internal static void Validate(string? key, string parameterName)
    {
        if (!TryValidate(key, out var error))
            throw new ArgumentException(GetMessage(error), parameterName);
    }

    /// <summary>Returns a stable, non-user-input diagnostic message for the given validation error.</summary>
    /// <param name="error">The validation failure.</param>
    /// <returns>English message suitable for APIs and logs (no raw key material).</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="error" /> is not a known validation failure.</exception>
    private static string GetMessage(CacheKeyValidationError error) => error switch
    {
        CacheKeyValidationError.Required => "Cache key is required.",
        CacheKeyValidationError.TooLong => TooLongMessage,
        CacheKeyValidationError.ControlCharacters => "Cache key contains control characters.",
        _ => throw new ArgumentOutOfRangeException(nameof(error), "Unsupported cache key validation error."),
    };

    private static bool IsWhiteSpaceOnly(string key)
    {
        for (var i = 0; i < key.Length; i++)
        {
            if (!char.IsWhiteSpace(key[i]))
                return false;
        }

        return true;
    }

    /// <summary>Attempts to validate a key without throwing.</summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="error">The failure reason when validation fails.</param>
    /// <returns><see langword="true" /> if the key is valid; otherwise <see langword="false" />.</returns>
    private static bool TryValidate(string? key, out CacheKeyValidationError error)
    {
        if (string.IsNullOrEmpty(key) || IsWhiteSpaceOnly(key))
        {
            error = CacheKeyValidationError.Required;
            return false;
        }

        if (key.Length > MaxLength)
        {
            error = CacheKeyValidationError.TooLong;
            return false;
        }

        for (var i = 0; i < key.Length; i++)
        {
            if (!char.IsControl(key[i]))
                continue;
            error = CacheKeyValidationError.ControlCharacters;
            return false;
        }

        error = default;
        return true;
    }
}
