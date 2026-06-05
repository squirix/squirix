using System;

namespace Squirix.Core;

/// <summary>
/// Central validation for user cache entry keys.
/// </summary>
internal static class CacheKeyValidator
{
    /// <summary>
    /// Maximum allowed length for a cache key (Unicode scalar values; .NET string length).
    /// </summary>
    internal const int MaxLength = 1024;

    /// <summary>
    /// Returns a stable, non-user-input diagnostic message for the given validation error.
    /// </summary>
    /// <param name="error">The validation failure.</param>
    /// <returns>English message suitable for APIs and logs (no raw key material).</returns>
    internal static string GetMessage(CacheKeyValidationError error) => error switch
    {
        CacheKeyValidationError.Required => "Cache key is required.",
        CacheKeyValidationError.TooLong => $"Cache key exceeds the maximum length of {MaxLength} characters.",
        CacheKeyValidationError.ControlCharacters => "Cache key contains control characters.",
        _ => throw new ArgumentOutOfRangeException(nameof(error), error, "Unsupported cache key validation error."),
    };

    /// <summary>
    /// Attempts to validate a key without throwing.
    /// </summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="error">The failure reason when validation fails.</param>
    /// <returns><c>true</c> if the key is valid; otherwise <c>false</c>.</returns>
    internal static bool TryValidate(string? key, out CacheKeyValidationError error)
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

    /// <summary>
    /// Validates a key and returns it, or throws <see cref="ArgumentException" />.
    /// </summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="parameterName">The caller parameter name for the exception.</param>
    /// <returns>The original key when valid.</returns>
    internal static string Validate(string? key, string parameterName) => TryValidate(key, out var error) ? key! : throw new ArgumentException(GetMessage(error), parameterName);

    private static bool IsWhiteSpaceOnly(string key)
    {
        for (var i = 0; i < key.Length; i++)
        {
            if (!char.IsWhiteSpace(key[i]))
                return false;
        }

        return true;
    }
}
