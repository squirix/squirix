using System;

namespace Squirix.Server.Core;

/// <summary>Central validation for user cache entry keys.</summary>
internal static class CacheKeyValidator
{
    /// <summary>
    /// Maximum allowed length for a cache key (Unicode scalar values; .NET string length).
    /// </summary>
    private const int MaxLength = 1024;

    private const string TooLongMessage = "Cache key exceeds the maximum length of 1024 characters.";

    /// <summary>Returns a stable, non-user-input diagnostic message for the given validation error.</summary>
    /// <param name="error">The validation failure.</param>
    /// <returns>English message suitable for APIs and logs (no raw key material).</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="error" /> is not a known validation failure.</exception>
    internal static string GetMessage(ServerKeyValidationError error) => error switch
    {
        ServerKeyValidationError.Required => "Cache key is required.",
        ServerKeyValidationError.TooLong => TooLongMessage,
        ServerKeyValidationError.ControlCharacters => "Cache key contains control characters.",
        _ => throw new ArgumentOutOfRangeException(nameof(error), "Unsupported cache key validation error."),
    };

    /// <summary>Attempts to validate a key without throwing.</summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="error">The failure reason when validation fails.</param>
    /// <returns><see langword="true" /> if the key is valid; otherwise <see langword="false" />.</returns>
    public static bool TryValidate(string? key, out CacheKeyValidationError error)
    {
        if (string.IsNullOrEmpty(key) || IsWhiteSpaceOnly(key))
        {
            error = ServerKeyValidationError.Required;
            return false;
        }

        if (key.Length > MaxLength)
        {
            error = ServerKeyValidationError.TooLong;
            return false;
        }

        for (var i = 0; i < key.Length; i++)
        {
            if (!char.IsControl(key[i]))
                continue;
            error = ServerKeyValidationError.ControlCharacters;
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
    /// <exception cref="ArgumentException">Thrown when <paramref name="key" /> is invalid.</exception>
    public static string Validate(string? key, string parameterName) => TryValidate(key, out var error) ? key! : throw new ArgumentException(GetMessage(error), parameterName);

    /// <summary>Returns a stable, non-user-input diagnostic message for the given validation error.</summary>
    /// <param name="error">The validation failure.</param>
    /// <returns>English message suitable for APIs and logs (no raw key material).</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="error" /> is not a known validation failure.</exception>
    private static string GetMessage(CacheKeyValidationError error) => error switch
    {
        CacheKeyValidationError.Required => "Cache key is required.",
        CacheKeyValidationError.TooLong => $"Cache key exceeds the maximum length of {MaxLength} characters.",
        CacheKeyValidationError.ControlCharacters => "Cache key contains control characters.",
        _ => throw new ArgumentOutOfRangeException(nameof(error), "Unsupported cache key validation error."),
    };

    private static bool IsWhiteSpaceOnly(string key)
    {
        for (var i = 0; i < key.Length; i++)
            if (!char.IsWhiteSpace(key[i]))
                return false;

        return true;
    }
}
