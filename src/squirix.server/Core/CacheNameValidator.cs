using System;

namespace Squirix.Server.Core;

internal static class CacheNameValidator
{
    private const int MaxLength = 128;

    public static string Validate(string? cacheName, string p) => TryValidate(cacheName, out var error) ? cacheName! : throw new ArgumentException(GetMessage(error), p);

    private static string GetMessage(CacheNameValidationError error) => error switch
    {
        CacheNameValidationError.Required => "Cache name is required.",
        CacheNameValidationError.TooLong => $"Cache name exceeds the maximum length of {MaxLength} characters.",
        CacheNameValidationError.InvalidCharacters => "Cache name contains invalid characters. Allowed characters are A-Z, a-z, 0-9, '.', '_', and '-'.",
        CacheNameValidationError.Reserved => "Cache name is reserved.",
        _ => throw new ArgumentOutOfRangeException(nameof(error), error, null),
    };

    private static bool IsAllowed(char ch) => ch <= sbyte.MaxValue && (char.IsAsciiLetterOrDigit(ch) || ch is '.' or '_' or '-');

    private static bool IsWhiteSpaceOnly(string cacheName)
    {
        for (var i = 0; i < cacheName.Length; i++)
        {
            if (!char.IsWhiteSpace(cacheName[i]))
                return false;
        }

        return true;
    }

    private static bool TryValidate(string? cacheName, out CacheNameValidationError error)
    {
        if (string.IsNullOrEmpty(cacheName) || IsWhiteSpaceOnly(cacheName))
        {
            error = CacheNameValidationError.Required;
            return false;
        }

        if (cacheName.Length > MaxLength)
        {
            error = CacheNameValidationError.TooLong;
            return false;
        }

        if (cacheName is "." or "..")
        {
            error = CacheNameValidationError.Reserved;
            return false;
        }

        for (var i = 0; i < cacheName.Length; i++)
        {
            var ch = cacheName[i];
            if (IsAllowed(ch))
                continue;
            error = CacheNameValidationError.InvalidCharacters;
            return false;
        }

        error = default;
        return true;
    }
}
