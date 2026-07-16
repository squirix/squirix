using System;

namespace Squirix.Core;

/// <summary>Canonical logical cache name for routing, journal namespaces, scan, watch, and tag invalidation after public validation.</summary>
internal sealed record CacheName
{
    private CacheName(string canonical)
    {
        Canonical = canonical;
    }

    /// <summary>Gets the canonical string used consistently across routing, persistence keys, and observability.</summary>
    internal string Canonical { get; }

    /// <summary>
    /// Validates <paramref name="name" /> using public cache name rules and returns the canonical runtime value.
    /// </summary>
    /// <param name="name">Logical cache name from a public or wire boundary.</param>
    /// <param name="parameterName">Caller parameter name for exceptions.</param>
    /// <returns>A <see cref="CacheName" /> whose <see cref="Canonical" /> is safe for the internal pipeline.</returns>
    internal static CacheName ParsePublic(string? name, string parameterName = "cacheName")
    {
        var validated = CacheNameValidator.Validate(name, parameterName);
        return new CacheName(NormalizeUnvalidated(validated));
    }

    /// <summary>
    /// Maps null, empty, or whitespace-only names to <see cref="CacheNames.DefaultNamespace" /> without applying public validation.
    /// </summary>
    /// <param name="cacheName">Logical name from an already-validated pipeline segment or trusted persistence.</param>
    /// <returns>The canonical cache name string for routing.</returns>
    private static string NormalizeUnvalidated(string? cacheName) => string.IsNullOrWhiteSpace(cacheName) ? CacheNames.DefaultNamespace : cacheName;

    private static class CacheNameValidator
    {
        private const int MaxLength = 128;

        internal static string Validate(string? cacheName, string p) => TryValidate(cacheName, out var error) ? cacheName! : throw new ArgumentException(GetMessage(error), p);

        private static string GetMessage(CacheNameValidationError error) => error switch
        {
            CacheNameValidationError.Required => "Cache name is required.",
            CacheNameValidationError.TooLong => $"Cache name exceeds the maximum length of {MaxLength} characters.",
            CacheNameValidationError.InvalidCharacters => "Cache name contains invalid characters. Allowed characters are A-Z, a-z, 0-9, '.', '_', and '-'.",
            CacheNameValidationError.ForbiddenDotSegment => "Cache name is reserved.",
            _ => throw new ArgumentOutOfRangeException(nameof(error), "Unknown cache name validation error."),
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

            if (cacheName.Equals(".", StringComparison.Ordinal) || cacheName.Equals("..", StringComparison.Ordinal))
            {
                error = CacheNameValidationError.ForbiddenDotSegment;
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
}
