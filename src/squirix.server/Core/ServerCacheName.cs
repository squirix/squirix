using System;

namespace Squirix.Server.Core;

/// <summary>Canonical logical cache name for routing, journal namespaces, scan, watch, and tag invalidation after public validation.</summary>
internal sealed record ServerCacheName
{
    private ServerCacheName(string canonical)
    {
        Canonical = canonical;
    }

    /// <summary>Gets the canonical string used consistently across routing, persistence keys, and observability.</summary>
    internal string Canonical { get; }

    /// <summary>
    /// Maps null, empty, or whitespace-only names to <see cref="ServerCacheNames.DefaultNamespace" /> without applying public validation.
    /// </summary>
    /// <param name="cacheName">Logical name from an already-validated pipeline segment or trusted persistence.</param>
    /// <returns>The canonical cache name string for routing and <see cref="CacheKey" /> construction.</returns>
    internal static string NormalizeUnvalidated(string? cacheName) => string.IsNullOrWhiteSpace(cacheName) ? ServerCacheNames.DefaultNamespace : cacheName;

    /// <summary>
    /// Validates <paramref name="name" /> using public cache name rules and returns the canonical runtime value.
    /// </summary>
    /// <param name="name">Logical cache name from a public or wire boundary.</param>
    /// <param name="parameterName">Caller parameter name for exceptions.</param>
    /// <returns>A <see cref="ServerCacheName" /> whose <see cref="Canonical" /> is safe for the internal pipeline.</returns>
    internal static ServerCacheName ParsePublic(string? name, string parameterName = "cacheName")
    {
        var validated = ServerCacheNameValidator.Validate(name, parameterName);
        return new ServerCacheName(NormalizeUnvalidated(validated));
    }

    private static class ServerCacheNameValidator
    {
        private const int MaxLength = 128;

        private const string TooLongMessage = "Cache name exceeds the maximum length of 128 characters.";

        internal static string Validate(string? cacheName, string p) => TryValidate(cacheName, out var error) ? cacheName! : throw new ArgumentException(GetMessage(error), p);

        private static string GetMessage(ServerCacheNameValidationError error) => error switch
        {
            ServerCacheNameValidationError.Required => "Cache name is required.",
            ServerCacheNameValidationError.TooLong => TooLongMessage,
            ServerCacheNameValidationError.InvalidCharacters => "Cache name contains invalid characters. Allowed characters are A-Z, a-z, 0-9, '.', '_', and '-'.",
            ServerCacheNameValidationError.ForbiddenDotSegment => "Cache name is reserved.",
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

        private static bool TryValidate(string? cacheName, out ServerCacheNameValidationError error)
        {
            if (string.IsNullOrEmpty(cacheName) || IsWhiteSpaceOnly(cacheName))
            {
                error = ServerCacheNameValidationError.Required;
                return false;
            }

            if (cacheName.Length > MaxLength)
            {
                error = ServerCacheNameValidationError.TooLong;
                return false;
            }

            if (cacheName.Equals(".", StringComparison.Ordinal) || cacheName.Equals("..", StringComparison.Ordinal))
            {
                error = ServerCacheNameValidationError.ForbiddenDotSegment;
                return false;
            }

            for (var i = 0; i < cacheName.Length; i++)
            {
                var ch = cacheName[i];
                if (IsAllowed(ch))
                    continue;
                error = ServerCacheNameValidationError.InvalidCharacters;
                return false;
            }

            error = default;
            return true;
        }
    }
}
