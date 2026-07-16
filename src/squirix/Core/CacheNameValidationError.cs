namespace Squirix.Core;

/// <summary>Canonical cache-name validation failures.</summary>
internal enum CacheNameValidationError
{
    /// <summary>Cache name is null, empty, or whitespace-only.</summary>
    Required = 0,

    /// <summary>Cache name exceeds the configured maximum length.</summary>
    TooLong = 1,

    /// <summary>Cache name contains characters outside the supported ASCII set.</summary>
    InvalidCharacters = 2,

    /// <summary>Cache name is the single-dot or double-dot relative segment.</summary>
    ForbiddenDotSegment = 3,
}
