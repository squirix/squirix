namespace Squirix.Server.Core;

/// <summary>
/// Canonical cache-name validation failures.
/// </summary>
internal enum CacheNameValidationError
{
    /// <summary>
    /// Cache name is null, empty, or whitespace-only.
    /// </summary>
    Required,

    /// <summary>
    /// Cache name exceeds the configured maximum length.
    /// </summary>
    TooLong,

    /// <summary>
    /// Cache name contains characters outside the supported ASCII set.
    /// </summary>
    InvalidCharacters,

    /// <summary>
    /// Cache name is reserved for internal semantics.
    /// </summary>
    Reserved,
}
