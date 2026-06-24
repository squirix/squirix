namespace Squirix.Server.Core;

/// <summary>Canonical cache-key validation failures for user-provided entry keys.</summary>
internal enum CacheKeyValidationError
{
    /// <summary>Key is null, empty, or contains only whitespace characters.</summary>
    Required = 0,

    /// <summary>Key exceeds the configured maximum length.</summary>
    TooLong = 1,

    /// <summary>Key contains Unicode or ASCII control characters.</summary>
    ControlCharacters = 2,
}
