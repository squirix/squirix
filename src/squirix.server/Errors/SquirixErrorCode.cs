namespace Squirix.Server.Errors;

/// <summary>
/// Defines stable squirix error codes used by protocol adapters and structured error payloads.
/// </summary>
public enum SquirixErrorCode
{
    /// <summary>
    /// Cache name validation failed.
    /// </summary>
    InvalidCacheName,

    /// <summary>
    /// Cache key validation failed.
    /// </summary>
    InvalidCacheKey,

    /// <summary>
    /// Request validation failed.
    /// </summary>
    BadRequest,

    /// <summary>
    /// Requested resource was not found.
    /// </summary>
    NotFound,

    /// <summary>
    /// Request conflicts with current resource state.
    /// </summary>
    Conflict,

    /// <summary>
    /// Request payload exceeds the configured limit.
    /// </summary>
    PayloadTooLarge,

    /// <summary>
    /// Request was rejected by admission control.
    /// </summary>
    TooManyRequests,

    /// <summary>
    /// Estimated cache memory usage is critical; memory-growing writes are rejected (admission control).
    /// </summary>
    MemoryPressure,
}
