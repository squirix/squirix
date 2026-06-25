namespace Squirix.Server.Errors;

/// <summary>Defines stable squirix error codes used by protocol adapters and structured error payloads.</summary>
public enum SquirixErrorCode
{
    /// <summary>Cache name validation failed.</summary>
    InvalidCacheName = 0,

    /// <summary>Cache key validation failed.</summary>
    InvalidCacheKey = 1,

    /// <summary>Request validation failed.</summary>
    BadRequest = 2,

    /// <summary>Requested resource was not found.</summary>
    NotFound = 3,

    /// <summary>Request conflicts with current resource state.</summary>
    Conflict = 4,

    /// <summary>Request payload exceeds the configured limit.</summary>
    PayloadTooLarge = 5,

    /// <summary>Request was rejected by admission control.</summary>
    TooManyRequests = 6,

    /// <summary>Estimated cache memory usage is critical; memory-growing writes are rejected (admission control).</summary>
    MemoryPressure = 7,

    /// <summary>Mutating RPC is missing a required operation identifier.</summary>
    OperationIdRequired = 8,

    /// <summary>Operation identifier is not 32 lowercase hex characters.</summary>
    OperationIdInvalidFormat = 9,

    /// <summary>Operation identifier exceeds the maximum allowed length.</summary>
    OperationIdTooLong = 10,

    /// <summary>An operation identifier was reused with a different mutation fingerprint.</summary>
    OperationIdReuseMismatch = 11,
}
