namespace Squirix.Server.Errors;

/// <summary>Defines stable squirix error codes used by protocol adapters and structured error payloads.</summary>
public enum SquirixErrorCode
{
    /// <summary>Unspecified; not emitted as a structured protocol error.</summary>
    None = 0,

    /// <summary>Cache key validation failed.</summary>
    InvalidCacheKey = 1,

    /// <summary>Request payload exceeds the configured limit.</summary>
    PayloadTooLarge = 2,

    /// <summary>Request was rejected by admission control.</summary>
    TooManyRequests = 3,

    /// <summary>Estimated cache memory usage is critical; memory-growing writes are rejected (admission control).</summary>
    MemoryPressure = 4,

    /// <summary>Mutating RPC is missing a required operation identifier.</summary>
    OperationIdRequired = 5,

    /// <summary>Operation identifier is not 32 lowercase hex characters.</summary>
    OperationIdInvalidFormat = 6,

    /// <summary>Operation identifier exceeds the maximum allowed length.</summary>
    OperationIdTooLong = 7,

    /// <summary>An operation identifier was reused with a different mutation fingerprint.</summary>
    OperationIdReuseMismatch = 8,

    /// <summary>Cache entry tags exceed configured count or UTF-8 size limits.</summary>
    InvalidEntryTags = 9,

    /// <summary>On-disk journal size reached the configured hard limit; durable writes are rejected.</summary>
    JournalDiskQuota = 10,
}
