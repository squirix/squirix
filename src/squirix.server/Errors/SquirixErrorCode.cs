namespace Squirix.Server.Errors;

/// <summary>Defines stable squirix error codes used by protocol adapters and structured error payloads.</summary>
public enum SquirixErrorCode
{
    /// <summary>Unspecified; not emitted as a structured protocol error.</summary>
    None = 0,

    /// <summary>Cache key validation failed.</summary>
    InvalidCacheKey = 1,

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

    /// <summary>Cache entry tags exceed configured count or UTF-8 size limits.</summary>
    InvalidEntryTags = 12,

    /// <summary>On-disk journal size reached the configured hard limit; durable writes are rejected.</summary>
    JournalDiskQuota = 13,
}
