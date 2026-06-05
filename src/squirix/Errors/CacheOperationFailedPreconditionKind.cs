namespace Squirix.Errors;

/// <summary>
/// Stable contract classification for cache-operation transport faults that must stay aligned across
/// REST projections, gRPC adapters, remote cluster helpers, and <c>DomainTransportErrorMapper</c>.
/// </summary>
internal enum CacheOperationFailedPreconditionKind
{
    /// <summary>
    /// No recognized stable contract for the given detail string.
    /// </summary>
    None = 0,

    /// <summary>
    /// Counter increment type mismatch (FailedPrecondition detail).
    /// </summary>
    CounterIncrementTypeMismatch = 1,

    /// <summary>
    /// Explicit insert version is not greater than the stored version (FailedPrecondition detail).
    /// </summary>
    InsertVersionMustExceedCurrent = 2,
}
