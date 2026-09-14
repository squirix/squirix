namespace Squirix.Server.Node.Services;

/// <summary>Result of a write-ahead intent reservation for one mutating RPC.</summary>
internal enum IdempotencyReserveResult
{
    /// <summary>No prior record existed; the caller owns this execution.</summary>
    Acquired = 0,

    /// <summary>A completed outcome is already stored; the caller should replay it.</summary>
    AlreadyCompleted = 1,

    /// <summary>Execution for this operation is already in flight; the outcome is unknown to this caller.</summary>
    AlreadyStarted = 2,
}
