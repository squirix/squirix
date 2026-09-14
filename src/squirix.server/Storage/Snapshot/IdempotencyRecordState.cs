namespace Squirix.Server.Storage.Snapshot;

/// <summary>Lifecycle state of a durable idempotency record for one mutating RPC.</summary>
internal enum IdempotencyRecordState
{
    /// <summary>The mutation outcome was recorded and may be replayed.</summary>
    Completed = 0,

    /// <summary>Write-ahead intent: execution began (or the mutation committed) but the outcome is unknown.</summary>
    Started = 1,
}
