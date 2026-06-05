namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Represents the current state of journal compaction workflow.
/// </summary>
internal enum CompactionState
{
    /// <summary>
    /// No compaction is scheduled or running.
    /// </summary>
    Idle,

    /// <summary>
    /// Compaction is requested and pending (e.g., waiting for a trigger/min-gap).
    /// </summary>
    Waiting,

    /// <summary>
    /// Compaction is in progress.
    /// </summary>
    Running,

    /// <summary>
    /// Compaction is temporarily deferred (backoff after completion or failure).
    /// </summary>
    BackingOff,

    /// <summary>
    /// The last compaction attempt failed.
    /// </summary>
    Failed,
}
