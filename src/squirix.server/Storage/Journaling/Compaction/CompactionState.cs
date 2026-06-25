namespace Squirix.Server.Storage.Journaling.Compaction;

/// <summary>Represents the current state of journal compaction workflow.</summary>
internal enum CompactionState
{
    /// <summary>No compaction is scheduled or running.</summary>
    Idle = 0,

    /// <summary>Compaction is requested and pending (e.g., waiting for a trigger/min-gap).</summary>
    Waiting = 1,

    /// <summary>Compaction is in progress.</summary>
    Running = 2,

    /// <summary>Compaction is temporarily deferred (backoff after completion or failure).</summary>
    BackingOff = 3,

    /// <summary>The last compaction attempt failed.</summary>
    Failed = 4,
}
