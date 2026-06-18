using System;

namespace Squirix.Server.Storage;

/// <summary>Readiness state for best-effort manifest retention cleanup after durable writes.</summary>
internal interface IRetentionCleanupReadinessStatus
{
    /// <summary>Gets the number of consecutive manifest writes whose retention cleanup reported at least one failure.</summary>
    int ConsecutiveWriteFailures { get; }

    /// <summary>Gets a value indicating whether retention cleanup is persistently failing and readiness should degrade.</summary>
    bool IsDegraded { get; }

    /// <summary>Gets the UTC timestamp of the most recent retention cleanup failure, if any.</summary>
    DateTime? LastFailureUtc { get; }

    /// <summary>Gets the number of retention cleanup failures observed inside the configured sliding window.</summary>
    int RecentFailureCount { get; }

    /// <summary>Records the outcome of retention cleanup performed during one manifest write.</summary>
    /// <param name="hadFailure">Whether any retention delete or cleanup exception was reported.</param>
    void RecordWriteOutcome(bool hadFailure);
}
