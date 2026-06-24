using System;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Retention cleanup subsection of health-ready diagnostics.</summary>
/// <param name="Degraded">Whether retention cleanup is in a degraded state.</param>
/// <param name="ConsecutiveWriteFailures">Consecutive retention write failures.</param>
/// <param name="RecentFailureCount">Failures observed in the recent evaluation window.</param>
/// <param name="LastFailureUtc">UTC timestamp of the most recent failure, if any.</param>
internal readonly record struct HealthRetentionCleanupSnapshot(
    bool Degraded,
    int ConsecutiveWriteFailures,
    int RecentFailureCount,
    DateTime? LastFailureUtc);
