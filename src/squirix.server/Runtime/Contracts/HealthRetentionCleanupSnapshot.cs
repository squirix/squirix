using System;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Retention cleanup subsection of health-ready diagnostics.
/// </summary>
internal readonly record struct HealthRetentionCleanupSnapshot(
    bool Degraded,
    int ConsecutiveWriteFailures,
    int RecentFailureCount,
    DateTime? LastFailureUtc);
