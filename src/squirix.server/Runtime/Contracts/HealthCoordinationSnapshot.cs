namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Coordination subsection of health-ready diagnostics.
/// </summary>
internal readonly record struct HealthCoordinationSnapshot(HealthLeaseSnapshot Lease, HealthWatchSnapshot Watch);
