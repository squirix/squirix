namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>Coordination subsection of health-ready diagnostics.</summary>
/// <param name="Lease">Lease coordination metrics.</param>
/// <param name="Watch">Watch coordination metrics.</param>
internal readonly record struct HealthCoordinationSnapshot(HealthLeaseSnapshot Lease, HealthWatchSnapshot Watch);
