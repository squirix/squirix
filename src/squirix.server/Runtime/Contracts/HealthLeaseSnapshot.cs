namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Lease subsection of health-ready diagnostics.
/// </summary>
internal readonly record struct HealthLeaseSnapshot(bool Enabled, int ActiveLeases, int PendingGrants, int PendingReleases);
