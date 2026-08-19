using Squirix.Server.Attributes;

namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>Lease subsection of health-ready diagnostics.</summary>
/// <param name="Enabled">Whether lease coordination is enabled.</param>
/// <param name="ActiveLeases">Number of active leases.</param>
/// <param name="PendingGrants">Number of pending lease grants.</param>
/// <param name="PendingReleases">Number of pending lease releases.</param>
[Immutable]
internal readonly record struct HealthLeaseSnapshot(bool Enabled, int ActiveLeases, int PendingGrants, int PendingReleases);
