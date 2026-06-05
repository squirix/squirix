using System;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// journal compaction subsection of health-ready diagnostics.
/// </summary>
internal readonly record struct HealthCompactionSnapshot(string State, DateTime? LastRunUtc, bool InFlight);
