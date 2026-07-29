using System;

namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>Journal compaction subsection of health-ready diagnostics.</summary>
/// <param name="State">Current compaction state label.</param>
/// <param name="LastRunUtc">UTC timestamp of the last compaction run, if any.</param>
/// <param name="InFlight">Whether compaction is currently running.</param>
internal readonly record struct HealthCompactionSnapshot(string State, DateTime? LastRunUtc, bool InFlight);
