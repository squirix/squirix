using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>Retained size of one served replica group log in health-ready diagnostics.</summary>
/// <param name="GroupId">The replica group identifier.</param>
/// <param name="LogBytes">The durable size of the group log file in bytes, header included.</param>
/// <param name="RetainedEntries">The number of entry frames the group log file holds.</param>
/// <param name="SnapshotIndex">The last log index the published group snapshot covers, or zero when none is published.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct HealthReplicaGroupSnapshot(string GroupId, long LogBytes, int RetainedEntries, ulong SnapshotIndex);
