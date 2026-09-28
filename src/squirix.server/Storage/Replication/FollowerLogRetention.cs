using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>How much of a replica group log is retained on disk and in memory.</summary>
/// <param name="LogBytes">The durable length of the group log file in bytes, header included.</param>
/// <param name="RetainedEntries">The number of entry frames the group log file holds.</param>
/// <param name="RetainedPayloads">The number of entry payloads held in memory: the entries not yet applied, and the uncommitted tail.</param>
/// <param name="SnapshotIndex">The last log index the published snapshot covers, or zero when none is published.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct FollowerLogRetention(long LogBytes, int RetainedEntries, int RetainedPayloads, ulong SnapshotIndex);
