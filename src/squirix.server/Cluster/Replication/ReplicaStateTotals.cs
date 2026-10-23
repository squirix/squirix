using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>What a state sink received since it was entered.</summary>
/// <param name="EntryCount">The number of entries written.</param>
/// <param name="Crc">The CRC32C over the entries in arrival order.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaStateTotals(ulong EntryCount, uint Crc);
