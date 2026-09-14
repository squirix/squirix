using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Canonical encoded length and checksum of a snapshot payload.</summary>
/// <param name="Length">Encoded payload length in bytes.</param>
/// <param name="Checksum">CRC32C of the encoded payload.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct GroupSnapshotPayloadIntegrity(int Length, uint Checksum);
