using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Reassembled snapshot file with its declared wire framing.</summary>
/// <param name="FileBytes">Complete snapshot file bytes.</param>
/// <param name="DeclaredChecksum">Payload checksum declared by the transfer.</param>
/// <param name="DeclaredLastIncludedIndex">Boundary index declared by the transfer.</param>
/// <param name="DeclaredLastIncludedTerm">Boundary term declared by the transfer.</param>
[Immutable]
internal readonly record struct ReplicaSnapshotUpload(
    ReadOnlyMemory<byte> FileBytes,
    uint DeclaredChecksum,
    ulong DeclaredLastIncludedIndex,
    ulong DeclaredLastIncludedTerm);
