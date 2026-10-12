using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The point a state transfer is taken at, and the snapshot image that goes with it.</summary>
/// <param name="Index">The applied index of the leader the state is taken at.</param>
/// <param name="IndexTerm">The term of the entry at <paramref name="Index" />.</param>
/// <param name="SnapshotBytes">The length of the published snapshot image that follows.</param>
/// <param name="SnapshotChecksum">The CRC32C the image ends with.</param>
/// <param name="Fingerprint">The topology fingerprint of the leader.</param>
/// <param name="Generation">The configuration generation of the leader.</param>
[Immutable]
internal readonly record struct ReplicaTransferBaseline(
    ulong Index,
    ulong IndexTerm,
    int SnapshotBytes,
    uint SnapshotChecksum,
    ReadOnlyMemory<byte> Fingerprint,
    ulong Generation);
