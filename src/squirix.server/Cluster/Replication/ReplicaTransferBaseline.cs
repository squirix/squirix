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
/// <param name="CeilingChecked">
/// Whether the leader read the current terms of members that, with itself and without the receiver, form a majority, and found none above its
/// own (#1126); only then may an unseeded copy take the transfer.
/// </param>
[Immutable]
internal readonly record struct ReplicaTransferBaseline(
    ulong Index,
    ulong IndexTerm,
    int SnapshotBytes,
    uint SnapshotChecksum,
    ReadOnlyMemory<byte> Fingerprint,
    ulong Generation,
    bool CeilingChecked = false);
