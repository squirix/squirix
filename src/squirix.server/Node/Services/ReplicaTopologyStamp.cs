using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>The static topology identity shared by the members of a replica set.</summary>
/// <param name="Fingerprint">Static topology fingerprint.</param>
/// <param name="Generation">Static configuration generation.</param>
[Immutable]
internal readonly record struct ReplicaTopologyStamp(ReadOnlyMemory<byte> Fingerprint, ulong Generation);
