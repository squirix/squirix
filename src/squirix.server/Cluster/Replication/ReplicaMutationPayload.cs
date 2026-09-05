using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Canonical payloads carried by one replica-group mutation.</summary>
/// <param name="CanonicalPayload">Canonical mutation payload.</param>
/// <param name="OutcomePayload">Canonical successful outcome payload.</param>
/// <param name="PayloadChecksum">CRC32C of the canonical mutation payload.</param>
[Immutable]
internal sealed record ReplicaMutationPayload(
    ReadOnlyMemory<byte> CanonicalPayload,
    ReadOnlyMemory<byte> OutcomePayload,
    uint PayloadChecksum);
