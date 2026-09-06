using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Replication;

/// <summary>Durable identity of the activated replica-set topology Frozen at first start.</summary>
[Immutable]
internal sealed class ActivatedTopologyStamp
{
    /// <summary>Gets the stopped-topology configuration generation.</summary>
    internal ulong Generation { get; init; }

    /// <summary>Gets the 32-byte static topology fingerprint.</summary>
    internal required ReadOnlyMemory<byte> Fingerprint { get; init; }

    /// <summary>Gets the replica factor including the original owner.</summary>
    internal int ReplicaCount { get; init; }

    /// <summary>Returns whether another stamp describes the same activated identity.</summary>
    /// <param name="other">The stamp to compare.</param>
    /// <returns><see langword="true" /> when generation, replica count, and fingerprint all match.</returns>
    internal bool Matches(ActivatedTopologyStamp other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Generation == other.Generation && ReplicaCount == other.ReplicaCount && Fingerprint.Span.SequenceEqual(other.Fingerprint.Span);
    }
}
