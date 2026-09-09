using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Topology inputs required to compute a canonical topology fingerprint.</summary>
/// <remarks>
/// A point-in-time snapshot: values are captured at construction and never re-read, so the
/// hashed vector cannot drift mid-computation. Closed policy constants travel separately in
/// <see cref="FingerprintPolicy" />.
/// </remarks>
[Immutable]
internal sealed class FingerprintInputs
{
    /// <summary>Gets the cluster identifier.</summary>
    internal required string ClusterId { get; init; }

    /// <summary>Gets the stopped-topology configuration generation.</summary>
    internal required ulong ConfigurationGeneration { get; init; }

    /// <summary>Gets the minimum cluster package version token.</summary>
    internal required string MinClusterPackageVersion { get; init; }

    /// <summary>Gets peer descriptors included in the fingerprint.</summary>
    internal required IReadOnlyList<FingerprintPeer> Peers { get; init; }

    /// <summary>Gets the closed policy vector participating in the fingerprint.</summary>
    internal required FingerprintPolicy Policy { get; init; }

    /// <summary>Gets the quorum acknowledgement mode token.</summary>
    internal required string QuorumAckMode { get; init; }

    /// <summary>Gets the configured replica factor.</summary>
    internal required int ReplicaCount { get; init; }

    /// <summary>Gets the vnode count used by the ownership ring.</summary>
    internal required int VirtualNodes { get; init; }
}
